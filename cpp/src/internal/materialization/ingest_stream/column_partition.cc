// Implements one-parse, disjoint-column packet ownership and Arrow reparenting.

#include "internal/materialization/ingest_stream/column_partition.hh"

#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/materialization/batch_appender_internal.hh"
#include "internal/materialization/builders/detail.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/planning/plan_compile.hh"

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>

namespace sanitize::internal {
namespace {

constexpr std::size_t kMinimumPartitionColumns = 128;
constexpr std::size_t kMinimumColumnsPerGroup = 2;
constexpr std::size_t kMaximumColumnGroups = 8;

[[nodiscard]] constexpr std::size_t
column_conversion_cost(const sanitize::ColumnPlan &column) noexcept {
  switch (column.logical_type.kind) {
  case sanitize::LogicalKind::kNull:
  case sanitize::LogicalKind::kBool:
    return 1;
  case sanitize::LogicalKind::kInt64:
  case sanitize::LogicalKind::kFloat64:
    return 2;
  case sanitize::LogicalKind::kUtf8:
    return 4;
  case sanitize::LogicalKind::kTimestampNs:
  case sanitize::LogicalKind::kDate32:
  case sanitize::LogicalKind::kTime32s:
    return 3;
  case sanitize::LogicalKind::kStruct:
  case sanitize::LogicalKind::kList:
    return 8;
  }
  return 1;
}

[[nodiscard]] constexpr std::size_t
saturating_add_cost(std::size_t left, std::size_t right) noexcept {
  const auto max = std::numeric_limits<std::size_t>::max();
  return right > max - left ? max : left + right;
}

[[nodiscard]] std::size_t range_cost(std::span<const std::size_t> prefix,
                                     std::size_t first,
                                     std::size_t end) noexcept {
  if (first >= end || end >= prefix.size()) {
    return 0;
  }
  return prefix[end] - prefix[first];
}

[[nodiscard]] std::size_t distance_from_target(std::size_t value,
                                               std::size_t target) noexcept {
  return value >= target ? value - target : target - value;
}

[[nodiscard]] bool
is_scalar_column(const sanitize::ColumnPlan &column) noexcept {
  const auto kind = column.logical_type.kind;
  return !column.has_variant_sibling &&
         kind != sanitize::LogicalKind::kStruct &&
         kind != sanitize::LogicalKind::kList;
}

[[nodiscard]] bool
is_fixed_width_dominant(const sanitize::CompiledPlan &plan) noexcept {
  const auto utf8_columns = static_cast<std::size_t>(std::count_if(
      plan.columns.begin(), plan.columns.end(), [](const auto &column) {
        return column.logical_type.kind == sanitize::LogicalKind::kUtf8;
      }));
  return utf8_columns * 4 < plan.columns.size();
}

[[nodiscard]] ColumnPartitionFailure strict_failure(std::size_t row_index,
                                                    std::string field) {
  ColumnPartitionFailure failure;
  failure.status = sanitize::Status::Invalid(
      "Strict schema evolution: observed extra field '", field, "'");
  failure.source_row_index = row_index;
  // Serial direct conversion performs strict row validation before column 0.
  failure.column_order = 0;
  failure.present = true;
  return failure;
}

} // namespace

ColumnPartitionInput::ColumnPartitionInput(
    std::shared_ptr<PoolResource> input_resource)
    : resource(std::move(input_resource)), field_indices(resource.get()) {}

std::span<const std::int32_t>
ColumnPartitionInput::row_field_indices(std::size_t row_index) const noexcept {
  if (column_count == 0 || row_index >= owned.rows.size()) {
    return {};
  }
  const auto offset = row_index * column_count;
  if (offset > field_indices.size() ||
      column_count > field_indices.size() - offset) {
    return {};
  }
  return std::span<const std::int32_t>(field_indices.data() + offset,
                                       column_count);
}

bool is_column_partition_candidate(
    const sanitize::CompiledPlan &plan) noexcept {
  return plan.columns.size() >= kMinimumPartitionColumns &&
         is_fixed_width_dominant(plan) &&
         std::ranges::all_of(plan.columns, &is_scalar_column);
}

bool column_partition_enabled(const sanitize::CompiledPlan &plan,
                              const sanitize::PreparedOptions &opts) noexcept {
  return opts.spec.on_error == sanitize::OnErrorPolicy::kStop &&
         is_column_partition_candidate(plan);
}

bool should_use_column_partition(const sanitize::CompiledPlan &plan,
                                 const sanitize::PreparedOptions &opts,
                                 std::int64_t effective_workers,
                                 std::int64_t expected_rows,
                                 std::int64_t input_size_hint_bytes) noexcept {
  if (!column_partition_enabled(plan, opts)) {
    return false;
  }
  const auto workers = std::max<std::int64_t>(1, effective_workers);
  const auto micro_rows = std::clamp<std::int64_t>(workers * 8, 64, 128);
  const bool micro_by_rows = expected_rows > 0 && expected_rows <= micro_rows;
  // Wide scalar JSON rows commonly occupy a few KiB. Either trustworthy hint
  // may retain the microload path; unknown streams prefer row parallelism so
  // serial plan-ordering cannot dominate an unbounded operation.
  constexpr std::int64_t kEstimatedWideJsonRowBytes = 4096;
  const bool micro_by_bytes =
      input_size_hint_bytes > 0 &&
      input_size_hint_bytes <= micro_rows * kEstimatedWideJsonRowBytes;
  return micro_by_rows || micro_by_bytes;
}

std::size_t column_partition_packet_window(std::size_t worker_count,
                                           std::size_t group_count) noexcept {
  const auto groups = std::max<std::size_t>(1, group_count);
  const auto workers = std::max<std::size_t>(1, worker_count);
  const auto packets = 1 + (workers - 1) / groups;
  return workers >= 16 ? 2 : std::min<std::size_t>(2, packets);
}

sanitize::Result<std::vector<ColumnPartitionRange>>
make_column_partition_ranges(const sanitize::CompiledPlan &plan,
                             std::size_t worker_count) {
  if (!is_column_partition_candidate(plan)) {
    return sanitize::Status::Invalid(
        "make_column_partition_ranges: plan is not partitionable");
  }
  const auto columns = plan.columns.size();
  const auto useful_groups = std::max<std::size_t>(
      1, std::min({worker_count, columns / kMinimumColumnsPerGroup,
                   kMaximumColumnGroups}));
  std::vector<ColumnPartitionRange> ranges;
  std::vector<std::size_t> prefix_cost;
  std::size_t first = 0;
  try {
    ranges.reserve(useful_groups);
    prefix_cost.reserve(columns + 1);
    prefix_cost.push_back(0);
    for (const auto &column : plan.columns) {
      prefix_cost.push_back(saturating_add_cost(
          prefix_cost.back(), column_conversion_cost(column)));
    }

    for (std::size_t group = 0; group < useful_groups; ++group) {
      const auto groups_after = useful_groups - group - 1;
      if (groups_after == 0) {
        const auto cost = range_cost(prefix_cost, first, columns);
        ranges.push_back(ColumnPartitionRange{
            .first_column = first,
            .column_count = columns - first,
            .estimated_cost = cost,
        });
        first = columns;
        continue;
      }

      const auto minimum_end = first + kMinimumColumnsPerGroup;
      const auto maximum_end = columns - groups_after * kMinimumColumnsPerGroup;
      const auto remaining_cost = range_cost(prefix_cost, first, columns);
      const auto groups_remaining = useful_groups - group;
      const auto target = remaining_cost / groups_remaining +
                          (remaining_cost % groups_remaining != 0 ? 1U : 0U);

      auto end = minimum_end;
      while (end < maximum_end &&
             range_cost(prefix_cost, first, end) < target) {
        ++end;
      }
      if (end > minimum_end) {
        const auto previous = end - 1;
        const auto current_distance =
            distance_from_target(range_cost(prefix_cost, first, end), target);
        const auto previous_distance = distance_from_target(
            range_cost(prefix_cost, first, previous), target);
        if (previous_distance <= current_distance) {
          end = previous;
        }
      }
      end = std::clamp(end, minimum_end, maximum_end);
      ranges.push_back(ColumnPartitionRange{
          .first_column = first,
          .column_count = end - first,
          .estimated_cost = range_cost(prefix_cost, first, end),
      });
      first = end;
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "make_column_partition_ranges: allocation failed");
  }
  if (first != columns || ranges.size() != useful_groups ||
      std::ranges::any_of(ranges, [](const auto &range) {
        return range.column_count < kMinimumColumnsPerGroup ||
               range.estimated_cost == 0;
      })) {
    return sanitize::Status::Invalid(
        "make_column_partition_ranges: invalid balanced partition");
  }
  return ranges;
}

sanitize::Result<std::shared_ptr<const sanitize::CompiledPlan>>
make_column_partition_plan(const sanitize::CompiledPlan &plan,
                           const ColumnPartitionRange &range) {
  if (range.column_count == 0 || range.first_column > plan.columns.size() ||
      range.column_count > plan.columns.size() - range.first_column) {
    return sanitize::Status::Invalid(
        "make_column_partition_plan: invalid column range");
  }

  sanitize::LogicalSchema schema;
  try {
    schema.fields.reserve(range.column_count);
    for (std::size_t offset = 0; offset < range.column_count; ++offset) {
      const auto &column = plan.columns[range.first_column + offset];
      sanitize::LogicalField field;
      field.name = column.name;
      field.nullable = column.nullable;
      field.type = std::make_unique<sanitize::LogicalType>(column.logical_type);
      schema.fields.push_back(std::move(field));
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "make_column_partition_plan: schema allocation failed");
  }

  SAN_ASSIGN_OR_RAISE(auto compiled, sanitize::compile_plan(schema));
  try {
    return std::shared_ptr<const sanitize::CompiledPlan>(
        std::make_shared<sanitize::CompiledPlan>(std::move(compiled)));
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "make_column_partition_plan: plan allocation failed");
  }
}

sanitize::Result<std::shared_ptr<const ColumnPartitionInput>>
make_column_partition_input(OwnedRowPacket &&owned,
                            const sanitize::CompiledPlan &plan,
                            const sanitize::PreparedOptions &opts,
                            std::shared_ptr<PoolResource> resource) {
  if (!column_partition_enabled(plan, opts) || owned.rows.empty()) {
    return std::shared_ptr<const ColumnPartitionInput>{};
  }
  if (!resource) {
    return sanitize::Status::Invalid(
        "make_column_partition_input: resource is null");
  }

  std::shared_ptr<ColumnPartitionInput> input;
  try {
    std::pmr::polymorphic_allocator<ColumnPartitionInput> allocator(
        resource.get());
    input = std::allocate_shared<ColumnPartitionInput>(allocator, resource);
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "make_column_partition_input: owner allocation failed");
  }
  input->column_count = plan.columns.size();
  input->plan_ordered = std::ranges::all_of(owned.rows, [](const auto &row) {
    return (row.flags & std::to_underlying(RowFlags::kPlanOrdered)) != 0;
  });

  RowFieldSnapshot snapshot;
  try {
    if (!input->plan_ordered) {
      if (owned.rows.size() >
          std::numeric_limits<std::size_t>::max() / input->column_count) {
        return sanitize::Status::OutOfMemory(
            "make_column_partition_input: index matrix is too large");
      }
      input->field_indices.reserve(owned.rows.size() * input->column_count);
    }

    for (std::size_t row_index = 0; row_index < owned.rows.size();
         ++row_index) {
      const auto &row = owned.rows[row_index];
      if ((row.flags & std::to_underlying(RowFlags::kRawOnly)) != 0 ||
          !row.fields) {
        return std::shared_ptr<const ColumnPartitionInput>{};
      }
      if (input->plan_ordered && row.size < input->column_count) {
        return sanitize::Status::Invalid(
            "make_column_partition_input: truncated plan-ordered row");
      }

      if (opts.spec.arrow_schema_contract &&
          opts.spec.schema_evolution ==
              sanitize::SchemaEvolutionMode::kStrict) {
        FieldLookup lookup{&row};
        std::string extra;
        SAN_ASSIGN_OR_RAISE(
            bool has_unplanned,
            lookup.has_unplanned_field(plan.root_layout, opts, &extra));
        if (has_unplanned &&
            (!input->row_validation_failure.present ||
             row_index < input->row_validation_failure.source_row_index)) {
          input->row_validation_failure = strict_failure(row_index, extra);
        }
      }

      if (input->plan_ordered) {
        // Plan-ordered rows are emitted only after the frontend parsed every
        // field and ruled out non-empty nested values. No second row x column
        // validation pass is required here.
        continue;
      }

      SAN_RETURN_NOT_OK(snapshot.build(row, plan, opts));
      for (std::size_t column = 0; column < input->column_count; ++column) {
        const auto field_index = snapshot.column_field_indices[column];
        if (field_index >= 0) {
          const auto source_index = static_cast<std::size_t>(field_index);
          if (source_index >= row.size) {
            return sanitize::Status::Invalid(
                "make_column_partition_input: invalid field snapshot");
          }
          const auto value = row.fields[source_index].value;
          if (value.is_object() || value.is_array()) {
            return std::shared_ptr<const ColumnPartitionInput>{};
          }
        }
        input->field_indices.push_back(field_index);
      }
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "make_column_partition_input: allocation failed");
  }

  input->owned = std::move(owned);
  return std::shared_ptr<const ColumnPartitionInput>(std::move(input));
}

sanitize::Status merge_column_partition_arrays(
    std::span<std::shared_ptr<sanitize::CArrayGuard>> groups,
    const std::shared_ptr<PoolResource> &pool, ArrowArray *out) {
  if (!pool || !out || groups.empty()) {
    return sanitize::Status::Invalid(
        "merge_column_partition_arrays: invalid arguments");
  }

  int64_t length = -1;
  std::size_t child_count = 0;
  for (const auto &guard : groups) {
    if (!guard) {
      return sanitize::Status::Invalid(
          "merge_column_partition_arrays: null group");
    }
    const auto &array = guard->value();
    if (!array.release || array.length < 0 || array.offset != 0 ||
        array.n_children <= 0 || !array.children ||
        array.release != &array_release) {
      return sanitize::Status::Invalid(
          "merge_column_partition_arrays: incompatible group array");
    }
    if (length < 0) {
      length = array.length;
    } else if (array.length != length) {
      return sanitize::Status::Invalid(
          "merge_column_partition_arrays: row count mismatch");
    }
    const auto count = static_cast<std::size_t>(array.n_children);
    if (count > std::numeric_limits<std::size_t>::max() - child_count) {
      return sanitize::Status::OutOfMemory(
          "merge_column_partition_arrays: child count overflow");
    }
    child_count += count;
  }

  auto payload = make_array_payload(pool);
  if (!payload) {
    return sanitize::Status::OutOfMemory(
        "merge_column_partition_arrays: payload allocation failed");
  }
  try {
    payload->buffers.assign(1, nullptr);
    payload->children.reserve(child_count);
    for (auto &guard : groups) {
      auto *array = guard->get();
      auto *source = static_cast<ArrayPayload *>(array->private_data);
      if (!source || source->children.size() !=
                         static_cast<std::size_t>(array->n_children)) {
        return sanitize::Status::Invalid(
            "merge_column_partition_arrays: invalid owned payload");
      }
      for (auto *&child : source->children) {
        if (!child) {
          return sanitize::Status::Invalid(
              "merge_column_partition_arrays: null owned child");
        }
        payload->children.push_back(child);
        child = nullptr;
      }
      cdata_stream::release_array_nothrow(array);
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "merge_column_partition_arrays: allocation failed");
  }

  std::memset(out, 0, sizeof(*out));
  out->length = std::max<int64_t>(0, length);
  out->null_count = 0;
  out->offset = 0;
  out->n_buffers = 1;
  out->n_children = static_cast<int64_t>(payload->children.size());
  out->buffers = payload->buffers.data();
  out->children = payload->children.data();
  out->private_data = payload.release();
  out->release = &array_release;
  return sanitize::Status::OK();
}

bool column_partition_failure_precedes(
    const ColumnPartitionFailure &candidate,
    const ColumnPartitionFailure &current) noexcept {
  if (!candidate.present) {
    return false;
  }
  if (!current.present) {
    return true;
  }
  if (candidate.source_row_index != current.source_row_index) {
    return candidate.source_row_index < current.source_row_index;
  }
  return candidate.column_order < current.column_order;
}

} // namespace sanitize::internal

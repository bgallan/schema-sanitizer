// Implements bounded packet sizing for parallel materialization.

#include "internal/materialization/ingest_stream/parallel_packets.hh"
#include "internal/parsing/json/validated_row.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <new>

namespace sanitize::internal {
namespace {

constexpr std::size_t kRowAccountingOverhead = 64;
constexpr std::size_t kValueAccountingOverhead = 96;
constexpr std::size_t kMaxValueAccountingDepth = 64;
constexpr std::int64_t kHighCoreWideFlatScoreLimit = 96;

[[nodiscard]] constexpr std::int64_t
sustained_wide_flat_worker_ceiling(std::int64_t effective_workers,
                                   std::int64_t score) noexcept {
  const auto workers = std::max<std::int64_t>(1, effective_workers);
  if (score > kHighCoreWideFlatScoreLimit) {
    return std::min<std::int64_t>(workers,
                                  std::max<std::int64_t>(4, workers / 8));
  }
  return std::min<std::int64_t>(workers,
                                std::max<std::int64_t>(4, workers / 2));
}

static_assert(sustained_wide_flat_worker_ceiling(8, 64) == 4);
static_assert(sustained_wide_flat_worker_ceiling(16, 64) == 8);
static_assert(sustained_wide_flat_worker_ceiling(32, 64) == 16);
static_assert(sustained_wide_flat_worker_ceiling(32, 128) == 4);
static_assert(sustained_wide_flat_worker_ceiling(64, 64) == 32);
static_assert(sustained_wide_flat_worker_ceiling(64, 128) == 8);

[[nodiscard]] constexpr std::int64_t
scaled_worker_baseline(std::int64_t effective_workers,
                       std::int64_t baseline_at_32) noexcept {
  const auto workers = std::max<std::int64_t>(1, effective_workers);
  if (workers <= 32) {
    return std::min(workers, baseline_at_32);
  }
  return std::min(workers, std::max<std::int64_t>(
                               baseline_at_32,
                               (workers * baseline_at_32 + std::int64_t{31}) /
                                   std::int64_t{32}));
}

static_assert(scaled_worker_baseline(32, 8) == 8);
static_assert(scaled_worker_baseline(64, 8) == 16);
static_assert(scaled_worker_baseline(128, 16) == 64);

[[nodiscard]] std::int64_t saturating_add_score(std::int64_t left,
                                                std::int64_t right) noexcept {
  const auto max = std::numeric_limits<std::int64_t>::max();
  return right > max - left ? max : left + right;
}

[[nodiscard]] std::int64_t
saturating_multiply_score(std::int64_t value, std::int64_t factor) noexcept {
  const auto max = std::numeric_limits<std::int64_t>::max();
  if (value <= 0 || factor <= 0) {
    return 0;
  }
  return value > max / factor ? max : value * factor;
}

[[nodiscard]] std::int64_t
logical_materialization_score(const LogicalType &type) noexcept {
  switch (type.kind) {
  case LogicalKind::kNull:
  case LogicalKind::kBool:
  case LogicalKind::kInt64:
  case LogicalKind::kFloat64:
    return 1;
  case LogicalKind::kUtf8:
    return 2;
  case LogicalKind::kTimestampNs:
  case LogicalKind::kDate32:
  case LogicalKind::kTime32s:
    return 3;
  case LogicalKind::kList:
    return saturating_add_score(
        8, type.value ? saturating_multiply_score(
                            logical_materialization_score(*type.value), 2)
                      : 2);
  case LogicalKind::kStruct: {
    std::int64_t score = 4;
    for (const auto &field : type.fields) {
      if (field.type) {
        score = saturating_add_score(
            score, logical_materialization_score(*field.type));
      }
    }
    return score;
  }
  }
  return 1;
}

[[nodiscard]] bool is_wide_flat_plan(const CompiledPlan &plan) noexcept {
  constexpr std::size_t kWideFlatColumnThreshold = 24;
  if (plan.columns.size() < kWideFlatColumnThreshold) {
    return false;
  }
  return std::none_of(
      plan.columns.begin(), plan.columns.end(), [](const auto &column) {
        const auto kind = column.logical_type.kind;
        return kind == LogicalKind::kList || kind == LogicalKind::kStruct;
      });
}

[[nodiscard]] bool
is_variable_width_heavy_flat_plan(const CompiledPlan &plan) noexcept {
  if (plan.columns.empty()) {
    return false;
  }
  const auto utf8_columns = static_cast<std::size_t>(std::count_if(
      plan.columns.begin(), plan.columns.end(), [](const auto &column) {
        return column.logical_type.kind == LogicalKind::kUtf8;
      }));
  return utf8_columns * 4 >= plan.columns.size();
}

[[nodiscard]] std::int64_t
plan_materialization_score(const CompiledPlan &plan) noexcept {
  std::int64_t score = 0;
  for (const auto &column : plan.columns) {
    score = saturating_add_score(
        score, logical_materialization_score(column.logical_type));
    if (column.has_variant_sibling) {
      score = saturating_add_score(score, 1);
    }
  }
  return std::max<std::int64_t>(1, score);
}

[[nodiscard]] std::size_t saturating_add(std::size_t left,
                                         std::size_t right) noexcept {
  const auto max = std::numeric_limits<std::size_t>::max();
  return right > max - left ? max : left + right;
}

struct ValueAccounting {
  std::size_t bytes = 0;
  std::size_t cap = 1;
  bool saturated = false;

  void add(std::size_t amount) noexcept {
    if (saturated) {
      return;
    }
    bytes = saturating_add(bytes, amount);
    if (bytes >= cap) {
      bytes = cap;
      saturated = true;
    }
  }
};

sanitize::Status account_value(const ValueView &value, std::size_t depth,
                               ValueAccounting *accounting) {
  accounting->add(kValueAccountingOverhead);
  if (accounting->saturated) {
    return sanitize::Status::OK();
  }
  if (value.is_string()) {
    accounting->add(value.as_string_view().size());
    return sanitize::Status::OK();
  }
  if ((!value.is_array() && !value.is_object()) ||
      depth >= kMaxValueAccountingDepth) {
    if (depth >= kMaxValueAccountingDepth &&
        (value.is_array() || value.is_object())) {
      accounting->add(accounting->cap);
    }
    return sanitize::Status::OK();
  }

  bool stopped_at_cap = false;
  sanitize::Status status = sanitize::Status::OK();
  if (value.is_array()) {
    status =
        value.for_each_array_element([&](ValueView child) -> sanitize::Status {
          if (accounting->saturated) {
            stopped_at_cap = true;
            return sanitize::Status::Cancelled(
                "materialization packet accounting reached its cap");
          }
          return account_value(child, depth + 1, accounting);
        });
  } else {
    status =
        value.for_each_object_field([&](std::string_view key, std::uint64_t,
                                        ValueView child) -> sanitize::Status {
          accounting->add(sizeof(FieldRef));
          accounting->add(key.size());
          if (accounting->saturated) {
            stopped_at_cap = true;
            return sanitize::Status::Cancelled(
                "materialization packet accounting reached its cap");
          }
          return account_value(child, depth + 1, accounting);
        });
  }

  if (stopped_at_cap || accounting->saturated) {
    accounting->bytes = accounting->cap;
    accounting->saturated = true;
    return sanitize::Status::OK();
  }
  if (!status.ok()) {
    // Accounting is deliberately best effort. If a frontend cannot safely
    // expose a container twice, force this row into its own packet and let the
    // real materializer report the authoritative conversion error later.
    accounting->bytes = accounting->cap;
    accounting->saturated = true;
  }
  return sanitize::Status::OK();
}

[[nodiscard]] std::size_t
estimate_row_source_bytes(const RowRef &row,
                          std::size_t accounting_cap) noexcept {
  ValueAccounting accounting{.bytes = 0,
                             .cap = std::max<std::size_t>(1, accounting_cap)};
  accounting.add(sizeof(RowRef));
  accounting.add(kRowAccountingOverhead);
  accounting.add(row.source_file.size());

  if (!row.raw.empty()) {
    accounting.add(row.raw.size());
    return accounting.bytes;
  }

  try {
    for (std::size_t index = 0; index < row.size && !accounting.saturated;
         ++index) {
      const auto &field = row.fields[index];
      accounting.add(sizeof(FieldRef));
      accounting.add(field.key.size());
      const auto status = account_value(field.value, 0, &accounting);
      if (!status.ok()) {
        accounting.bytes = accounting.cap;
        accounting.saturated = true;
      }
    }
  } catch (...) {
    // Estimation must never change user-visible error ordering. Isolate the row
    // and defer any real failure to the worker's normal conversion path.
    accounting.bytes = accounting.cap;
    accounting.saturated = true;
  }
  return accounting.bytes;
}

} // namespace

ExecutionPolicy materialization_execution_policy(
    const CompiledPlan &plan, const ExecutionPolicy &policy,
    std::int64_t expected_rows, std::int64_t input_size_hint_bytes) noexcept {
  if (policy.effective_workers <= 1) {
    return policy;
  }

  const auto score = plan_materialization_score(plan);
  constexpr std::int64_t kShortOperationRowLimit = 131072;
  constexpr std::int64_t kShortInputByteLimit = 2 * kMinimumWorkerArenaBytes;
  constexpr std::int64_t kWideShortOperationRowLimit = 32768;
  constexpr std::int64_t kWideShortInputByteLimit =
      2 * kMinimumWorkerArenaBytes;
  const bool short_by_rows =
      expected_rows > 0 && expected_rows <= kShortOperationRowLimit;
  const bool short_by_bytes = expected_rows <= 0 && input_size_hint_bytes > 0 &&
                              input_size_hint_bytes <= kShortInputByteLimit;
  const bool short_operation = short_by_rows || short_by_bytes;
  const bool wide_flat = is_wide_flat_plan(plan);
  const bool wide_short_by_rows =
      expected_rows > 0 && expected_rows <= kWideShortOperationRowLimit;
  const bool wide_short_by_bytes =
      expected_rows <= 0 && input_size_hint_bytes > 0 &&
      input_size_hint_bytes <= kWideShortInputByteLimit;
  const bool wide_short_operation = wide_short_by_rows || wide_short_by_bytes;
  const bool variable_width_heavy =
      wide_flat && is_variable_width_heavy_flat_plan(plan);
  std::int64_t useful_workers = policy.effective_workers;
  if (wide_flat) {
    if (!wide_short_operation) {
      useful_workers =
          sustained_wide_flat_worker_ceiling(policy.effective_workers, score);
    } else if (variable_width_heavy) {
      useful_workers = scaled_worker_baseline(policy.effective_workers, 8);
    } else if (score <= kHighCoreWideFlatScoreLimit) {
      useful_workers = policy.effective_workers;
    } else {
      useful_workers = scaled_worker_baseline(policy.effective_workers, 16);
    }
  } else if (score <= 4) {
    useful_workers = scaled_worker_baseline(policy.effective_workers, 2);
  } else if (score <= 48) {
    useful_workers = scaled_worker_baseline(policy.effective_workers,
                                            short_operation ? 9 : 8);
  } else if (score <= 96) {
    useful_workers = scaled_worker_baseline(policy.effective_workers, 8);
  } else {
    useful_workers = scaled_worker_baseline(policy.effective_workers, 16);
  }

  const auto worker_ceiling = std::max<std::int64_t>(2, useful_workers);
  auto out = execution_policy_with_worker_ceiling(policy, worker_ceiling);
  if (wide_flat && !variable_width_heavy && wide_short_operation &&
      out.effective_workers >= 12) {
    out.materialization_packet_target_bytes = std::max<std::int64_t>(
        128 * 1024, out.materialization_packet_target_bytes / 8);
    out.materialization_packet_max_rows =
        std::min<std::int64_t>(out.materialization_packet_max_rows, 2048);
  }
  return out;
}

ExecutionPolicy jsonl_row_parallel_execution_policy(
    const ExecutionPolicy &policy, std::int64_t expected_rows,
    std::int64_t input_size_hint_bytes) noexcept {
  auto out = execution_policy_with_worker_ceiling(
      policy,
      std::min(policy.effective_workers,
               std::max<std::int64_t>(16, policy.effective_workers / 2)));
  const auto workers = std::max<std::int64_t>(1, out.effective_workers);
  const auto desired_packets = std::max<std::int64_t>(
      1, std::min<std::int64_t>(out.task_queue_capacity, workers * 2));
  if (input_size_hint_bytes > 0) {
    const auto bytes_per_packet = std::max<std::int64_t>(
        16 * 1024,
        (input_size_hint_bytes + desired_packets - 1) / desired_packets);
    out.materialization_packet_target_bytes =
        std::min(out.materialization_packet_target_bytes, bytes_per_packet);
  }
  if (expected_rows > 0) {
    const auto rows_per_packet = std::max<std::int64_t>(
        1, (expected_rows + desired_packets - 1) / desired_packets);
    out.materialization_packet_max_rows =
        std::min(out.materialization_packet_max_rows, rows_per_packet);
  }
  return out;
}

MaterializationPacketLimits
materialization_packet_limits(const ExecutionPolicy &policy,
                              std::int64_t observed_bytes_per_row) noexcept {
  const auto target = static_cast<std::size_t>(
      std::max<std::int64_t>(1, policy.materialization_packet_target_bytes));
  const auto policy_max_rows = static_cast<std::size_t>(
      std::max<std::int64_t>(1, policy.materialization_packet_max_rows));
  const auto row_bytes = static_cast<std::size_t>(
      std::max<std::int64_t>(1, observed_bytes_per_row));
  const auto rows_from_observation =
      std::max<std::size_t>(1, target / row_bytes);
  return MaterializationPacketLimits{
      .max_rows = std::min(policy_max_rows, rows_from_observation),
      .target_bytes = target,
  };
}

sanitize::Result<std::shared_ptr<OwnedRowBatch>>
make_owned_row_batch(std::vector<RowRef> rows,
                     std::shared_ptr<const void> source_owner) {
  try {
    return std::make_shared<OwnedRowBatch>(OwnedRowBatch{
        .rows = std::move(rows), .source_owner = std::move(source_owner)});
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "make_owned_row_batch: allocation failed");
  }
}

sanitize::Result<OwnedRowPacket>
build_owned_row_packet(const std::shared_ptr<OwnedRowBatch> &batch_owner,
                       std::size_t start, MaterializationPacketLimits limits) {
  if (!batch_owner || start >= batch_owner->rows.size()) {
    return sanitize::Status::Invalid(
        "build_owned_row_packet: start index is out of range");
  }
  limits.max_rows = std::max<std::size_t>(1, limits.max_rows);
  limits.target_bytes = std::max<std::size_t>(1, limits.target_bytes);

  OwnedRowPacket packet;
  packet.owner = batch_owner;
  auto &rows = batch_owner->rows;
  const auto remaining = rows.size() - start;
  const auto row_limit = std::min(remaining, limits.max_rows);
  std::size_t row_count = 0;
  for (; row_count < row_limit; ++row_count) {
    const auto &row = rows[start + row_count];
    const auto row_bytes = estimate_row_source_bytes(row, limits.target_bytes);
    const auto next_bytes =
        saturating_add(packet.estimated_source_bytes, row_bytes);
    if (row_count != 0 && next_bytes > limits.target_bytes) {
      break;
    }
    if ((row.flags & std::to_underlying(RowFlags::kRawOnly)) != 0) {
      if (const auto *tokens = json_validated_row_tokens(row)) {
        ++packet.json_tokenized_rows;
        packet.json_tokenized_fields += tokens->field_count;
        if (json_validated_row_tokens_are_plan_ordered(row)) {
          ++packet.json_plan_ordered_rows;
        }
      } else {
        ++packet.json_token_fallback_rows;
      }
    }
    packet.estimated_source_bytes = next_bytes;
    if (packet.estimated_source_bytes >= limits.target_bytes) {
      ++row_count;
      break;
    }
  }
  if (row_count == 0) {
    return sanitize::Status::Invalid(
        "build_owned_row_packet: failed to retain the first row");
  }
  packet.rows = std::span<RowRef>(rows.data() + start, row_count);
  return packet;
}

} // namespace sanitize::internal

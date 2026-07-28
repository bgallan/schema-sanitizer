// Implements JSON Lines serialization for Arrow C streams.

#include "internal/json_output/jsonl_stream_writer.hh"

#include "internal/arrow_c/cdata_stream_runtime.hh"
#include "internal/json_encoding/token_writer.hh"
#include "internal/json_output/jsonl_value_writer.hh"
#include "internal/json_output/schema/model.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/output/ordered_text_output.hh"
#include "internal/output/text_output_estimator.hh"
#include "internal/runtime/execution_policy.hh"
#include "sanitize/abi/cdata_types.hh"

#include <algorithm>
#include <cstdint>
#include <stop_token>
#include <string>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

constexpr std::size_t kFlushThresholdBytes = 1U << 20;
constexpr std::size_t kMaxRetainedOutputBytes = 4U << 20;

constexpr std::int64_t kMinimumWideFlatJsonlWorkers = 4;
constexpr std::int64_t kMaximumWideFlatJsonlWorkers = 8;
constexpr std::size_t kWideFlatJsonlFieldThreshold = 24;
constexpr std::int64_t kNestedJsonlWorkItemsPerWorker = 16;

[[nodiscard]] bool is_nested_output_kind(JsonlKind kind) noexcept {
  switch (kind) {
  case JsonlKind::kStruct:
  case JsonlKind::kList:
  case JsonlKind::kLargeList:
  case JsonlKind::kFixedSizeList:
  case JsonlKind::kMap:
  case JsonlKind::kDictionary:
    return true;
  default:
    return false;
  }
}

[[nodiscard]] bool has_nested_output(const JsonlField &field) noexcept {
  return std::any_of(field.children.begin(), field.children.end(),
                     [](const JsonlField &child) {
                       return is_nested_output_kind(child.kind) ||
                              has_nested_output(child);
                     });
}

[[nodiscard]] bool is_wide_flat_schema(const JsonlField &root) noexcept {
  return root.kind == JsonlKind::kStruct &&
         root.children.size() >= kWideFlatJsonlFieldThreshold &&
         std::none_of(root.children.begin(), root.children.end(),
                      [](const JsonlField &field) {
                        return is_nested_output_kind(field.kind);
                      });
}

[[nodiscard]] bool is_wide_fixed_flat_schema(const JsonlField &root) noexcept {
  return is_wide_flat_schema(root) &&
         std::all_of(
             root.children.begin(), root.children.end(),
             [](const JsonlField &field) {
               return text_output_estimator::fixed_cost_jsonl_scalar_kind(
                   field.kind);
             });
}

[[nodiscard]] constexpr std::int64_t
wide_flat_worker_ceiling_for(std::int64_t operation_workers) noexcept {
  const auto half_arena = std::max<std::int64_t>(
      1, std::max<std::int64_t>(1, operation_workers) / 2);
  return std::clamp<std::int64_t>(half_arena, kMinimumWideFlatJsonlWorkers,
                                  kMaximumWideFlatJsonlWorkers);
}

static_assert(wide_flat_worker_ceiling_for(4) == 4);
static_assert(wide_flat_worker_ceiling_for(8) == 4);
static_assert(wide_flat_worker_ceiling_for(16) == 8);

[[nodiscard]] constexpr bool
should_scale_wide_fixed_output(bool wide_fixed_flat,
                               std::int64_t operation_workers) noexcept {
  return wide_fixed_flat && operation_workers > 8;
}

static_assert(!should_scale_wide_fixed_output(true, 8));
static_assert(should_scale_wide_fixed_output(true, 16));
static_assert(!should_scale_wide_fixed_output(false, 16));

class JsonlRowEstimator final {
public:
  explicit JsonlRowEstimator(const JsonlField &root) noexcept
      : root_(&root),
        fixed_row_estimate_(
            text_output_estimator::estimate_wide_fixed_jsonl_row_upper_bound(
                root)) {}

  void prepare(const ArrowArray &array) noexcept {
    use_fixed_estimate_ =
        fixed_row_estimate_ > 0 &&
        text_output_estimator::wide_fixed_jsonl_batch_has_no_nulls(*root_,
                                                                   array);
  }

  [[nodiscard]] std::int64_t operator()(const ArrowArray &array,
                                        std::int64_t row,
                                        std::int64_t cap) const noexcept {
    if (use_fixed_estimate_) {
      return std::min(fixed_row_estimate_, cap);
    }
    return text_output_estimator::estimate_jsonl_row_bytes(*root_, array, row,
                                                           cap);
  }

private:
  const JsonlField *root_;
  std::int64_t fixed_row_estimate_;
  bool use_fixed_estimate_ = false;
};

void clear_output_buffer(std::string &buffer) noexcept {
  if (secure_memory_cleanup_enabled() && !buffer.empty()) {
    secure_zero_memory(buffer.data(), buffer.size());
  }
  if (buffer.capacity() > kMaxRetainedOutputBytes) {
    std::string empty;
    buffer.swap(empty);
  } else {
    buffer.clear();
  }
}

class ScopedStringWipe final {
public:
  explicit ScopedStringWipe(std::string *value) noexcept : value_(value) {}
  ~ScopedStringWipe() {
    if (value_ && secure_memory_cleanup_enabled() && !value_->empty()) {
      secure_zero_memory(value_->data(), value_->size());
    }
  }

  ScopedStringWipe(const ScopedStringWipe &) = delete;
  ScopedStringWipe &operator=(const ScopedStringWipe &) = delete;

private:
  std::string *value_;
};

sanitize::Status flush_buffer(Output &out_file, std::string &buffer) {
  if (buffer.empty()) {
    return sanitize::Status::OK();
  }
  auto status = out_file.Write(buffer);
  clear_output_buffer(buffer);
  return status;
}

sanitize::Status flush_buffer_if_large(Output &out_file, std::string &buffer) {
  if (buffer.size() < kFlushThresholdBytes) {
    return sanitize::Status::OK();
  }
  return flush_buffer(out_file, buffer);
}

sanitize::Status append_rows_jsonl(const JsonlField &root,
                                   const ArrowArray &array,
                                   std::int64_t first_row,
                                   std::int64_t row_count, std::stop_token stop,
                                   std::string *out) {
  if (!out || first_row < 0 || row_count < 0 || first_row > array.length ||
      row_count > array.length - first_row) {
    return sanitize::Status::Invalid(
        "JSONL writer: invalid output packet range");
  }
  if (root.member_prefixes.size() != root.children.size()) {
    return sanitize::Status::Invalid(
        "JSONL writer: object member prefix/schema mismatch");
  }
  if (row_count > 0) {
    constexpr std::int64_t kDefaultRowReserve = 128;
    const auto reserve_size = static_cast<std::size_t>(
        std::min<std::int64_t>(row_count, 8192) * kDefaultRowReserve);
    out->reserve(reserve_size);
  }
  const auto end_row = first_row + row_count;
  for (std::int64_t row = first_row; row < end_row; ++row) {
    if ((row & 63) == 0 && stop.stop_requested()) {
      return sanitize::Status::Cancelled("JSONL output packet cancelled");
    }
    out->push_back('{');
    for (std::size_t col = 0; col < root.children.size(); ++col) {
      out->append(root.member_prefixes[col]);
      SAN_RETURN_NOT_OK(
          append_value(*out, root.children[col], *array.children[col], row));
    }
    out->append("}\n");
  }
  return sanitize::Status::OK();
}

sanitize::Status write_batch_jsonl(Output &out_file, const JsonlField &root,
                                   const ArrowArray &array,
                                   const ArrayValidationLimits &limits) {
  SAN_RETURN_NOT_OK(validate_batch(root, array, limits));
  std::string batch_text;
  ScopedStringWipe batch_text_wipe(&batch_text);
  SAN_RETURN_NOT_OK(
      append_rows_jsonl(root, array, 0, array.length, {}, &batch_text));
  SAN_RETURN_NOT_OK(flush_buffer_if_large(out_file, batch_text));
  return flush_buffer(out_file, batch_text);
}

} // namespace

sanitize::Status write_batch(Output &out_file, const ArrowSchema &schema,
                             const ArrowArray &array,
                             std::int64_t memory_limit_bytes) {
  SAN_ASSIGN_OR_RAISE(auto root, parse_schema_field(schema));
  const auto limits = array_validation_limits(memory_limit_bytes);
  SAN_RETURN_NOT_OK(write_batch_jsonl(out_file, root, array, limits));
  return out_file.Flush();
}

sanitize::Result<WriteStats>
write_stream(ArrowArrayStream *stream, Output &out_file,
             std::int64_t memory_limit_bytes,
             sanitize::ThreadingMode threading_mode) {
  if (!stream) {
    return sanitize::Status::Invalid("JSONL writer: Arrow C stream is null");
  }
  sanitize::CSchemaGuard schema;
  const int schema_rc = stream->get_schema(stream, schema.get());
  if (schema_rc != 0) {
    const char *detail =
        stream->get_last_error ? stream->get_last_error(stream) : nullptr;
    return sanitize::Status::IOError(
        "JSONL writer: get_schema failed",
        detail && *detail ? std::string(": ") + detail : std::string{});
  }
  SAN_ASSIGN_OR_RAISE(auto root, parse_schema_field(schema.value()));
  const auto limits = array_validation_limits(memory_limit_bytes);

  const auto wide_flat = is_wide_flat_schema(root);
  const auto wide_fixed_flat = is_wide_fixed_flat_schema(root);
  std::int64_t operation_workers = 1;
  if (wide_fixed_flat) {
    const auto task_arena = sanitize::internal::task_arena_for_stream(stream);
    operation_workers =
        task_arena ? static_cast<std::int64_t>(task_arena->worker_count())
                   : execution_policy_from(threading_mode, memory_limit_bytes)
                         .effective_workers;
  }
  const auto scale_wide_fixed_output =
      should_scale_wide_fixed_output(wide_fixed_flat, operation_workers);
  const auto output_worker_ceiling =
      scale_wide_fixed_output ? wide_flat_worker_ceiling_for(operation_workers)
      : wide_flat             ? kMinimumWideFlatJsonlWorkers
                  : ordered_text_output::kDefaultOutputWorkerCeiling;
  const auto reclaim_wide_variable_packet_window =
      wide_flat && !wide_fixed_flat;
  // Wide variable batches commonly contain only one or two output packets.
  // Per-batch admission would therefore narrow the shared output lane and
  // force a commit barrier before the next Arrow batch, even though v62
  // already bounds the complete reorder window. Admit the stage ceiling from
  // the first batch so adjacent batches can overlap without creating workers
  // or enlarging the memory-derived output policy.
  const auto admit_full_wide_variable_output =
      reclaim_wide_variable_packet_window;

  return ordered_text_output::write_stream<WriteStats>(
      stream, out_file, memory_limit_bytes, threading_mode, "JSONL writer",
      [&root, &limits](const ArrowArray &array) {
        return validate_batch(root, array, limits);
      },
      JsonlRowEstimator(root),
      [&root](const ordered_text_output::BatchPacket &packet, std::size_t,
              std::stop_token stop, std::string *out) {
        return append_rows_jsonl(root, packet.owner->value(), packet.first_row,
                                 packet.row_count, stop, out);
      },
      output_worker_ceiling, !wide_flat,
      has_nested_output(root) || (wide_flat && !scale_wide_fixed_output)
          ? TaskArenaLane::kUpstream
          : TaskArenaLane::kOutput,
      has_nested_output(root) ? kNestedJsonlWorkItemsPerWorker : 1,
      scale_wide_fixed_output || has_nested_output(root),
      scale_wide_fixed_output || admit_full_wide_variable_output,
      reclaim_wide_variable_packet_window);
}

} // namespace sanitize::internal::jsonl_stream_writer

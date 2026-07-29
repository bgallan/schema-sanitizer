// Implements CSV serialization for Arrow C streams.

#include "internal/csv/csv_stream_writer.hh"

#include "internal/json_output/jsonl_value_writer.hh"
#include "internal/json_output/jsonl_value_writer_parts.hh"
#include "internal/json_output/schema/model.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/output/csv_fixed_estimator.hh"
#include "internal/output/ordered_text_output.hh"
#include "internal/output/text_output_estimator.hh"
#include "internal/parsing/json/string_decode.hh"
#include "sanitize/abi/cdata_types.hh"

#include "internal/runtime/thread_compat.hh"
#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize::internal::csv_stream_writer {
namespace {

namespace jsonl = sanitize::internal::jsonl_stream_writer;

// Variable-width CSV uses a conservative fraction of the operation arena.
// Wide fixed-cost schemas publish enough uniform packets to use half of it.
// Both policies continue scaling beyond the historical 32-worker range.
inline constexpr std::int64_t kMinimumCsvOutputWorkers = 4;

[[nodiscard]] constexpr std::int64_t
csv_worker_ceiling_for(std::int64_t operation_workers,
                       bool wide_fixed) noexcept {
  const auto workers = std::max<std::int64_t>(1, operation_workers);
  const auto divisor = wide_fixed ? 2 : 8;
  return std::min(workers, std::max<std::int64_t>(kMinimumCsvOutputWorkers,
                                                  workers / divisor));
}

static_assert(csv_worker_ceiling_for(4, true) == 4);
static_assert(csv_worker_ceiling_for(8, true) == 4);
static_assert(csv_worker_ceiling_for(16, true) == 8);
static_assert(csv_worker_ceiling_for(32, true) == 16);
static_assert(csv_worker_ceiling_for(64, true) == 32);
static_assert(csv_worker_ceiling_for(32, false) == 4);
static_assert(csv_worker_ceiling_for(64, false) == 8);

class CsvRowEstimator final {
public:
  explicit CsvRowEstimator(const jsonl::JsonlField &root)
      : root_(&root),
        plan_(text_output_estimator::make_csv_fixed_estimate_plan(root)) {}

  void prepare(const ArrowArray &) noexcept {}

  [[nodiscard]] bool high_core_eligible() const noexcept {
    return plan_.eligible;
  }

  [[nodiscard]] std::size_t fixed_field_count() const noexcept {
    return plan_.fixed_fields;
  }

  [[nodiscard]] std::size_t dynamic_field_count() const noexcept {
    return plan_.dynamic_fields.size();
  }

  [[nodiscard]] std::int64_t operator()(const ArrowArray &array,
                                        std::int64_t row,
                                        std::int64_t cap) const noexcept {
    return text_output_estimator::estimate_csv_row_bytes_from_plan(
        plan_, *root_, array, row, cap);
  }

private:
  const jsonl::JsonlField *root_;
  text_output_estimator::CsvFixedEstimatePlan plan_;
};

constexpr std::size_t kMaxRetainedOutputBytes = 4U << 20;

void clear_decode_buffer(std::vector<char> &buffer) noexcept {
  if (secure_memory_cleanup_enabled() && !buffer.empty()) {
    secure_zero_memory(buffer.data(), buffer.size());
  }
  if (buffer.capacity() > kMaxRetainedOutputBytes) {
    std::vector<char> empty;
    buffer.swap(empty);
  } else {
    buffer.clear();
  }
}

class ScopedStringWipe final {
public:
  explicit ScopedStringWipe(TextBuffer *value) noexcept : value_(value) {}
  ~ScopedStringWipe() {
    if (value_ && secure_memory_cleanup_enabled() && !value_->empty()) {
      secure_zero_memory(value_->data(), value_->size());
    }
  }

  ScopedStringWipe(const ScopedStringWipe &) = delete;
  ScopedStringWipe &operator=(const ScopedStringWipe &) = delete;

private:
  TextBuffer *value_;
};

class ScopedDecodeBufferClear final {
public:
  explicit ScopedDecodeBufferClear(std::vector<char> *value) noexcept
      : value_(value) {}
  ~ScopedDecodeBufferClear() {
    if (value_) {
      clear_decode_buffer(*value_);
    }
  }

  ScopedDecodeBufferClear(const ScopedDecodeBufferClear &) = delete;
  ScopedDecodeBufferClear &operator=(const ScopedDecodeBufferClear &) = delete;

private:
  std::vector<char> *value_;
};

constexpr json_string_decode::DecodeErrors kCsvJsonDecodeErrors{
    .truncated_escape = "CSV writer: truncated JSON string escape",
    .incomplete_unicode_escape = "CSV writer: incomplete JSON unicode escape",
    .invalid_unicode_hex = "CSV writer: invalid JSON unicode escape",
    .missing_low_surrogate = "CSV writer: missing JSON low surrogate",
    .invalid_low_surrogate_hex =
        "CSV writer: invalid JSON low surrogate escape",
    .invalid_low_surrogate_range =
        "CSV writer: invalid JSON low surrogate range",
    .unexpected_low_surrogate = "CSV writer: unexpected JSON low surrogate",
    .invalid_escape = "CSV writer: invalid JSON string escape",
};

bool needs_csv_quotes(std::string_view value) {
  return value.find_first_of(",\"\r\n") != std::string_view::npos;
}

void append_csv_escaped(TextBuffer &out, std::string_view value) {
  if (!needs_csv_quotes(value)) {
    out.append(value);
    return;
  }
  out.push_back('"');
  for (const char ch : value) {
    if (ch == '"') {
      out += "\"\"";
    } else {
      out.push_back(ch);
    }
  }
  out.push_back('"');
}

bool is_json_string_literal(std::string_view value) {
  return value.size() >= 2 && value.front() == '"' && value.back() == '"';
}

[[nodiscard]] bool validity_bit_is_set(const std::uint8_t *bitmap,
                                       std::int64_t index) noexcept {
  return (bitmap[index >> 3] & static_cast<std::uint8_t>(1U << (index & 7))) !=
         0U;
}

[[nodiscard]] bool array_is_null(const ArrowArray &array,
                                 std::int64_t row) noexcept {
  if (array.null_count == 0 || !array.buffers || !array.buffers[0]) {
    return false;
  }
  const auto *bitmap = static_cast<const std::uint8_t *>(array.buffers[0]);
  return !validity_bit_is_set(bitmap, array.offset + row);
}

[[nodiscard]] bool is_direct_csv_scalar(jsonl::JsonlKind kind) noexcept {
  switch (kind) {
  case jsonl::JsonlKind::kBool:
  case jsonl::JsonlKind::kInt8:
  case jsonl::JsonlKind::kUInt8:
  case jsonl::JsonlKind::kInt16:
  case jsonl::JsonlKind::kUInt16:
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kUInt32:
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kUInt64:
  case jsonl::JsonlKind::kFloat16:
  case jsonl::JsonlKind::kFloat32:
  case jsonl::JsonlKind::kFloat64:
    return true;
  default:
    return false;
  }
}

sanitize::Status
append_direct_csv_logical_scalar(TextBuffer &out,
                                 const jsonl::JsonlField &field,
                                 const ArrowArray &array, std::int64_t row) {
  switch (field.kind) {
  case jsonl::JsonlKind::kBinary:
    return jsonl::append_binary32_value(out, array, row, false);
  case jsonl::JsonlKind::kLargeBinary:
    return jsonl::append_binary64_value(out, array, row, false);
  case jsonl::JsonlKind::kFixedSizeBinary:
    return jsonl::append_fixed_size_binary_value(out, field, array, row, false);
  case jsonl::JsonlKind::kTimestampMillis:
    return jsonl::append_timestamp_value(out, array, row, 1000, false);
  case jsonl::JsonlKind::kTimestampMicros:
    return jsonl::append_timestamp_value(out, array, row, 1000000, false);
  case jsonl::JsonlKind::kTimestampNanos:
    return jsonl::append_timestamp_value(out, array, row, 1000000000, false);
  case jsonl::JsonlKind::kDate32:
    return jsonl::append_date32_value(out, array, row, false);
  case jsonl::JsonlKind::kDate64:
    return jsonl::append_date64_value(out, array, row, false);
  case jsonl::JsonlKind::kTime32s:
    return jsonl::append_time32s_value(out, array, row, false);
  case jsonl::JsonlKind::kTime32ms:
    return jsonl::append_time32ms_value(out, array, row, false);
  case jsonl::JsonlKind::kTime64us:
    return jsonl::append_time64_value(out, array, row, 1000000, false);
  case jsonl::JsonlKind::kTime64ns:
    return jsonl::append_time64_value(out, array, row, 1000000000, false);
  case jsonl::JsonlKind::kDuration:
    return jsonl::append_duration_value(out, field, array, row, false);
  case jsonl::JsonlKind::kDecimal:
    return jsonl::append_decimal_value(out, field, array, row, false);
  default:
    return sanitize::Status::Invalid(
        "CSV writer: field is not a direct logical scalar");
  }
}

[[nodiscard]] bool
is_direct_csv_logical_scalar(jsonl::JsonlKind kind) noexcept {
  switch (kind) {
  case jsonl::JsonlKind::kBinary:
  case jsonl::JsonlKind::kLargeBinary:
  case jsonl::JsonlKind::kFixedSizeBinary:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
  case jsonl::JsonlKind::kDate32:
  case jsonl::JsonlKind::kDate64:
  case jsonl::JsonlKind::kTime32s:
  case jsonl::JsonlKind::kTime32ms:
  case jsonl::JsonlKind::kTime64us:
  case jsonl::JsonlKind::kTime64ns:
  case jsonl::JsonlKind::kDuration:
  case jsonl::JsonlKind::kDecimal:
    return true;
  default:
    return false;
  }
}

template <class Offset>
sanitize::Status append_csv_string(TextBuffer &out, const ArrowArray &array,
                                   std::int64_t row) {
  if (!array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid("CSV writer: missing string offsets");
  }
  const auto *offsets = static_cast<const Offset *>(array.buffers[1]);
  const auto index = array.offset + row;
  const auto begin = offsets[index];
  const auto end = offsets[index + 1];
  if (begin < 0 || end < begin) {
    return sanitize::Status::Invalid("CSV writer: invalid string offsets");
  }
  const auto length = static_cast<std::uint64_t>(end - begin);
  if (length == 0) {
    return sanitize::Status::OK();
  }
  if (!array.buffers[2]) {
    return sanitize::Status::Invalid("CSV writer: missing string data");
  }
  const auto *data = static_cast<const char *>(array.buffers[2]);
  append_csv_escaped(out,
                     std::string_view(data + static_cast<std::uint64_t>(begin),
                                      static_cast<std::size_t>(length)));
  return sanitize::Status::OK();
}

sanitize::Status append_csv_cell_from_json(TextBuffer &out,
                                           std::string_view json_value,
                                           std::vector<char> *decode_buffer) {
  if (json_value == "null") {
    return sanitize::Status::OK();
  }
  if (!is_json_string_literal(json_value)) {
    append_csv_escaped(out, json_value);
    return sanitize::Status::OK();
  }
  decode_buffer->assign(json_value.size(), '\0');
  SAN_ASSIGN_OR_RAISE(auto decoded,
                      json_string_decode::decode_json_string_slice(
                          decode_buffer->data(), json_value.data() + 1,
                          json_value.data() + json_value.size() - 1, json_value,
                          0, kCsvJsonDecodeErrors));
  append_csv_escaped(out, decoded);
  return sanitize::Status::OK();
}

sanitize::Status append_csv_cell(TextBuffer &out,
                                 const jsonl::JsonlField &field,
                                 const ArrowArray &array, std::int64_t row,
                                 std::vector<char> *decode_buffer) {
  if (field.kind == jsonl::JsonlKind::kNull || array_is_null(array, row)) {
    return sanitize::Status::OK();
  }
  if (is_direct_csv_scalar(field.kind)) {
    return jsonl::append_value(out, field, array, row);
  }
  if (field.kind == jsonl::JsonlKind::kString) {
    return append_csv_string<std::int32_t>(out, array, row);
  }
  if (field.kind == jsonl::JsonlKind::kLargeString) {
    return append_csv_string<std::int64_t>(out, array, row);
  }
  if (is_direct_csv_logical_scalar(field.kind)) {
    return append_direct_csv_logical_scalar(out, field, array, row);
  }

  TextBuffer json_value(out.get_allocator().resource());
  ScopedStringWipe json_value_wipe(&json_value);
  SAN_RETURN_NOT_OK(jsonl::append_value(json_value, field, array, row));
  return append_csv_cell_from_json(out, json_value, decode_buffer);
}

sanitize::Status write_header(Output &out_file, const jsonl::JsonlField &root) {
  TextBuffer buffer;
  ScopedStringWipe buffer_wipe(&buffer);
  for (std::size_t i = 0; i < root.children.size(); ++i) {
    if (i != 0) {
      buffer.push_back(',');
    }
    append_csv_escaped(buffer, root.children[i].name);
  }
  buffer.push_back('\n');
  return out_file.Write(buffer);
}

sanitize::Status append_rows_csv(const jsonl::JsonlField &root,
                                 const ArrowArray &array,
                                 std::int64_t first_row, std::int64_t row_count,
                                 sanitize::internal::StopToken stop,
                                 TextBuffer *out) {
  if (!out || first_row < 0 || row_count < 0 || first_row > array.length ||
      row_count > array.length - first_row) {
    return sanitize::Status::Invalid("CSV writer: invalid output packet range");
  }
  if (row_count > 0) {
    constexpr std::int64_t kDefaultRowReserve = 96;
    const auto reserve_size = static_cast<std::size_t>(
        std::min<std::int64_t>(row_count, 8192) * kDefaultRowReserve);
    out->reserve(reserve_size);
  }
  std::vector<char> decode_buffer;
  ScopedDecodeBufferClear decode_buffer_clear(&decode_buffer);
  const auto end_row = first_row + row_count;
  for (std::int64_t row = first_row; row < end_row; ++row) {
    if ((row & 63) == 0 && stop.stop_requested()) {
      return sanitize::Status::Cancelled("CSV output packet cancelled");
    }
    for (std::size_t col = 0; col < root.children.size(); ++col) {
      if (col != 0) {
        out->push_back(',');
      }
      SAN_RETURN_NOT_OK(append_csv_cell(
          *out, root.children[col], *array.children[col], row, &decode_buffer));
    }
    out->push_back('\n');
  }
  return sanitize::Status::OK();
}

} // namespace

sanitize::Result<WriteStats>
write_stream(ArrowArrayStream *stream, Output &out_file,
             std::int64_t memory_limit_bytes,
             sanitize::ThreadingMode threading_mode) {
  if (!stream) {
    return sanitize::Status::Invalid("CSV writer: Arrow C stream is null");
  }
  sanitize::CSchemaGuard schema;
  const int schema_rc = stream->get_schema(stream, schema.get());
  if (schema_rc != 0) {
    const char *detail =
        stream->get_last_error ? stream->get_last_error(stream) : nullptr;
    return sanitize::Status::IOError(
        "CSV writer: get_schema failed",
        detail && *detail ? std::string(": ") + detail : std::string{});
  }
  SAN_ASSIGN_OR_RAISE(auto root, jsonl::parse_schema_field(schema.value()));
  const auto limits = jsonl::array_validation_limits(memory_limit_bytes);
  SAN_RETURN_NOT_OK(write_header(out_file, root));

  CsvRowEstimator row_estimator(root);
  const auto wide_fixed = row_estimator.high_core_eligible();
  const auto task_arena = sanitize::internal::task_arena_for_stream(stream);
  const auto operation_workers =
      task_arena ? static_cast<std::int64_t>(task_arena->worker_count())
                 : execution_policy_from(threading_mode, memory_limit_bytes)
                       .effective_workers;
  const auto output_worker_ceiling =
      csv_worker_ceiling_for(operation_workers, wide_fixed);
  const auto scale_wide_fixed =
      wide_fixed && output_worker_ceiling > kMinimumCsvOutputWorkers;
  if (task_arena && task_arena->telemetry()) {
    const auto telemetry = task_arena->telemetry();
    telemetry->AddCounter(
        PerformanceCounter::kCsvFixedPlanFixedFields,
        static_cast<std::int64_t>(row_estimator.fixed_field_count()));
    telemetry->AddCounter(
        PerformanceCounter::kCsvFixedPlanDynamicFields,
        static_cast<std::int64_t>(row_estimator.dynamic_field_count()));
    telemetry->AddCounter(PerformanceCounter::kCsvOutputWorkerCeiling,
                          output_worker_ceiling);
  }

  return ordered_text_output::write_stream<WriteStats>(
      stream, out_file, memory_limit_bytes, threading_mode, "CSV writer",
      [&root, &limits](const ArrowArray &array) {
        return jsonl::validate_batch(root, array, limits);
      },
      std::move(row_estimator),
      [&root](const ordered_text_output::BatchPacket &packet, std::size_t,
              sanitize::internal::StopToken stop, TextBuffer *out) {
        return append_rows_csv(root, packet.owner->value(), packet.first_row,
                               packet.row_count, stop, out);
      },
      output_worker_ceiling, true, TaskArenaLane::kOutputCompact, 1,
      scale_wide_fixed, scale_wide_fixed);
}

bool schema_is_supported(const ArrowSchema &schema) {
  return jsonl::parse_schema_field(schema).ok();
}

} // namespace sanitize::internal::csv_stream_writer

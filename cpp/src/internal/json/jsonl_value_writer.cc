// Dispatches Arrow value serialization for the JSONL stream writer.

#include "internal/json/jsonl_value_writer.hh"

#include "internal/json/jsonl_value_writer_parts.hh"

#include <cstdint>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

bool validity_bit_is_set(const uint8_t *bitmap, int64_t index) {
  return (bitmap[index >> 3] & static_cast<uint8_t>(1u << (index & 7))) != 0;
}

bool array_is_null(const ArrowArray &array, int64_t row) {
  if (array.null_count == 0 || !array.buffers || !array.buffers[0]) {
    return false;
  }
  const auto *bitmap = static_cast<const uint8_t *>(array.buffers[0]);
  return !validity_bit_is_set(bitmap, array.offset + row);
}

} // namespace

sanitize::Status append_value(std::string &out, const JsonlField &field,
                              const ArrowArray &array, int64_t row) {
  if (array_is_null(array, row)) {
    out += "null";
    return sanitize::Status::OK();
  }

  switch (field.kind) {
  case JsonlKind::kNull:
    out += "null";
    return sanitize::Status::OK();
  case JsonlKind::kBool: {
    if (!array.buffers || !array.buffers[1]) {
      return sanitize::Status::Invalid("JSONL writer: missing bool buffer");
    }
    const auto *bitmap = static_cast<const uint8_t *>(array.buffers[1]);
    out += validity_bit_is_set(bitmap, array.offset + row) ? "true" : "false";
    return sanitize::Status::OK();
  }
  case JsonlKind::kInt8:
    return append_int8_value(out, array, row);
  case JsonlKind::kUInt8:
    return append_uint8_value(out, array, row);
  case JsonlKind::kInt16:
    return append_int16_value(out, array, row);
  case JsonlKind::kUInt16:
    return append_uint16_value(out, array, row);
  case JsonlKind::kInt32:
    return append_int32_value(out, array, row);
  case JsonlKind::kUInt32:
    return append_uint32_value(out, array, row);
  case JsonlKind::kInt64:
    return append_int64_value(out, array, row);
  case JsonlKind::kUInt64:
    return append_uint64_value(out, array, row);
  case JsonlKind::kFloat16:
    return append_float16_value(out, array, row);
  case JsonlKind::kFloat32:
    return append_float32_value(out, array, row);
  case JsonlKind::kFloat64:
    return append_float64_value(out, array, row);
  case JsonlKind::kString:
    return append_string32_value(out, array, row);
  case JsonlKind::kLargeString:
    return append_string64_value(out, array, row);
  case JsonlKind::kBinary:
    return append_binary32_value(out, array, row);
  case JsonlKind::kLargeBinary:
    return append_binary64_value(out, array, row);
  case JsonlKind::kTimestampMillis:
    return append_timestamp_value(out, array, row, 1000);
  case JsonlKind::kTimestampMicros:
    return append_timestamp_value(out, array, row, 1000000);
  case JsonlKind::kTimestampNanos:
    return append_timestamp_value(out, array, row, 1000000000);
  case JsonlKind::kDate32:
    return append_date32_value(out, array, row);
  case JsonlKind::kDate64:
    return append_date64_value(out, array, row);
  case JsonlKind::kTime32s:
    return append_time32s_value(out, array, row);
  case JsonlKind::kTime32ms:
    return append_time32ms_value(out, array, row);
  case JsonlKind::kTime64us:
    return append_time64_value(out, array, row, 1000000);
  case JsonlKind::kTime64ns:
    return append_time64_value(out, array, row, 1000000000);
  case JsonlKind::kDuration:
    return append_duration_value(out, field, array, row);
  case JsonlKind::kInterval:
    return append_interval_value(out, field, array, row);
  case JsonlKind::kDecimal:
    return append_decimal_value(out, field, array, row);
  case JsonlKind::kStruct:
    return append_struct_value(out, field, array, row);
  case JsonlKind::kList:
  case JsonlKind::kMap:
    return append_list32_value(out, field, array, row);
  case JsonlKind::kLargeList:
    return append_list64_value(out, field, array, row);
  case JsonlKind::kFixedSizeList:
    return append_fixed_size_list_value(out, field, array, row);
  case JsonlKind::kDictionary:
    return append_dictionary_value(out, field, array, row);
  }
  return sanitize::Status::Invalid("JSONL writer: unsupported field kind");
}

} // namespace sanitize::internal::jsonl_stream_writer

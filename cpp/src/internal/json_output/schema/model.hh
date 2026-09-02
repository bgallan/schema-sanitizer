// Declares the compact Arrow schema model shared by native text writers.
// The code validates Arrow layouts and emits deterministic JSON with correct
// null and logical-type semantics.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "nanoarrow/nanoarrow.h"

#include "sanitize/core/status.hh"

namespace sanitize::internal::jsonl_stream_writer {

enum class JsonlKind {
  kNull,
  kBool,
  kInt8,
  kUInt8,
  kInt16,
  kUInt16,
  kInt32,
  kUInt32,
  kInt64,
  kUInt64,
  kFloat16,
  kFloat32,
  kFloat64,
  kString,
  kLargeString,
  kBinary,
  kLargeBinary,
  kFixedSizeBinary,
  kTimestampMillis,
  kTimestampMicros,
  kTimestampNanos,
  kDate32,
  kDate64,
  kTime32s,
  kTime32ms,
  kTime64us,
  kTime64ns,
  kDuration,
  kInterval,
  kDecimal,
  kStruct,
  kList,
  kLargeList,
  kFixedSizeList,
  kMap,
  kDictionary,
};

struct ArrayValidationLimits {
  std::int64_t logical_slots = 0;
  std::int64_t logical_buffer_bytes = 0;
};

/// Derives Arrow validation limits from the operation memory budget.
ArrayValidationLimits array_validation_limits(std::int64_t memory_limit_bytes);

struct JsonlField {
  JsonlKind kind = JsonlKind::kNull;
  JsonlKind dictionary_index_kind = JsonlKind::kNull;
  std::string name;
  std::string format;
  bool nullable = true;
  int32_t decimal_precision = 0;
  int32_t decimal_scale = 0;
  int32_t decimal_byte_width = 16;
  int32_t fixed_size_binary_size = 0;
  int32_t fixed_size_list_size = 0;
  std::vector<JsonlField> children;
  // Pre-escaped JSON object member literals. Struct fields store one entry per
  // child, including the leading comma for every member after the first.
  std::vector<std::string> member_prefixes;
};

/// Maps Arrow C Data format strings to JSONL writer field kinds.
sanitize::Result<JsonlKind> kind_from_format(std::string_view format);

/// Parses one Arrow C schema field into the writer's compact schema model.
sanitize::Result<JsonlField> parse_schema_field(const ArrowSchema &schema);

/// Validates a root struct field against one record batch array.
sanitize::Status validate_batch(const JsonlField &root, const ArrowArray &array,
                                const ArrayValidationLimits &limits);

/// Validates one logical Arrow array slice against its parsed field schema.
sanitize::Status validate_array_slice(const JsonlField &field,
                                      const ArrowArray &array,
                                      std::int64_t offset, std::int64_t length,
                                      const ArrayValidationLimits &limits);

/// Returns whether an Arrow C schema can be serialized by the native writer.
bool schema_is_supported(const ArrowSchema &schema);

} // namespace sanitize::internal::jsonl_stream_writer

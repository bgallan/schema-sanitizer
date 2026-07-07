// Declares Arrow schema helpers used by the native JSONL stream writer.

#pragma once

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

struct JsonlField {
  JsonlKind kind = JsonlKind::kNull;
  JsonlKind dictionary_index_kind = JsonlKind::kNull;
  std::string name;
  std::string format;
  int32_t decimal_precision = 0;
  int32_t decimal_scale = 0;
  int32_t decimal_byte_width = 16;
  int32_t fixed_size_binary_size = 0;
  int32_t fixed_size_list_size = 0;
  std::vector<JsonlField> children;
};

// Maps Arrow C Data format strings to JSONL writer field kinds.
sanitize::Result<JsonlKind> kind_from_format(std::string_view format);

// Parses one Arrow C schema field into the writer's compact schema model.
sanitize::Result<JsonlField> parse_schema_field(const ArrowSchema &schema);

// Validates a root struct field against one record batch array.
sanitize::Status validate_batch(const JsonlField &root,
                                const ArrowArray &array);

// Returns whether an Arrow C schema can be serialized by the native writer.
bool schema_is_supported(const ArrowSchema &schema);

} // namespace sanitize::internal::jsonl_stream_writer

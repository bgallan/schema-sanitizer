// Implements a native Parquet writer for Arrow C streams.

#include "internal/parquet/parquet_stream_writer.hh"

#include "internal/json/jsonl_stream_writer_schema.hh"
#include "sanitize/abi/cdata_types.hh"

#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
#include <zlib.h>
#endif

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace sanitize::internal::parquet_stream_writer {
namespace {

namespace jsonl = sanitize::internal::jsonl_stream_writer;

constexpr std::string_view kUnsupportedPrefix =
    "native Parquet writer: unsupported";
constexpr std::string_view kMagic = "PAR1";
constexpr std::int64_t kDefaultMaxRowsPerRowGroup = 64 * 1024;
constexpr std::int64_t kDefaultMaxBytesPerRowGroup = 64LL * 1024LL * 1024LL;
constexpr std::int64_t kDefaultTargetPageBytes = 1024LL * 1024LL;
constexpr std::int32_t kEncodingPlain = 0;
constexpr std::int32_t kEncodingDeltaBinaryPacked = 5;
constexpr std::int32_t kEncodingDeltaLengthByteArray = 6;
constexpr std::int32_t kEncodingRleDictionary = 8;
constexpr std::int32_t kEncodingByteStreamSplit = 9;

enum class PhysicalType : int32_t {
  kBoolean = 0,
  kInt32 = 1,
  kInt64 = 2,
  kFloat = 4,
  kDouble = 5,
  kByteArray = 6,
  kFixedLenByteArray = 7,
};

enum class CompressionCodec : int32_t {
  kUncompressed = 0,
  kGzip = 2,
};

enum class ConvertedType : int32_t {
  kUtf8 = 0,
  kMap = 1,
  kList = 3,
  kDecimal = 5,
  kDate = 6,
  kTimeMillis = 7,
  kUInt8 = 11,
  kUInt16 = 12,
  kUInt32 = 13,
  kUInt64 = 14,
  kInt8 = 15,
  kInt16 = 16,
  kInt32 = 17,
  kInt64 = 18,
};

enum class NodeKind {
  kRoot,
  kStruct,
  kList,
  kMap,
  kPrimitive,
};

struct ParquetNode {
  NodeKind node_kind = NodeKind::kPrimitive;
  jsonl::JsonlKind arrow_kind = jsonl::JsonlKind::kNull;
  jsonl::JsonlKind dictionary_index_kind = jsonl::JsonlKind::kNull;
  jsonl::JsonlKind dictionary_value_kind = jsonl::JsonlKind::kNull;
  std::string name;
  bool required = false;
  PhysicalType physical_type = PhysicalType::kInt64;
  bool has_converted_type = false;
  ConvertedType converted_type = ConvertedType::kUtf8;
  bool has_null_logical_type = false;
  bool has_timestamp_logical_type = false;
  bool has_time_millis_logical_type = false;
  bool has_int_logical_type = false;
  std::int8_t int_bit_width = 0;
  bool int_is_signed = true;
  bool has_decimal_metadata = false;
  std::int32_t decimal_precision = 0;
  std::int32_t decimal_scale = 0;
  std::int32_t decimal_byte_width = 0;
  std::int32_t fixed_binary_byte_width = 0;
  std::vector<ParquetNode> children;
  std::unique_ptr<ParquetNode> element;
  std::size_t leaf_index = 0;
  std::int16_t repeated_repetition_level = 0;
  std::int32_t fixed_size_list_size = 0;
};

struct LeafColumn {
  const ParquetNode *node = nullptr;
  std::vector<std::string> path;
  std::int16_t max_definition_level = 0;
  std::int16_t max_repetition_level = 0;
};

struct ColumnPageData {
  std::vector<std::int16_t> definition_levels;
  std::vector<std::int16_t> repetition_levels;
  std::string values;
  std::vector<bool> bool_values;
};

struct PageInfo {
  std::int64_t first_page_offset = 0;
  std::int64_t offset = 0;
  std::int64_t dictionary_page_offset = -1;
  std::int64_t dictionary_header_size = 0;
  std::int64_t dictionary_uncompressed_payload_size = 0;
  std::int64_t dictionary_compressed_payload_size = 0;
  std::int64_t header_size = 0;
  std::int64_t uncompressed_payload_size = 0;
  std::int64_t compressed_payload_size = 0;
  std::int64_t num_values = 0;
  std::int64_t null_count = 0;
  std::optional<std::string> min_value;
  std::optional<std::string> max_value;
  bool dictionary_encoded = false;
  bool has_plain_encoding = false;
  bool has_delta_binary_packed_encoding = false;
  bool has_delta_length_byte_array_encoding = false;
  bool has_byte_stream_split_encoding = false;
  std::int32_t value_encoding = kEncodingPlain;
};

struct ColumnChunkInfo {
  PageInfo aggregate;
  std::vector<PageInfo> pages;
  std::int64_t column_index_offset = -1;
  std::int32_t column_index_length = 0;
  std::int64_t offset_index_offset = -1;
  std::int32_t offset_index_length = 0;
};

struct RowGroupInfo {
  std::int64_t num_rows = 0;
  std::vector<ColumnChunkInfo> columns;
};

class CountingOutput {
public:
  // Wraps an output target and tracks bytes written through it.
  explicit CountingOutput(Output &out) : out_(out) {}

  // Writes bytes and advances the logical file offset.
  sanitize::Status Write(std::string_view data) {
    SAN_RETURN_NOT_OK(out_.Write(data));
    offset_ += static_cast<std::int64_t>(data.size());
    return sanitize::Status::OK();
  }

  // Flushes the wrapped target.
  sanitize::Status Flush() { return out_.Flush(); }

  // Returns the current logical file offset.
  [[nodiscard]] std::int64_t offset() const noexcept { return offset_; }

private:
  Output &out_;
  std::int64_t offset_ = 0;
};

std::optional<std::string> env_value(const char *name) {
  if (!name || *name == '\0') {
    return std::nullopt;
  }
#if defined(_MSC_VER)
  char *raw = nullptr;
  std::size_t size = 0;
  if (_dupenv_s(&raw, &size, name) != 0 || !raw) {
    return std::nullopt;
  }
  std::string value(raw, size > 0 ? size - 1 : 0);
  std::free(raw);
  return value;
#else
  const char *raw = std::getenv(name);
  if (!raw) {
    return std::nullopt;
  }
  return std::string(raw);
#endif
}

std::int64_t configured_max_rows_per_row_group() {
  const auto raw = env_value("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_ROWS");
  if (!raw || raw->empty()) {
    return kDefaultMaxRowsPerRowGroup;
  }
  char *end = nullptr;
  const char *begin = raw->c_str();
  const auto parsed = std::strtoll(begin, &end, 10);
  if (end == begin || (end && *end != '\0') || parsed <= 0) {
    return kDefaultMaxRowsPerRowGroup;
  }
  return parsed;
}

std::int64_t configured_positive_i64_env(const char *name,
                                         std::int64_t default_value) {
  const auto raw = env_value(name);
  if (!raw || raw->empty()) {
    return default_value;
  }
  char *end = nullptr;
  const char *begin = raw->c_str();
  const auto parsed = std::strtoll(begin, &end, 10);
  if (end == begin || (end && *end != '\0') || parsed <= 0) {
    return default_value;
  }
  return parsed;
}

std::int64_t configured_max_bytes_per_row_group() {
  return configured_positive_i64_env(
      "SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES",
      kDefaultMaxBytesPerRowGroup);
}

bool has_configured_max_bytes_per_row_group() {
  const auto raw = env_value("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES");
  return raw && !raw->empty();
}

std::int64_t
adaptive_max_bytes_per_row_group(const std::vector<LeafColumn> &columns,
                                 std::int64_t configured_value) {
  if (has_configured_max_bytes_per_row_group()) {
    return configured_value;
  }
  bool has_repeated = false;
  for (const auto &column : columns) {
    has_repeated = has_repeated || column.max_repetition_level > 0;
  }
  std::int64_t adaptive = configured_value;
  if (columns.size() >= 512) {
    adaptive = std::min<std::int64_t>(adaptive, 16LL * 1024LL * 1024LL);
  } else if (columns.size() >= 128 || has_repeated) {
    adaptive = std::min<std::int64_t>(adaptive, 32LL * 1024LL * 1024LL);
  }
  return std::max<std::int64_t>(adaptive, 1024LL * 1024LL);
}

std::int64_t configured_target_page_bytes() {
  return configured_positive_i64_env(
      "SCHEMA_SANITIZER_NATIVE_PARQUET_PAGE_BYTES", kDefaultTargetPageBytes);
}

sanitize::Result<CompressionCodec> configured_compression_codec() {
  const auto raw = env_value("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION");
  if (!raw || raw->empty()) {
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
    return CompressionCodec::kGzip;
#else
    return sanitize::Status::Invalid(
        "native Parquet writer: gzip compression is the default but zlib is "
        "not available; set SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION="
        "uncompressed to write uncompressed output");
#endif
  }
  const std::string_view value(*raw);
  if (value == "none" || value == "uncompressed") {
    return CompressionCodec::kUncompressed;
  }
  if (value == "gzip") {
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
    return CompressionCodec::kGzip;
#else
    return sanitize::Status::Invalid(
        "native Parquet writer: gzip compression requested but zlib is not "
        "available");
#endif
  }
  return sanitize::Status::Invalid(
      "native Parquet writer: unsupported compression '", value,
      "'; expected gzip, uncompressed, or none");
}

sanitize::Result<int> configured_gzip_level() {
  const auto raw = env_value("SCHEMA_SANITIZER_NATIVE_PARQUET_GZIP_LEVEL");
  if (!raw || raw->empty()) {
    return -1; // zlib default compression level.
  }
  char *end = nullptr;
  const char *begin = raw->c_str();
  const auto parsed = std::strtol(begin, &end, 10);
  if (end == begin || (end && *end != '\0') || parsed < 0 || parsed > 9) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid gzip level '", *raw,
        "'; expected an integer from 0 to 9");
  }
  return static_cast<int>(parsed);
}

// Appends one little-endian 32-bit integer.
void append_u32_le(std::string &out, std::uint32_t value) {
  out.push_back(static_cast<char>(value & 0xFFU));
  out.push_back(static_cast<char>((value >> 8U) & 0xFFU));
  out.push_back(static_cast<char>((value >> 16U) & 0xFFU));
  out.push_back(static_cast<char>((value >> 24U) & 0xFFU));
}

// Appends raw bytes to a string buffer.
void append_bytes(std::string &out, const void *data, std::size_t size) {
  out.append(static_cast<const char *>(data), size);
}

// Returns whether an Arrow value is valid at a logical row index.
bool bitmap_is_valid(const ArrowArray &array, std::int64_t logical_index) {
  if (array.n_buffers <= 0 || !array.buffers || !array.buffers[0]) {
    return true;
  }
  const auto *bits = static_cast<const std::uint8_t *>(array.buffers[0]);
  const auto index = array.offset + logical_index;
  return (bits[index >> 3] & (std::uint8_t{1} << (index & 7))) != 0;
}

// Reads one boolean value from an Arrow bitmap array.
bool bool_value(const ArrowArray &array, std::int64_t logical_index) {
  if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1]) {
    return false;
  }
  const auto *bits = static_cast<const std::uint8_t *>(array.buffers[1]);
  const auto index = array.offset + logical_index;
  return (bits[index >> 3] & (std::uint8_t{1} << (index & 7))) != 0;
}

// Returns a typed pointer to a fixed-width Arrow value buffer.
template <class T> const T *data_buffer(const ArrowArray &array) {
  if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1]) {
    return nullptr;
  }
  return static_cast<const T *>(array.buffers[1]);
}

// Reads one supported Arrow dictionary index.
template <class T>
std::optional<std::int64_t> dictionary_index_value(const ArrowArray &array,
                                                   std::int64_t row) {
  const T *values = data_buffer<T>(array);
  if (!values) {
    return std::nullopt;
  }
  return static_cast<std::int64_t>(values[array.offset + row]);
}

std::optional<std::int64_t> dictionary_index_at(const ArrowArray &array,
                                                jsonl::JsonlKind index_kind,
                                                std::int64_t row) {
  switch (index_kind) {
  case jsonl::JsonlKind::kInt8:
    return dictionary_index_value<std::int8_t>(array, row);
  case jsonl::JsonlKind::kUInt8:
    return dictionary_index_value<std::uint8_t>(array, row);
  case jsonl::JsonlKind::kInt16:
    return dictionary_index_value<std::int16_t>(array, row);
  case jsonl::JsonlKind::kUInt16:
    return dictionary_index_value<std::uint16_t>(array, row);
  case jsonl::JsonlKind::kInt32:
    return dictionary_index_value<std::int32_t>(array, row);
  case jsonl::JsonlKind::kUInt32:
    return dictionary_index_value<std::uint32_t>(array, row);
  case jsonl::JsonlKind::kInt64:
    return dictionary_index_value<std::int64_t>(array, row);
  default:
    return std::nullopt;
  }
}

// Converts one supported Arrow primitive field into Parquet leaf metadata.
sanitize::Result<ParquetNode>
primitive_node_from_field(const jsonl::JsonlField &field, std::string name) {
  ParquetNode node;
  node.node_kind = NodeKind::kPrimitive;
  node.arrow_kind = field.kind;
  node.name = std::move(name);
  auto set_int_logical = [&](std::int8_t bit_width, bool is_signed,
                             ConvertedType converted_type) {
    node.has_converted_type = true;
    node.converted_type = converted_type;
    node.has_int_logical_type = true;
    node.int_bit_width = bit_width;
    node.int_is_signed = is_signed;
  };
  switch (field.kind) {
  case jsonl::JsonlKind::kNull:
    node.physical_type = PhysicalType::kInt32;
    node.has_null_logical_type = true;
    return node;
  case jsonl::JsonlKind::kBool:
    node.physical_type = PhysicalType::kBoolean;
    return node;
  case jsonl::JsonlKind::kInt8:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(8, true, ConvertedType::kInt8);
    return node;
  case jsonl::JsonlKind::kUInt8:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(8, false, ConvertedType::kUInt8);
    return node;
  case jsonl::JsonlKind::kInt16:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(16, true, ConvertedType::kInt16);
    return node;
  case jsonl::JsonlKind::kUInt16:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(16, false, ConvertedType::kUInt16);
    return node;
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kDate32:
    node.physical_type = PhysicalType::kInt32;
    if (field.kind == jsonl::JsonlKind::kDate32) {
      node.has_converted_type = true;
      node.converted_type = ConvertedType::kDate;
    }
    return node;
  case jsonl::JsonlKind::kTime32s:
    node.physical_type = PhysicalType::kInt32;
    node.has_converted_type = true;
    node.converted_type = ConvertedType::kTimeMillis;
    node.has_time_millis_logical_type = true;
    return node;
  case jsonl::JsonlKind::kUInt32:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(32, false, ConvertedType::kUInt32);
    return node;
  case jsonl::JsonlKind::kInt64:
    node.physical_type = PhysicalType::kInt64;
    return node;
  case jsonl::JsonlKind::kUInt64:
    node.physical_type = PhysicalType::kInt64;
    set_int_logical(64, false, ConvertedType::kUInt64);
    return node;
  case jsonl::JsonlKind::kFloat32:
    node.physical_type = PhysicalType::kFloat;
    return node;
  case jsonl::JsonlKind::kFloat64:
    node.physical_type = PhysicalType::kDouble;
    return node;
  case jsonl::JsonlKind::kString:
  case jsonl::JsonlKind::kLargeString:
  case jsonl::JsonlKind::kBinary:
  case jsonl::JsonlKind::kLargeBinary:
    node.physical_type = PhysicalType::kByteArray;
    if (field.kind == jsonl::JsonlKind::kString ||
        field.kind == jsonl::JsonlKind::kLargeString) {
      node.has_converted_type = true;
      node.converted_type = ConvertedType::kUtf8;
    }
    return node;
  case jsonl::JsonlKind::kFixedSizeBinary:
    if (field.fixed_size_binary_size <= 0) {
      return sanitize::Status::NotImplemented(
          kUnsupportedPrefix, " Arrow fixed-size binary field '", field.name,
          "' has unsupported format '", field.format, "'");
    }
    node.physical_type = PhysicalType::kFixedLenByteArray;
    node.fixed_binary_byte_width = field.fixed_size_binary_size;
    return node;
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
    node.physical_type = PhysicalType::kInt64;
    node.has_timestamp_logical_type = true;
    return node;
  case jsonl::JsonlKind::kDecimal:
    if (field.decimal_precision <= 0 ||
        (field.decimal_byte_width != 16 && field.decimal_byte_width != 32)) {
      return sanitize::Status::NotImplemented(
          kUnsupportedPrefix, " Arrow decimal field '", field.name,
          "' has unsupported format '", field.format, "'");
    }
    node.physical_type = PhysicalType::kFixedLenByteArray;
    node.has_converted_type = true;
    node.converted_type = ConvertedType::kDecimal;
    node.has_decimal_metadata = true;
    node.decimal_precision = field.decimal_precision;
    node.decimal_scale = field.decimal_scale;
    node.decimal_byte_width = field.decimal_byte_width;
    node.fixed_binary_byte_width = field.decimal_byte_width;
    return node;
  default:
    return sanitize::Status::NotImplemented(
        kUnsupportedPrefix, " Arrow field '", field.name,
        "' has unsupported format '", field.format, "'");
  }
}

// Converts one supported Arrow schema field into a recursive Parquet node.
sanitize::Result<ParquetNode> node_from_field(const jsonl::JsonlField &field,
                                              std::string name);

// Converts a supported Arrow dictionary field into a plain Parquet leaf.
sanitize::Result<ParquetNode>
dictionary_node_from_field(const jsonl::JsonlField &field, std::string name) {
  if (field.children.size() != 1) {
    return sanitize::Status::NotImplemented(kUnsupportedPrefix,
                                            " dictionary field '", field.name,
                                            "' must have one value child");
  }
  SAN_ASSIGN_OR_RAISE(auto value_node,
                      node_from_field(field.children.front(), std::move(name)));
  if (value_node.node_kind != NodeKind::kPrimitive ||
      value_node.arrow_kind == jsonl::JsonlKind::kDictionary) {
    return sanitize::Status::NotImplemented(
        kUnsupportedPrefix, " dictionary field '", field.name,
        "' has unsupported value format '", field.children.front().format, "'");
  }
  value_node.dictionary_value_kind = value_node.arrow_kind;
  value_node.arrow_kind = jsonl::JsonlKind::kDictionary;
  value_node.dictionary_index_kind = field.dictionary_index_kind;
  return value_node;
}

// Converts a supported Arrow struct field into a Parquet group node.
sanitize::Result<ParquetNode>
struct_node_from_field(const jsonl::JsonlField &field, std::string name) {
  ParquetNode node;
  node.node_kind = name == "schema" ? NodeKind::kRoot : NodeKind::kStruct;
  node.arrow_kind = field.kind;
  node.name = std::move(name);
  node.children.reserve(field.children.size());
  for (const auto &child : field.children) {
    SAN_ASSIGN_OR_RAISE(auto child_node, node_from_field(child, child.name));
    node.children.push_back(std::move(child_node));
  }
  return node;
}

// Converts a supported Arrow list field into a Parquet LIST node.
sanitize::Result<ParquetNode>
list_node_from_field(const jsonl::JsonlField &field, std::string name) {
  if (field.children.size() != 1) {
    return sanitize::Status::NotImplemented(kUnsupportedPrefix, " list field '",
                                            field.name,
                                            "' must have one child");
  }
  const auto &value = field.children.front();
  if (value.kind == jsonl::JsonlKind::kMap) {
    return sanitize::Status::NotImplemented(
        kUnsupportedPrefix, " list field '", field.name,
        "' has unsupported element format '", value.format, "'");
  }
  ParquetNode node;
  node.node_kind = NodeKind::kList;
  node.arrow_kind = field.kind;
  node.name = std::move(name);
  node.fixed_size_list_size = field.fixed_size_list_size;
  SAN_ASSIGN_OR_RAISE(auto element, node_from_field(value, "element"));
  node.element = std::make_unique<ParquetNode>(std::move(element));
  return node;
}

// Converts a supported Arrow map field into a Parquet MAP node.
sanitize::Result<ParquetNode>
map_node_from_field(const jsonl::JsonlField &field, std::string name) {
  if (field.children.size() != 1 ||
      field.children.front().kind != jsonl::JsonlKind::kStruct ||
      field.children.front().children.size() != 2) {
    return sanitize::Status::NotImplemented(
        kUnsupportedPrefix, " map field '", field.name,
        "' does not have key/value entry children");
  }
  const auto &entry = field.children.front();
  const auto &key = entry.children[0];
  const auto &value = entry.children[1];
  if (key.kind == jsonl::JsonlKind::kList ||
      key.kind == jsonl::JsonlKind::kLargeList ||
      key.kind == jsonl::JsonlKind::kFixedSizeList ||
      key.kind == jsonl::JsonlKind::kMap ||
      key.kind == jsonl::JsonlKind::kStruct) {
    return sanitize::Status::NotImplemented(
        kUnsupportedPrefix, " map field '", field.name,
        "' has unsupported key format '", key.format, "'");
  }

  ParquetNode node;
  node.node_kind = NodeKind::kMap;
  node.arrow_kind = field.kind;
  node.name = std::move(name);
  auto entry_node = std::make_unique<ParquetNode>();
  entry_node->node_kind = NodeKind::kStruct;
  entry_node->arrow_kind = entry.kind;
  entry_node->name = "key_value";
  entry_node->required = true;
  entry_node->children.reserve(2);
  SAN_ASSIGN_OR_RAISE(auto key_node, node_from_field(key, "key"));
  key_node.required = true;
  SAN_ASSIGN_OR_RAISE(auto value_node, node_from_field(value, "value"));
  entry_node->children.push_back(std::move(key_node));
  entry_node->children.push_back(std::move(value_node));
  node.element = std::move(entry_node);
  return node;
}

sanitize::Result<ParquetNode> node_from_field(const jsonl::JsonlField &field,
                                              std::string name) {
  if (field.kind == jsonl::JsonlKind::kDictionary) {
    return dictionary_node_from_field(field, std::move(name));
  }
  if (field.kind == jsonl::JsonlKind::kStruct) {
    return struct_node_from_field(field, std::move(name));
  }
  if (field.kind == jsonl::JsonlKind::kList ||
      field.kind == jsonl::JsonlKind::kLargeList ||
      field.kind == jsonl::JsonlKind::kFixedSizeList) {
    return list_node_from_field(field, std::move(name));
  }
  if (field.kind == jsonl::JsonlKind::kMap) {
    return map_node_from_field(field, std::move(name));
  }
  return primitive_node_from_field(field, std::move(name));
}

// Appends the path to every primitive leaf in schema order.
void collect_leaf_columns(const ParquetNode &node,
                          std::vector<std::string> path,
                          std::int16_t definition_level,
                          std::int16_t repetition_level,
                          std::vector<LeafColumn> *columns) {
  switch (node.node_kind) {
  case NodeKind::kRoot:
    for (const auto &child : node.children) {
      auto child_path = path;
      child_path.push_back(child.name);
      collect_leaf_columns(child, std::move(child_path), definition_level,
                           repetition_level, columns);
    }
    return;
  case NodeKind::kStruct:
    if (!node.required) {
      ++definition_level;
    }
    for (const auto &child : node.children) {
      auto child_path = path;
      child_path.push_back(child.name);
      collect_leaf_columns(child, std::move(child_path), definition_level,
                           repetition_level, columns);
    }
    return;
  case NodeKind::kList: {
    const auto list_definition =
        static_cast<std::int16_t>(definition_level + 1);
    const auto element_definition =
        static_cast<std::int16_t>(list_definition + 1);
    const auto element_repetition =
        static_cast<std::int16_t>(repetition_level + 1);
    auto element_path = std::move(path);
    element_path.push_back("list");
    element_path.push_back("element");
    collect_leaf_columns(*node.element, std::move(element_path),
                         element_definition, element_repetition, columns);
    return;
  }
  case NodeKind::kMap: {
    const auto map_definition = static_cast<std::int16_t>(definition_level + 1);
    const auto entry_definition = static_cast<std::int16_t>(map_definition + 1);
    const auto entry_repetition =
        static_cast<std::int16_t>(repetition_level + 1);
    auto entry_path = std::move(path);
    entry_path.push_back("key_value");
    collect_leaf_columns(*node.element, std::move(entry_path), entry_definition,
                         entry_repetition, columns);
    return;
  }
  case NodeKind::kPrimitive:
    columns->push_back(LeafColumn{
        .node = &node,
        .path = std::move(path),
        .max_definition_level = static_cast<std::int16_t>(
            definition_level + (node.required ? 0 : 1)),
        .max_repetition_level = repetition_level,
    });
    return;
  }
}

// Assigns stable leaf indexes after flattening the schema.
void assign_leaf_indexes(ParquetNode *node, std::size_t *next) {
  if (node->node_kind == NodeKind::kPrimitive) {
    node->leaf_index = (*next)++;
    return;
  }
  for (auto &child : node->children) {
    assign_leaf_indexes(&child, next);
  }
  if (node->element) {
    assign_leaf_indexes(node->element.get(), next);
  }
}

// Assigns absolute repetition levels for repeated container nodes.
void assign_repetition_levels(ParquetNode *node, std::int16_t definition_level,
                              std::int16_t repetition_level) {
  switch (node->node_kind) {
  case NodeKind::kRoot:
    for (auto &child : node->children) {
      assign_repetition_levels(&child, definition_level, repetition_level);
    }
    return;
  case NodeKind::kStruct: {
    const auto child_definition =
        static_cast<std::int16_t>(definition_level + (node->required ? 0 : 1));
    for (auto &child : node->children) {
      assign_repetition_levels(&child, child_definition, repetition_level);
    }
    return;
  }
  case NodeKind::kList: {
    node->repeated_repetition_level =
        static_cast<std::int16_t>(repetition_level + 1);
    const auto element_definition =
        static_cast<std::int16_t>(definition_level + 2);
    assign_repetition_levels(node->element.get(), element_definition,
                             node->repeated_repetition_level);
    return;
  }
  case NodeKind::kMap: {
    node->repeated_repetition_level =
        static_cast<std::int16_t>(repetition_level + 1);
    const auto entry_definition =
        static_cast<std::int16_t>(definition_level + 2);
    assign_repetition_levels(node->element.get(), entry_definition,
                             node->repeated_repetition_level);
    return;
  }
  case NodeKind::kPrimitive:
    return;
  }
}

// Parses and validates the root schema supported by this writer.
sanitize::Result<ParquetNode>
parse_supported_root_schema(const ArrowSchema &schema) {
  SAN_ASSIGN_OR_RAISE(auto root_field, jsonl::parse_schema_field(schema));
  if (root_field.kind != jsonl::JsonlKind::kStruct) {
    return sanitize::Status::NotImplemented(kUnsupportedPrefix,
                                            " root schema is not a struct");
  }
  SAN_ASSIGN_OR_RAISE(auto root, struct_node_from_field(root_field, "schema"));
  return root;
}

// Appends an unsigned varint using Thrift compact encoding.
void append_varint(std::string &out, std::uint64_t value) {
  while ((value & ~std::uint64_t{0x7F}) != 0) {
    out.push_back(static_cast<char>((value & 0x7FU) | 0x80U));
    value >>= 7U;
  }
  out.push_back(static_cast<char>(value));
}

// Zig-zag encodes a signed integer for Thrift compact fields.
std::uint64_t zigzag(std::int64_t value) {
  return (static_cast<std::uint64_t>(value) << 1U) ^
         static_cast<std::uint64_t>(value >> 63U);
}

class CompactWriter {
public:
  // Creates a Thrift compact writer over an existing byte buffer.
  explicit CompactWriter(std::string &out) : out_(out) {}

  // Writes an i32 field.
  void FieldI32(std::int16_t id, std::int32_t value) {
    field(id, 5);
    append_varint(out_, zigzag(value));
  }

  // Writes an i8 field.
  void FieldByte(std::int16_t id, std::int8_t value) {
    field(id, 3);
    out_.push_back(static_cast<char>(value));
  }

  // Writes an i64 field.
  void FieldI64(std::int16_t id, std::int64_t value) {
    field(id, 6);
    append_varint(out_, zigzag(value));
  }

  // Writes a binary/string field.
  void FieldString(std::int16_t id, std::string_view value) {
    field(id, 8);
    append_varint(out_, value.size());
    out_.append(value.data(), value.size());
  }

  // Writes a boolean field.
  void FieldBool(std::int16_t id, bool value) { field(id, value ? 1 : 2); }

  // Writes a nested struct field.
  void FieldStruct(std::int16_t id, auto write_struct) {
    field(id, 12);
    CompactWriter child(out_);
    write_struct(child);
    child.Stop();
  }

  // Writes a list of i32 values.
  void FieldListI32(std::int16_t id, const std::vector<std::int32_t> &values) {
    field(id, 9);
    list_header(values.size(), 5);
    for (const auto value : values) {
      append_varint(out_, zigzag(value));
    }
  }

  // Writes a list of i64 values.
  void FieldListI64(std::int16_t id, const std::vector<std::int64_t> &values) {
    field(id, 9);
    list_header(values.size(), 6);
    for (const auto value : values) {
      append_varint(out_, zigzag(value));
    }
  }

  // Writes a list of boolean values.
  void FieldListBool(std::int16_t id, const std::vector<bool> &values) {
    field(id, 9);
    list_header(values.size(), 2);
    for (const bool value : values) {
      out_.push_back(static_cast<char>(value ? 1 : 2));
    }
  }

  // Writes a list of strings.
  void FieldListString(std::int16_t id,
                       const std::vector<std::string> &values) {
    field(id, 9);
    list_header(values.size(), 8);
    for (const auto &value : values) {
      append_varint(out_, value.size());
      out_.append(value);
    }
  }

  // Writes a list of structs.
  void FieldListStruct(std::int16_t id, std::size_t size, auto write_items) {
    field(id, 9);
    list_header(size, 12);
    write_items(*this);
  }

  // Closes the current struct.
  void Stop() { out_.push_back('\0'); }

private:
  // Writes one compact field header.
  void field(std::int16_t id, std::uint8_t type) {
    const auto delta = id - last_field_id_;
    if (delta > 0 && delta <= 15) {
      out_.push_back(static_cast<char>((delta << 4) | type));
    } else {
      out_.push_back(static_cast<char>(type));
      append_varint(out_, zigzag(id));
    }
    last_field_id_ = id;
  }

  // Writes one compact list header.
  void list_header(std::size_t size, std::uint8_t element_type) {
    if (size <= 14) {
      out_.push_back(static_cast<char>((size << 4U) | element_type));
      return;
    }
    out_.push_back(static_cast<char>(0xF0U | element_type));
    append_varint(out_, size);
  }

  std::string &out_;
  std::int16_t last_field_id_ = 0;
};

// Returns the number of bytes required for one RLE level value.
std::uint8_t level_bit_width(std::int16_t max_level) {
  std::uint8_t width = 0;
  while ((std::int16_t{1} << width) <= max_level) {
    ++width;
  }
  return width;
}

// Appends one RLE run for repetition or definition levels.
void append_rle_run(std::string &out, std::int64_t run_length,
                    std::uint16_t value, std::uint8_t bit_width) {
  append_varint(out, static_cast<std::uint64_t>(run_length) << 1U);
  const auto byte_width = static_cast<std::uint8_t>((bit_width + 7U) / 8U);
  for (std::uint8_t i = 0; i < byte_width; ++i) {
    out.push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
  }
}

// Encodes Parquet RLE levels with a 4-byte length prefix.
std::string encode_levels(const std::vector<std::int16_t> &levels,
                          std::int16_t max_level) {
  const auto bit_width = level_bit_width(max_level);
  std::string encoded;
  if (!levels.empty() && bit_width > 0) {
    auto current = static_cast<std::uint16_t>(levels.front());
    std::int64_t run_length = 1;
    for (std::size_t i = 1; i < levels.size(); ++i) {
      const auto value = static_cast<std::uint16_t>(levels[i]);
      if (value == current) {
        ++run_length;
        continue;
      }
      append_rle_run(encoded, run_length, current, bit_width);
      current = value;
      run_length = 1;
    }
    append_rle_run(encoded, run_length, current, bit_width);
  }

  std::string out;
  append_u32_le(out, static_cast<std::uint32_t>(encoded.size()));
  out.append(encoded);
  return out;
}

// Encodes a Parquet data page header.
std::string encode_data_page_header(std::int32_t num_values,
                                    std::int32_t uncompressed_size,
                                    std::int32_t compressed_size,
                                    std::int32_t value_encoding) {
  std::string out;
  CompactWriter writer(out);
  writer.FieldI32(1, 0);                 // DATA_PAGE
  writer.FieldI32(2, uncompressed_size); // uncompressed size
  writer.FieldI32(3, compressed_size);   // compressed size
  writer.FieldStruct(5, [&](CompactWriter &page) {
    page.FieldI32(1, num_values);
    page.FieldI32(2, value_encoding);
    page.FieldI32(3, 3); // RLE
    page.FieldI32(4, 3); // RLE
  });
  writer.Stop();
  return out;
}

std::string encode_dictionary_page_header(std::int32_t num_values,
                                          std::int32_t uncompressed_size,
                                          std::int32_t compressed_size) {
  std::string out;
  CompactWriter writer(out);
  writer.FieldI32(1, 2);                 // DICTIONARY_PAGE
  writer.FieldI32(2, uncompressed_size); // uncompressed size
  writer.FieldI32(3, compressed_size);   // compressed size
  writer.FieldStruct(7, [&](CompactWriter &page) {
    page.FieldI32(1, num_values);
    page.FieldI32(2, 0); // PLAIN
    page.FieldBool(3, false);
  });
  writer.Stop();
  return out;
}

sanitize::Result<std::string> compress_payload(std::string_view payload,
                                               CompressionCodec codec) {
  if (codec == CompressionCodec::kUncompressed) {
    return std::string(payload);
  }
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
  if (codec == CompressionCodec::kGzip) {
    SAN_ASSIGN_OR_RAISE(const int gzip_level, configured_gzip_level());
    z_stream stream{};
    int rc = deflateInit2(&stream, gzip_level, Z_DEFLATED, MAX_WBITS + 16, 8,
                          Z_DEFAULT_STRATEGY);
    if (rc != Z_OK) {
      return sanitize::Status::IOError(
          "native Parquet writer: failed to initialize gzip compression");
    }
    std::string out;
    const auto bound = deflateBound(
        &stream, static_cast<uLong>(std::min<std::size_t>(
                     payload.size(), std::numeric_limits<uLong>::max())));
    out.resize(static_cast<std::size_t>(bound));
    stream.next_in =
        reinterpret_cast<Bytef *>(const_cast<char *>(payload.data()));
    stream.avail_in = static_cast<uInt>(payload.size());
    stream.next_out = reinterpret_cast<Bytef *>(out.data());
    stream.avail_out = static_cast<uInt>(out.size());
    rc = deflate(&stream, Z_FINISH);
    if (rc != Z_STREAM_END) {
      deflateEnd(&stream);
      return sanitize::Status::IOError(
          "native Parquet writer: gzip compression failed");
    }
    out.resize(stream.total_out);
    deflateEnd(&stream);
    return out;
  }
#endif
  return sanitize::Status::Invalid(
      "native Parquet writer: unsupported compression codec");
}

template <class T>
std::optional<std::pair<std::string, std::string>>
fixed_width_min_max(std::string_view values) {
  if (values.empty() || values.size() % sizeof(T) != 0) {
    return std::nullopt;
  }
  T min_value{};
  T max_value{};
  std::memcpy(&min_value, values.data(), sizeof(T));
  max_value = min_value;
  for (std::size_t offset = sizeof(T); offset < values.size();
       offset += sizeof(T)) {
    T value{};
    std::memcpy(&value, values.data() + offset, sizeof(T));
    min_value = std::min(min_value, value);
    max_value = std::max(max_value, value);
  }
  std::string min_bytes;
  std::string max_bytes;
  append_bytes(min_bytes, &min_value, sizeof(T));
  append_bytes(max_bytes, &max_value, sizeof(T));
  return std::pair<std::string, std::string>{std::move(min_bytes),
                                             std::move(max_bytes)};
}

template <class T> bool floating_less_for_stats(T left, T right) {
  if (left < right) {
    return true;
  }
  if (right < left) {
    return false;
  }
  return left == T{0} && right == T{0} && std::signbit(left) &&
         !std::signbit(right);
}

template <class T>
std::optional<std::pair<std::string, std::string>>
floating_min_max(std::string_view values) {
  if (values.empty() || values.size() % sizeof(T) != 0) {
    return std::nullopt;
  }
  std::optional<T> min_value;
  std::optional<T> max_value;
  for (std::size_t offset = 0; offset < values.size(); offset += sizeof(T)) {
    T value{};
    std::memcpy(&value, values.data() + offset, sizeof(T));
    if (std::isnan(value)) {
      continue;
    }
    if (!min_value) {
      min_value = value;
      max_value = value;
      continue;
    }
    if (floating_less_for_stats(value, *min_value)) {
      min_value = value;
    }
    if (floating_less_for_stats(*max_value, value)) {
      max_value = value;
    }
  }
  if (!min_value || !max_value) {
    return std::nullopt;
  }
  std::string min_bytes;
  std::string max_bytes;
  append_bytes(min_bytes, &*min_value, sizeof(T));
  append_bytes(max_bytes, &*max_value, sizeof(T));
  return std::pair<std::string, std::string>{std::move(min_bytes),
                                             std::move(max_bytes)};
}

std::optional<std::pair<std::string, std::string>>
boolean_min_max(const std::vector<bool> &values) {
  if (values.empty()) {
    return std::nullopt;
  }
  bool has_true = false;
  bool has_false = false;
  for (const bool value : values) {
    has_true = has_true || value;
    has_false = has_false || !value;
  }
  std::string min_bytes(1, has_false ? '\0' : '\1');
  std::string max_bytes(1, has_true ? '\1' : '\0');
  return std::pair<std::string, std::string>{std::move(min_bytes),
                                             std::move(max_bytes)};
}

bool signed_big_endian_bytes_less(std::string_view left,
                                  std::string_view right) {
  const bool left_negative =
      (static_cast<std::uint8_t>(left.front()) & 0x80U) != 0;
  const bool right_negative =
      (static_cast<std::uint8_t>(right.front()) & 0x80U) != 0;
  if (left_negative != right_negative) {
    return left_negative;
  }
  return std::lexicographical_compare(left.begin(), left.end(), right.begin(),
                                      right.end(), [](char lhs, char rhs) {
                                        return static_cast<std::uint8_t>(lhs) <
                                               static_cast<std::uint8_t>(rhs);
                                      });
}

std::optional<std::pair<std::string, std::string>>
fixed_len_byte_array_min_max(std::string_view values, std::int32_t byte_width) {
  if (byte_width <= 0 || values.empty() ||
      values.size() % static_cast<std::size_t>(byte_width) != 0) {
    return std::nullopt;
  }
  const auto width = static_cast<std::size_t>(byte_width);
  std::optional<std::string> min_value;
  std::optional<std::string> max_value;
  for (std::size_t offset = 0; offset < values.size(); offset += width) {
    std::string current(values.substr(offset, width));
    if (!min_value || signed_big_endian_bytes_less(current, *min_value)) {
      min_value = current;
    }
    if (!max_value || signed_big_endian_bytes_less(*max_value, current)) {
      max_value = std::move(current);
    }
  }
  if (!min_value || !max_value) {
    return std::nullopt;
  }
  return std::pair<std::string, std::string>{std::move(*min_value),
                                             std::move(*max_value)};
}

std::uint32_t read_u32_le(std::string_view data, std::size_t offset) {
  return static_cast<std::uint32_t>(static_cast<std::uint8_t>(data[offset])) |
         (static_cast<std::uint32_t>(
              static_cast<std::uint8_t>(data[offset + 1]))
          << 8U) |
         (static_cast<std::uint32_t>(
              static_cast<std::uint8_t>(data[offset + 2]))
          << 16U) |
         (static_cast<std::uint32_t>(
              static_cast<std::uint8_t>(data[offset + 3]))
          << 24U);
}

bool byte_array_less(std::string_view left, std::string_view right) {
  return std::lexicographical_compare(left.begin(), left.end(), right.begin(),
                                      right.end(), [](char lhs, char rhs) {
                                        return static_cast<std::uint8_t>(lhs) <
                                               static_cast<std::uint8_t>(rhs);
                                      });
}

std::optional<std::pair<std::string, std::string>>
fixed_size_binary_min_max(std::string_view values, std::int32_t byte_width) {
  if (byte_width <= 0 || values.empty() ||
      values.size() % static_cast<std::size_t>(byte_width) != 0) {
    return std::nullopt;
  }
  const auto width = static_cast<std::size_t>(byte_width);
  std::optional<std::string> min_value;
  std::optional<std::string> max_value;
  for (std::size_t offset = 0; offset < values.size(); offset += width) {
    std::string current(values.substr(offset, width));
    if (!min_value || byte_array_less(current, *min_value)) {
      min_value = current;
    }
    if (!max_value || byte_array_less(*max_value, current)) {
      max_value = std::move(current);
    }
  }
  if (!min_value || !max_value) {
    return std::nullopt;
  }
  return std::pair<std::string, std::string>{std::move(*min_value),
                                             std::move(*max_value)};
}

bool is_byte_array_kind(jsonl::JsonlKind kind) {
  return kind == jsonl::JsonlKind::kString ||
         kind == jsonl::JsonlKind::kLargeString ||
         kind == jsonl::JsonlKind::kBinary ||
         kind == jsonl::JsonlKind::kLargeBinary;
}

std::optional<std::size_t> fixed_dictionary_value_width(const ParquetNode &node,
                                                        jsonl::JsonlKind kind) {
  switch (kind) {
  case jsonl::JsonlKind::kInt8:
  case jsonl::JsonlKind::kUInt8:
  case jsonl::JsonlKind::kInt16:
  case jsonl::JsonlKind::kUInt16:
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kUInt32:
  case jsonl::JsonlKind::kDate32:
  case jsonl::JsonlKind::kTime32s:
  case jsonl::JsonlKind::kFloat32:
    return sizeof(std::int32_t);
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kUInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
  case jsonl::JsonlKind::kFloat64:
    return sizeof(std::int64_t);
  case jsonl::JsonlKind::kDecimal:
    if (node.decimal_byte_width <= 0) {
      return std::nullopt;
    }
    return static_cast<std::size_t>(node.decimal_byte_width);
  case jsonl::JsonlKind::kFixedSizeBinary:
    if (node.fixed_binary_byte_width <= 0) {
      return std::nullopt;
    }
    return static_cast<std::size_t>(node.fixed_binary_byte_width);
  default:
    return std::nullopt;
  }
}

std::optional<std::pair<std::string, std::string>>
byte_array_min_max(std::string_view values) {
  std::optional<std::string> min_value;
  std::optional<std::string> max_value;
  std::size_t offset = 0;
  while (offset < values.size()) {
    if (values.size() - offset < sizeof(std::uint32_t)) {
      return std::nullopt;
    }
    const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
    offset += sizeof(std::uint32_t);
    if (values.size() - offset < size) {
      return std::nullopt;
    }
    std::string current(values.substr(offset, size));
    offset += size;
    if (!min_value || byte_array_less(current, *min_value)) {
      min_value = current;
    }
    if (!max_value || byte_array_less(*max_value, current)) {
      max_value = std::move(current);
    }
  }
  if (!min_value || !max_value) {
    return std::nullopt;
  }
  return std::pair<std::string, std::string>{std::move(*min_value),
                                             std::move(*max_value)};
}

struct DictionaryEncodingData {
  std::string dictionary_values;
  std::string encoded_indices;
  std::int32_t dictionary_size = 0;
};

std::uint8_t dictionary_bit_width(std::size_t dictionary_size) {
  if (dictionary_size <= 1) {
    return 1;
  }
  std::uint8_t width = 0;
  std::size_t max_index = dictionary_size - 1;
  while (max_index != 0) {
    ++width;
    max_index >>= 1U;
  }
  return width;
}

void append_rle_run_u32(std::string &out, std::int64_t run_length,
                        std::uint32_t value, std::uint8_t bit_width) {
  append_varint(out, static_cast<std::uint64_t>(run_length) << 1U);
  const auto byte_width = static_cast<std::uint8_t>((bit_width + 7U) / 8U);
  for (std::uint8_t i = 0; i < byte_width; ++i) {
    out.push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
  }
}

void append_zigzag_varint(std::string &out, std::int64_t value) {
  const auto encoded = (static_cast<std::uint64_t>(value) << 1U) ^
                       static_cast<std::uint64_t>(value >> 63U);
  append_varint(out, encoded);
}

std::uint8_t bit_width_u64(std::uint64_t value) {
  std::uint8_t width = 0;
  while (value != 0) {
    ++width;
    value >>= 1U;
  }
  return width;
}

void append_bit_packed_values(std::string &out,
                              const std::vector<std::uint64_t> &values,
                              std::uint8_t bit_width) {
  if (bit_width == 0 || values.empty()) {
    return;
  }
  std::uint64_t buffer = 0;
  std::uint8_t bits_in_buffer = 0;
  for (const auto value : values) {
    std::uint8_t written = 0;
    while (written < bit_width) {
      const auto room = static_cast<std::uint8_t>(64U - bits_in_buffer);
      const auto take =
          static_cast<std::uint8_t>(std::min<int>(bit_width - written, room));
      const std::uint64_t mask = take == 64
                                     ? std::numeric_limits<std::uint64_t>::max()
                                     : ((std::uint64_t{1} << take) - 1U);
      buffer |= ((value >> written) & mask) << bits_in_buffer;
      bits_in_buffer = static_cast<std::uint8_t>(bits_in_buffer + take);
      written = static_cast<std::uint8_t>(written + take);
      while (bits_in_buffer >= 8) {
        out.push_back(static_cast<char>(buffer & 0xFFU));
        buffer >>= 8U;
        bits_in_buffer = static_cast<std::uint8_t>(bits_in_buffer - 8U);
      }
    }
  }
  if (bits_in_buffer > 0) {
    out.push_back(static_cast<char>(buffer & 0xFFU));
  }
}

std::string encode_dictionary_indices(const std::vector<std::uint32_t> &indices,
                                      std::size_t dictionary_size) {
  const auto bit_width = dictionary_bit_width(dictionary_size);
  std::string out;
  out.push_back(static_cast<char>(bit_width));
  if (indices.empty()) {
    return out;
  }
  auto current = indices.front();
  std::int64_t run_length = 1;
  for (std::size_t i = 1; i < indices.size(); ++i) {
    if (indices[i] == current) {
      ++run_length;
      continue;
    }
    append_rle_run_u32(out, run_length, current, bit_width);
    current = indices[i];
    run_length = 1;
  }
  append_rle_run_u32(out, run_length, current, bit_width);
  return out;
}

std::optional<std::int64_t> checked_i64_subtract(std::int64_t left,
                                                 std::int64_t right) {
  if ((right > 0 && left < std::numeric_limits<std::int64_t>::min() + right) ||
      (right < 0 && left > std::numeric_limits<std::int64_t>::max() + right)) {
    return std::nullopt;
  }
  return left - right;
}

std::uint64_t nonnegative_i64_difference(std::int64_t high, std::int64_t low) {
  if (low >= 0) {
    return static_cast<std::uint64_t>(high - low);
  }
  if (high < 0) {
    return static_cast<std::uint64_t>(high - low);
  }
  return static_cast<std::uint64_t>(high) +
         static_cast<std::uint64_t>(-(low + 1)) + 1U;
}

std::optional<std::string>
encode_delta_binary_packed_values(const std::vector<std::int64_t> &values) {
  constexpr std::int32_t kBlockSize = 128;
  constexpr std::int32_t kMiniBlockCount = 4;
  constexpr std::int32_t kMiniBlockSize = kBlockSize / kMiniBlockCount;
  std::string out;
  if (values.empty()) {
    return out;
  }
  append_varint(out, kBlockSize);
  append_varint(out, kMiniBlockCount);
  append_varint(out, static_cast<std::uint64_t>(values.size()));
  append_zigzag_varint(out, values.front());
  if (values.size() == 1) {
    return out;
  }

  std::vector<std::int64_t> deltas;
  deltas.reserve(values.size() - 1);
  for (std::size_t i = 1; i < values.size(); ++i) {
    auto delta = checked_i64_subtract(values[i], values[i - 1]);
    if (!delta) {
      return std::nullopt;
    }
    deltas.push_back(*delta);
  }

  for (std::size_t block_start = 0; block_start < deltas.size();
       block_start += kBlockSize) {
    const auto block_end =
        std::min<std::size_t>(block_start + kBlockSize, deltas.size());
    auto min_delta = deltas[block_start];
    for (std::size_t i = block_start + 1; i < block_end; ++i) {
      min_delta = std::min(min_delta, deltas[i]);
    }
    append_zigzag_varint(out, min_delta);

    std::array<std::uint8_t, kMiniBlockCount> bit_widths{};
    std::array<std::vector<std::uint64_t>, kMiniBlockCount> adjusted{};
    for (std::int32_t mini = 0; mini < kMiniBlockCount; ++mini) {
      adjusted[mini].reserve(kMiniBlockSize);
      std::uint64_t max_value = 0;
      const auto mini_start =
          block_start + static_cast<std::size_t>(mini * kMiniBlockSize);
      for (std::int32_t i = 0; i < kMiniBlockSize; ++i) {
        const auto delta_index = mini_start + static_cast<std::size_t>(i);
        const auto value =
            delta_index < block_end
                ? nonnegative_i64_difference(deltas[delta_index], min_delta)
                : std::uint64_t{0};
        adjusted[mini].push_back(value);
        max_value = std::max(max_value, value);
      }
      bit_widths[mini] = bit_width_u64(max_value);
      out.push_back(static_cast<char>(bit_widths[mini]));
    }
    for (std::int32_t mini = 0; mini < kMiniBlockCount; ++mini) {
      append_bit_packed_values(out, adjusted[mini], bit_widths[mini]);
    }
  }
  return out;
}

std::optional<DictionaryEncodingData>
try_dictionary_encode_column(const LeafColumn &column,
                             std::string_view values) {
  const auto kind = column.node->arrow_kind == jsonl::JsonlKind::kDictionary
                        ? column.node->dictionary_value_kind
                        : column.node->arrow_kind;
  if (kind == jsonl::JsonlKind::kBool || values.empty()) {
    return std::nullopt;
  }
  std::vector<std::uint32_t> indices;
  std::vector<std::string> dictionary;
  std::unordered_map<std::string, std::uint32_t> index_by_value;
  auto append_item = [&](std::string current) -> bool {
    auto found = index_by_value.find(current);
    if (found == index_by_value.end()) {
      if (dictionary.size() >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
        return false;
      }
      const auto next_index = static_cast<std::uint32_t>(dictionary.size());
      found = index_by_value.emplace(current, next_index).first;
      dictionary.push_back(std::move(current));
    }
    indices.push_back(found->second);
    return true;
  };

  if (is_byte_array_kind(kind)) {
    std::size_t offset = 0;
    while (offset < values.size()) {
      const auto item_start = offset;
      if (values.size() - offset < sizeof(std::uint32_t)) {
        return std::nullopt;
      }
      const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
      offset += sizeof(std::uint32_t);
      if (values.size() - offset < size) {
        return std::nullopt;
      }
      offset += size;
      if (!append_item(
              std::string(values.substr(item_start, offset - item_start)))) {
        return std::nullopt;
      }
    }
  } else {
    const auto width = fixed_dictionary_value_width(*column.node, kind);
    if (!width || *width == 0 || values.size() % *width != 0) {
      return std::nullopt;
    }
    for (std::size_t offset = 0; offset < values.size(); offset += *width) {
      if (!append_item(std::string(values.substr(offset, *width)))) {
        return std::nullopt;
      }
    }
  }

  if (dictionary.empty() || dictionary.size() >= indices.size()) {
    return std::nullopt;
  }

  DictionaryEncodingData out;
  out.dictionary_size = static_cast<std::int32_t>(dictionary.size());
  for (const auto &value : dictionary) {
    out.dictionary_values.append(value);
  }
  out.encoded_indices = encode_dictionary_indices(indices, dictionary.size());
  return out;
}

std::optional<std::pair<std::string, std::string>>
encoded_min_max_for_column(const LeafColumn &column,
                           const ColumnPageData &page_data) {
  const auto kind = column.node->arrow_kind == jsonl::JsonlKind::kDictionary
                        ? column.node->dictionary_value_kind
                        : column.node->arrow_kind;
  switch (kind) {
  case jsonl::JsonlKind::kBool:
    return boolean_min_max(page_data.bool_values);
  case jsonl::JsonlKind::kInt8:
  case jsonl::JsonlKind::kInt16:
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kDate32:
  case jsonl::JsonlKind::kTime32s:
    return fixed_width_min_max<std::int32_t>(page_data.values);
  case jsonl::JsonlKind::kUInt8:
  case jsonl::JsonlKind::kUInt16:
  case jsonl::JsonlKind::kUInt32:
    return fixed_width_min_max<std::uint32_t>(page_data.values);
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
    return fixed_width_min_max<std::int64_t>(page_data.values);
  case jsonl::JsonlKind::kUInt64:
    return fixed_width_min_max<std::uint64_t>(page_data.values);
  case jsonl::JsonlKind::kFloat32:
    return floating_min_max<float>(page_data.values);
  case jsonl::JsonlKind::kFloat64:
    return floating_min_max<double>(page_data.values);
  case jsonl::JsonlKind::kString:
  case jsonl::JsonlKind::kLargeString:
  case jsonl::JsonlKind::kBinary:
  case jsonl::JsonlKind::kLargeBinary:
    return byte_array_min_max(page_data.values);
  case jsonl::JsonlKind::kDecimal:
    return fixed_len_byte_array_min_max(page_data.values,
                                        column.node->decimal_byte_width);
  case jsonl::JsonlKind::kFixedSizeBinary:
    return fixed_size_binary_min_max(page_data.values,
                                     column.node->fixed_binary_byte_width);
  default:
    return std::nullopt;
  }
}

// Appends a null/empty entry for every primitive leaf under a node.
void emit_nulls_for_subtree(const ParquetNode &node,
                            std::vector<ColumnPageData> *pages,
                            std::int16_t definition_level,
                            std::int16_t repetition_level) {
  if (node.node_kind == NodeKind::kPrimitive) {
    auto &page = (*pages)[node.leaf_index];
    page.definition_levels.push_back(definition_level);
    page.repetition_levels.push_back(repetition_level);
    return;
  }
  for (const auto &child : node.children) {
    emit_nulls_for_subtree(child, pages, definition_level, repetition_level);
  }
  if (node.element) {
    emit_nulls_for_subtree(*node.element, pages, definition_level,
                           repetition_level);
  }
}

// Appends non-null fixed-width values from one Arrow array.
template <class T>
sanitize::Status append_fixed_value(std::string &out, const ArrowArray &array,
                                    std::int64_t row) {
  const T *values = data_buffer<T>(array);
  if (!values) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing fixed-width value buffer");
  }
  const T value = values[array.offset + row];
  append_bytes(out, &value, sizeof(T));
  return sanitize::Status::OK();
}

// Appends an Arrow integer using the physical Parquet storage width.
template <class Source, class Physical>
sanitize::Status append_integer_value(std::string &out, const ArrowArray &array,
                                      std::int64_t row) {
  const Source *values = data_buffer<Source>(array);
  if (!values) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing fixed-width value buffer");
  }
  const Physical value = static_cast<Physical>(values[array.offset + row]);
  append_bytes(out, &value, sizeof(Physical));
  return sanitize::Status::OK();
}

// Appends an Arrow time32[s] value as Parquet TIME(MILLIS).
sanitize::Status append_time32s_as_millis(std::string &out,
                                          const ArrowArray &array,
                                          std::int64_t row) {
  const std::int32_t *values = data_buffer<std::int32_t>(array);
  if (!values) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing fixed-width value buffer");
  }
  const auto seconds = static_cast<std::int64_t>(values[array.offset + row]);
  if (seconds > std::numeric_limits<std::int32_t>::max() / 1000LL ||
      seconds < std::numeric_limits<std::int32_t>::min() / 1000LL) {
    return sanitize::Status::Invalid(
        "native Parquet writer: time32[s] value overflows TIME(MILLIS)");
  }
  const auto millis = static_cast<std::int32_t>(seconds * 1000LL);
  append_bytes(out, &millis, sizeof(millis));
  return sanitize::Status::OK();
}

// Appends one non-null boolean value to a staging byte vector.
void append_bool_value(std::vector<bool> *values, const ArrowArray &array,
                       std::int64_t row) {
  values->push_back(bool_value(array, row));
}

// Appends one non-null binary or string value as a Parquet byte array.
template <class Offset>
sanitize::Status append_binary_value(std::string &out, const ArrowArray &array,
                                     std::int64_t row) {
  if (array.n_buffers <= 2 || !array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing binary value buffers");
  }
  const auto *offsets = static_cast<const Offset *>(array.buffers[1]);
  const auto *data = static_cast<const char *>(array.buffers[2]);
  const auto index = array.offset + row;
  const auto begin = offsets[index];
  const auto end = offsets[index + 1];
  if (end < begin || static_cast<std::uint64_t>(end - begin) >
                         std::numeric_limits<std::uint32_t>::max()) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid binary value length");
  }
  const auto value_size = static_cast<std::size_t>(end - begin);
  if (value_size > 0 && !data) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing binary value buffers");
  }
  append_u32_le(out, static_cast<std::uint32_t>(value_size));
  if (value_size > 0) {
    out.append(data + begin, value_size);
  }
  return sanitize::Status::OK();
}

// Appends one Arrow decimal128/decimal256 value as Parquet fixed bytes.
sanitize::Status append_decimal_value(std::string &out, const ParquetNode &node,
                                      const ArrowArray &array,
                                      std::int64_t row) {
  if (node.decimal_byte_width <= 0 || array.n_buffers <= 1 || !array.buffers ||
      !array.buffers[1]) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing decimal value buffer");
  }
  const auto *data = static_cast<const std::uint8_t *>(array.buffers[1]);
  const auto byte_width = static_cast<std::size_t>(node.decimal_byte_width);
  const auto offset = static_cast<std::size_t>(array.offset + row) * byte_width;
  for (std::size_t i = 0; i < byte_width; ++i) {
    out.push_back(static_cast<char>(data[offset + byte_width - 1U - i]));
  }
  return sanitize::Status::OK();
}

// Appends one Arrow fixed-size binary value as Parquet fixed bytes.
sanitize::Status append_fixed_size_binary_value(std::string &out,
                                                const ParquetNode &node,
                                                const ArrowArray &array,
                                                std::int64_t row) {
  if (node.fixed_binary_byte_width <= 0 || array.n_buffers <= 1 ||
      !array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing fixed-size binary value buffer");
  }
  const auto *data = static_cast<const char *>(array.buffers[1]);
  const auto byte_width =
      static_cast<std::size_t>(node.fixed_binary_byte_width);
  const auto offset = static_cast<std::size_t>(array.offset + row) * byte_width;
  out.append(data + offset, byte_width);
  return sanitize::Status::OK();
}

// Appends one non-null primitive value.
sanitize::Status append_plain_primitive_value(
    std::string &out, std::vector<bool> *bool_values, const ParquetNode &node,
    jsonl::JsonlKind value_kind, const ArrowArray &array, std::int64_t row) {
  switch (value_kind) {
  case jsonl::JsonlKind::kNull:
    return sanitize::Status::OK();
  case jsonl::JsonlKind::kBool:
    if (!bool_values) {
      return sanitize::Status::Invalid(
          "native Parquet writer: missing boolean staging buffer");
    }
    append_bool_value(bool_values, array, row);
    return sanitize::Status::OK();
  case jsonl::JsonlKind::kInt8:
    return append_integer_value<std::int8_t, std::int32_t>(out, array, row);
  case jsonl::JsonlKind::kUInt8:
    return append_integer_value<std::uint8_t, std::uint32_t>(out, array, row);
  case jsonl::JsonlKind::kInt16:
    return append_integer_value<std::int16_t, std::int32_t>(out, array, row);
  case jsonl::JsonlKind::kUInt16:
    return append_integer_value<std::uint16_t, std::uint32_t>(out, array, row);
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kDate32:
    return append_fixed_value<std::int32_t>(out, array, row);
  case jsonl::JsonlKind::kTime32s:
    return append_time32s_as_millis(out, array, row);
  case jsonl::JsonlKind::kUInt32:
    return append_fixed_value<std::uint32_t>(out, array, row);
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
    return append_fixed_value<std::int64_t>(out, array, row);
  case jsonl::JsonlKind::kUInt64:
    return append_fixed_value<std::uint64_t>(out, array, row);
  case jsonl::JsonlKind::kFloat32:
    return append_fixed_value<float>(out, array, row);
  case jsonl::JsonlKind::kFloat64:
    return append_fixed_value<double>(out, array, row);
  case jsonl::JsonlKind::kString:
  case jsonl::JsonlKind::kBinary:
    return append_binary_value<std::int32_t>(out, array, row);
  case jsonl::JsonlKind::kLargeString:
  case jsonl::JsonlKind::kLargeBinary:
    return append_binary_value<std::int64_t>(out, array, row);
  case jsonl::JsonlKind::kFixedSizeBinary:
    return append_fixed_size_binary_value(out, node, array, row);
  case jsonl::JsonlKind::kDecimal:
    return append_decimal_value(out, node, array, row);
  default:
    return sanitize::Status::NotImplemented(kUnsupportedPrefix,
                                            " unsupported column value kind");
  }
}

// Appends one non-null dictionary value as its plain value representation.
sanitize::Status append_dictionary_value(std::string &out,
                                         std::vector<bool> *bool_values,
                                         const ParquetNode &node,
                                         const ArrowArray &array,
                                         std::int64_t row) {
  if (!array.dictionary) {
    return sanitize::Status::Invalid(
        "native Parquet writer: dictionary column has no dictionary values");
  }
  auto index = dictionary_index_at(array, node.dictionary_index_kind, row);
  if (!index || *index < 0 || *index >= array.dictionary->length) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid dictionary index");
  }
  return append_plain_primitive_value(out, bool_values, node,
                                      node.dictionary_value_kind,
                                      *array.dictionary, *index);
}

// Returns whether a non-null dictionary index points at a null dictionary
// value.
sanitize::Result<bool> dictionary_value_is_null(const ParquetNode &node,
                                                const ArrowArray &array,
                                                std::int64_t row) {
  if (node.arrow_kind != jsonl::JsonlKind::kDictionary) {
    return false;
  }
  if (!array.dictionary) {
    return sanitize::Status::Invalid(
        "native Parquet writer: dictionary column has no dictionary values");
  }
  auto index = dictionary_index_at(array, node.dictionary_index_kind, row);
  if (!index || *index < 0 || *index >= array.dictionary->length) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid dictionary index");
  }
  const bool is_null = !bitmap_is_valid(*array.dictionary, *index);
  if (is_null && node.required) {
    return sanitize::Status::Invalid(
        "native Parquet writer: required dictionary value is null");
  }
  return is_null;
}

// Appends one non-null primitive value.
sanitize::Status append_primitive_value(std::string &out,
                                        std::vector<bool> *bool_values,
                                        const ParquetNode &node,
                                        const ArrowArray &array,
                                        std::int64_t row) {
  if (node.arrow_kind == jsonl::JsonlKind::kDictionary) {
    return append_dictionary_value(out, bool_values, node, array, row);
  }
  return append_plain_primitive_value(out, bool_values, node, node.arrow_kind,
                                      array, row);
}

// Recursively appends values and levels for one node.
sanitize::Status collect_node(const ParquetNode &node, const ArrowArray &array,
                              std::int64_t row,
                              std::vector<ColumnPageData> *pages,
                              std::int16_t definition_level,
                              std::int16_t repetition_level);

// Recursively appends values and levels for a present list element.
sanitize::Status collect_list_element(const ParquetNode &element,
                                      const ArrowArray &values,
                                      std::int64_t row,
                                      std::vector<ColumnPageData> *pages,
                                      std::int16_t definition_level,
                                      std::int16_t repetition_level) {
  return collect_node(element, values, row, pages, definition_level,
                      repetition_level);
}

sanitize::Status collect_node(const ParquetNode &node, const ArrowArray &array,
                              std::int64_t row,
                              std::vector<ColumnPageData> *pages,
                              std::int16_t definition_level,
                              std::int16_t repetition_level) {
  if (node.node_kind != NodeKind::kRoot && !node.required &&
      !bitmap_is_valid(array, row)) {
    emit_nulls_for_subtree(node, pages, definition_level, repetition_level);
    return sanitize::Status::OK();
  }

  switch (node.node_kind) {
  case NodeKind::kRoot:
    for (std::size_t i = 0; i < node.children.size(); ++i) {
      if (!array.children || !array.children[i]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing root child array");
      }
      SAN_RETURN_NOT_OK(collect_node(node.children[i], *array.children[i], row,
                                     pages, definition_level,
                                     repetition_level));
    }
    return sanitize::Status::OK();
  case NodeKind::kStruct: {
    const auto present_definition =
        static_cast<std::int16_t>(definition_level + (node.required ? 0 : 1));
    const auto child_row = array.offset + row;
    for (std::size_t i = 0; i < node.children.size(); ++i) {
      if (!array.children || !array.children[i]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing struct child array");
      }
      SAN_RETURN_NOT_OK(collect_node(node.children[i], *array.children[i],
                                     child_row, pages, present_definition,
                                     repetition_level));
    }
    return sanitize::Status::OK();
  }
  case NodeKind::kList: {
    if (!array.children || !array.children[0]) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid list array");
    }
    const auto present_definition =
        static_cast<std::int16_t>(definition_level + 1);
    const auto repeated_definition =
        static_cast<std::int16_t>(present_definition + 1);
    const auto repeated_repetition = node.repeated_repetition_level;
    const auto index = array.offset + row;
    std::int64_t begin = 0;
    std::int64_t end = 0;
    if (node.arrow_kind == jsonl::JsonlKind::kFixedSizeList) {
      if (node.fixed_size_list_size < 0) {
        return sanitize::Status::Invalid(
            "native Parquet writer: invalid fixed-size list size");
      }
      begin = index * node.fixed_size_list_size;
      end = begin + node.fixed_size_list_size;
    } else if (node.arrow_kind == jsonl::JsonlKind::kLargeList) {
      if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing large-list offsets");
      }
      const auto *offsets = static_cast<const std::int64_t *>(array.buffers[1]);
      begin = offsets[index];
      end = offsets[index + 1];
    } else {
      if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing list offsets");
      }
      const auto *offsets = static_cast<const std::int32_t *>(array.buffers[1]);
      begin = offsets[index];
      end = offsets[index + 1];
    }
    if (end < begin) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid list offsets");
    }
    if (begin == end) {
      emit_nulls_for_subtree(node, pages, present_definition, repetition_level);
      return sanitize::Status::OK();
    }
    for (std::int64_t item = begin; item < end; ++item) {
      const auto item_repetition =
          item == begin ? repetition_level : repeated_repetition;
      SAN_RETURN_NOT_OK(collect_list_element(*node.element, *array.children[0],
                                             item, pages, repeated_definition,
                                             item_repetition));
    }
    return sanitize::Status::OK();
  }
  case NodeKind::kMap: {
    if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1] ||
        !array.children || !array.children[0]) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid map array");
    }
    const auto present_definition =
        static_cast<std::int16_t>(definition_level + 1);
    const auto entry_definition =
        static_cast<std::int16_t>(present_definition + 1);
    const auto entry_repetition = node.repeated_repetition_level;
    const auto index = array.offset + row;
    const auto *offsets = static_cast<const std::int32_t *>(array.buffers[1]);
    const auto begin = offsets[index];
    const auto end = offsets[index + 1];
    if (end < begin) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid map offsets");
    }
    if (begin == end) {
      emit_nulls_for_subtree(node, pages, present_definition, repetition_level);
      return sanitize::Status::OK();
    }
    for (std::int64_t item = begin; item < end; ++item) {
      const auto item_repetition =
          item == begin ? repetition_level : entry_repetition;
      SAN_RETURN_NOT_OK(collect_node(*node.element, *array.children[0], item,
                                     pages, entry_definition, item_repetition));
    }
    return sanitize::Status::OK();
  }
  case NodeKind::kPrimitive: {
    auto &page = (*pages)[node.leaf_index];
    if (node.arrow_kind == jsonl::JsonlKind::kNull) {
      page.definition_levels.push_back(definition_level);
      page.repetition_levels.push_back(repetition_level);
      return sanitize::Status::OK();
    }
    SAN_ASSIGN_OR_RAISE(const bool dictionary_null,
                        dictionary_value_is_null(node, array, row));
    if (dictionary_null) {
      page.definition_levels.push_back(definition_level);
      page.repetition_levels.push_back(repetition_level);
      return sanitize::Status::OK();
    }
    page.definition_levels.push_back(
        static_cast<std::int16_t>(definition_level + (node.required ? 0 : 1)));
    page.repetition_levels.push_back(repetition_level);
    return append_primitive_value(page.values, &page.bool_values, node, array,
                                  row);
  }
  }
  return sanitize::Status::Invalid("native Parquet writer: invalid node kind");
}

// Encodes staged boolean values into Parquet PLAIN bytes.
void encode_bool_values_plain(std::string *out,
                              const std::vector<bool> &values) {
  std::uint8_t byte = 0;
  int bit = 0;
  for (const bool value : values) {
    if (value) {
      byte |= static_cast<std::uint8_t>(std::uint8_t{1} << bit);
    }
    ++bit;
    if (bit == 8) {
      out->push_back(static_cast<char>(byte));
      byte = 0;
      bit = 0;
    }
  }
  if (bit != 0) {
    out->push_back(static_cast<char>(byte));
  }
}

// Collects one batch into column page data.
sanitize::Result<std::vector<ColumnPageData>> collect_batch_pages(
    const ParquetNode &root, const std::vector<LeafColumn> &leafs,
    const ArrowArray &array, std::int64_t row_offset, std::int64_t row_count) {
  std::vector<ColumnPageData> pages(leafs.size());
  const auto row_end = row_offset + row_count;
  for (std::int64_t row = row_offset; row < row_end; ++row) {
    SAN_RETURN_NOT_OK(collect_node(root, array, row, &pages, 0, 0));
  }
  for (std::size_t i = 0; i < leafs.size(); ++i) {
    if (leafs[i].node &&
        (leafs[i].node->arrow_kind == jsonl::JsonlKind::kBool ||
         (leafs[i].node->arrow_kind == jsonl::JsonlKind::kDictionary &&
          leafs[i].node->dictionary_value_kind == jsonl::JsonlKind::kBool))) {
      encode_bool_values_plain(&pages[i].values, pages[i].bool_values);
    }
  }
  return pages;
}

bool is_float_column(const LeafColumn &column) {
  if (!column.node) {
    return false;
  }
  const auto kind = column.node->arrow_kind == jsonl::JsonlKind::kDictionary
                        ? column.node->dictionary_value_kind
                        : column.node->arrow_kind;
  return kind == jsonl::JsonlKind::kFloat32 ||
         kind == jsonl::JsonlKind::kFloat64;
}

std::size_t float_value_width(const LeafColumn &column) {
  const auto kind = column.node->arrow_kind == jsonl::JsonlKind::kDictionary
                        ? column.node->dictionary_value_kind
                        : column.node->arrow_kind;
  return kind == jsonl::JsonlKind::kFloat32 ? sizeof(float) : sizeof(double);
}

jsonl::JsonlKind leaf_value_kind(const LeafColumn &column) {
  return column.node->arrow_kind == jsonl::JsonlKind::kDictionary
             ? column.node->dictionary_value_kind
             : column.node->arrow_kind;
}

std::optional<std::vector<std::int64_t>>
signed_delta_values_for_column(const LeafColumn &column,
                               std::string_view values) {
  const auto kind = leaf_value_kind(column);
  switch (kind) {
  case jsonl::JsonlKind::kInt8:
  case jsonl::JsonlKind::kInt16:
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kDate32:
  case jsonl::JsonlKind::kTime32s: {
    if (values.empty() || values.size() % sizeof(std::int32_t) != 0) {
      return std::nullopt;
    }
    std::vector<std::int64_t> out;
    out.reserve(values.size() / sizeof(std::int32_t));
    for (std::size_t offset = 0; offset < values.size();
         offset += sizeof(std::int32_t)) {
      std::int32_t value{};
      std::memcpy(&value, values.data() + offset, sizeof(value));
      out.push_back(value);
    }
    return out;
  }
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos: {
    if (values.empty() || values.size() % sizeof(std::int64_t) != 0) {
      return std::nullopt;
    }
    std::vector<std::int64_t> out;
    out.reserve(values.size() / sizeof(std::int64_t));
    for (std::size_t offset = 0; offset < values.size();
         offset += sizeof(std::int64_t)) {
      std::int64_t value{};
      std::memcpy(&value, values.data() + offset, sizeof(value));
      out.push_back(value);
    }
    return out;
  }
  default:
    return std::nullopt;
  }
}

// Encodes float values with Parquet BYTE_STREAM_SPLIT.
std::optional<std::string> encode_byte_stream_split(std::string_view values,
                                                    std::size_t width) {
  if (width == 0 || values.empty() || values.size() % width != 0) {
    return std::nullopt;
  }
  const auto count = values.size() / width;
  std::string out;
  out.reserve(values.size());
  for (std::size_t byte_index = 0; byte_index < width; ++byte_index) {
    for (std::size_t value_index = 0; value_index < count; ++value_index) {
      out.push_back(values[value_index * width + byte_index]);
    }
  }
  return out;
}

std::optional<std::string>
encode_delta_binary_packed_column(const LeafColumn &column,
                                  std::string_view values) {
  auto extracted = signed_delta_values_for_column(column, values);
  if (!extracted || extracted->size() < 2) {
    return std::nullopt;
  }
  return encode_delta_binary_packed_values(*extracted);
}

std::optional<std::string>
encode_delta_length_byte_array(std::string_view values) {
  if (values.empty()) {
    return std::nullopt;
  }
  std::vector<std::int64_t> lengths;
  std::string bytes;
  std::size_t offset = 0;
  while (offset < values.size()) {
    if (values.size() - offset < sizeof(std::uint32_t)) {
      return std::nullopt;
    }
    const auto size = read_u32_le(values, offset);
    offset += sizeof(std::uint32_t);
    if (values.size() - offset < size) {
      return std::nullopt;
    }
    lengths.push_back(static_cast<std::int64_t>(size));
    bytes.append(values.substr(offset, size));
    offset += size;
  }
  if (lengths.size() < 2) {
    return std::nullopt;
  }
  auto encoded_lengths = encode_delta_binary_packed_values(lengths);
  if (!encoded_lengths) {
    return std::nullopt;
  }
  std::string out = std::move(*encoded_lengths);
  out.append(bytes);
  return out;
}

struct EncodedValueCandidate {
  std::int32_t encoding = kEncodingPlain;
  std::string values;
  std::optional<DictionaryEncodingData> dictionary;
};

std::vector<EncodedValueCandidate>
encoded_value_candidates(const LeafColumn &column,
                         const ColumnPageData &page_data,
                         bool allow_dictionary) {
  std::vector<EncodedValueCandidate> candidates;
  candidates.push_back(EncodedValueCandidate{
      .encoding = kEncodingPlain,
      .values = std::string(page_data.values),
      .dictionary = std::nullopt,
  });

  if (allow_dictionary) {
    if (auto dictionary =
            try_dictionary_encode_column(column, page_data.values)) {
      candidates.push_back(EncodedValueCandidate{
          .encoding = kEncodingRleDictionary,
          .values = dictionary->encoded_indices,
          .dictionary = std::move(dictionary),
      });
    }
  }

  if (is_float_column(column)) {
    if (auto encoded = encode_byte_stream_split(page_data.values,
                                                float_value_width(column))) {
      candidates.push_back(EncodedValueCandidate{
          .encoding = kEncodingByteStreamSplit,
          .values = std::move(*encoded),
          .dictionary = std::nullopt,
      });
    }
  }

  if (auto encoded =
          encode_delta_binary_packed_column(column, page_data.values)) {
    candidates.push_back(EncodedValueCandidate{
        .encoding = kEncodingDeltaBinaryPacked,
        .values = std::move(*encoded),
        .dictionary = std::nullopt,
    });
  }

  if (is_byte_array_kind(leaf_value_kind(column))) {
    if (auto encoded = encode_delta_length_byte_array(page_data.values)) {
      candidates.push_back(EncodedValueCandidate{
          .encoding = kEncodingDeltaLengthByteArray,
          .values = std::move(*encoded),
          .dictionary = std::nullopt,
      });
    }
  }

  return candidates;
}

std::string encode_page_levels_and_values(const LeafColumn &column,
                                          const ColumnPageData &page_data,
                                          std::string_view encoded_values) {
  std::string payload;
  if (column.max_repetition_level > 0) {
    payload.append(encode_levels(page_data.repetition_levels,
                                 column.max_repetition_level));
  }
  payload.append(
      encode_levels(page_data.definition_levels, column.max_definition_level));
  payload.append(encoded_values);
  return payload;
}

struct PreparedPagePayload {
  EncodedValueCandidate candidate;
  std::string payload;
  std::string compressed_payload;
  std::string compressed_dictionary;
};

bool prefers_dictionary_tie_break(const LeafColumn &column) {
  if (column.path.empty()) {
    return false;
  }
  const auto &name = column.path.back();
  return name == "source_file" || name == "schema_registry" ||
         name == "schema_drifts" || name == "year" || name == "month" ||
         name == "date" || name == "hour";
}

sanitize::Result<PreparedPagePayload>
choose_page_payload(const LeafColumn &column, const ColumnPageData &page_data,
                    CompressionCodec codec, bool allow_dictionary) {
  auto candidates =
      encoded_value_candidates(column, page_data, allow_dictionary);
  std::optional<PreparedPagePayload> best;
  std::size_t best_size = std::numeric_limits<std::size_t>::max();
  for (auto &candidate : candidates) {
    std::string payload =
        encode_page_levels_and_values(column, page_data, candidate.values);
    SAN_ASSIGN_OR_RAISE(auto compressed_payload,
                        compress_payload(payload, codec));
    std::string compressed_dictionary;
    std::size_t candidate_size = compressed_payload.size();
    if (candidate.dictionary) {
      SAN_ASSIGN_OR_RAISE(
          compressed_dictionary,
          compress_payload(candidate.dictionary->dictionary_values, codec));
      candidate_size += compressed_dictionary.size();
    }
    const bool dictionary_tie_break = candidate.dictionary &&
                                      candidate_size == best_size &&
                                      prefers_dictionary_tie_break(column);
    if (!best || candidate_size < best_size || dictionary_tie_break) {
      best_size = candidate_size;
      best = PreparedPagePayload{
          .candidate = std::move(candidate),
          .payload = std::move(payload),
          .compressed_payload = std::move(compressed_payload),
          .compressed_dictionary = std::move(compressed_dictionary),
      };
    }
  }
  if (!best) {
    return sanitize::Status::Invalid(
        "native Parquet writer: no encodable Parquet page candidate");
  }
  return std::move(*best);
}

// Writes one column data page and returns its file offsets and sizes.
sanitize::Result<PageInfo> write_column_page(CountingOutput &out,
                                             const LeafColumn &column,
                                             const ColumnPageData &page_data,
                                             CompressionCodec codec,
                                             bool allow_dictionary) {
  SAN_ASSIGN_OR_RAISE(
      auto prepared,
      choose_page_payload(column, page_data, codec, allow_dictionary));
  if (prepared.payload.size() >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()) ||
      page_data.definition_levels.size() >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: page is too large");
  }
  if (prepared.compressed_payload.size() >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: compressed page is too large");
  }
  std::string header = encode_data_page_header(
      static_cast<std::int32_t>(page_data.definition_levels.size()),
      static_cast<std::int32_t>(prepared.payload.size()),
      static_cast<std::int32_t>(prepared.compressed_payload.size()),
      prepared.candidate.encoding);
  PageInfo info;
  info.value_encoding = prepared.candidate.encoding;
  info.has_plain_encoding = prepared.candidate.encoding == kEncodingPlain;
  info.has_delta_binary_packed_encoding =
      prepared.candidate.encoding == kEncodingDeltaBinaryPacked;
  info.has_delta_length_byte_array_encoding =
      prepared.candidate.encoding == kEncodingDeltaLengthByteArray;
  info.has_byte_stream_split_encoding =
      prepared.candidate.encoding == kEncodingByteStreamSplit;
  if (prepared.candidate.dictionary) {
    if (prepared.candidate.dictionary->dictionary_values.size() >
        static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
      return sanitize::Status::Invalid(
          "native Parquet writer: dictionary page is too large");
    }
    if (prepared.compressed_dictionary.size() >
        static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
      return sanitize::Status::Invalid(
          "native Parquet writer: compressed dictionary page is too large");
    }
    std::string dictionary_header = encode_dictionary_page_header(
        prepared.candidate.dictionary->dictionary_size,
        static_cast<std::int32_t>(
            prepared.candidate.dictionary->dictionary_values.size()),
        static_cast<std::int32_t>(prepared.compressed_dictionary.size()));
    info.dictionary_encoded = true;
    info.dictionary_page_offset = out.offset();
    info.first_page_offset = info.dictionary_page_offset;
    info.dictionary_header_size =
        static_cast<std::int64_t>(dictionary_header.size());
    info.dictionary_uncompressed_payload_size = static_cast<std::int64_t>(
        prepared.candidate.dictionary->dictionary_values.size());
    info.dictionary_compressed_payload_size =
        static_cast<std::int64_t>(prepared.compressed_dictionary.size());
    SAN_RETURN_NOT_OK(out.Write(dictionary_header));
    SAN_RETURN_NOT_OK(out.Write(prepared.compressed_dictionary));
  }

  info.offset = out.offset();
  if (!prepared.candidate.dictionary) {
    info.first_page_offset = info.offset;
  }
  info.header_size = static_cast<std::int64_t>(header.size());
  info.uncompressed_payload_size =
      static_cast<std::int64_t>(prepared.payload.size());
  info.compressed_payload_size =
      static_cast<std::int64_t>(prepared.compressed_payload.size());
  info.num_values =
      static_cast<std::int64_t>(page_data.definition_levels.size());
  info.null_count = static_cast<std::int64_t>(std::count_if(
      page_data.definition_levels.begin(), page_data.definition_levels.end(),
      [&](std::int16_t level) { return level < column.max_definition_level; }));
  if (auto bounds = encoded_min_max_for_column(column, page_data)) {
    info.min_value = std::move(bounds->first);
    info.max_value = std::move(bounds->second);
  }
  SAN_RETURN_NOT_OK(out.Write(header));
  SAN_RETURN_NOT_OK(out.Write(prepared.compressed_payload));
  return info;
}

std::size_t fixed_value_width_for_leaf(const LeafColumn &column) {
  if (leaf_value_kind(column) == jsonl::JsonlKind::kBool) {
    return 0;
  }
  return fixed_dictionary_value_width(*column.node, leaf_value_kind(column))
      .value_or(0);
}

std::size_t estimate_column_page_data_bytes(const ColumnPageData &page_data) {
  return page_data.values.size() + page_data.bool_values.size() +
         page_data.definition_levels.size() * sizeof(std::int16_t) +
         page_data.repetition_levels.size() * sizeof(std::int16_t);
}

std::size_t
estimate_row_group_page_data_bytes(const std::vector<ColumnPageData> &pages) {
  std::size_t total = 0;
  for (const auto &page : pages) {
    total += estimate_column_page_data_bytes(page);
  }
  return total;
}

std::vector<std::pair<std::size_t, std::size_t>>
byte_array_value_offsets(std::string_view values) {
  std::vector<std::pair<std::size_t, std::size_t>> offsets;
  std::size_t offset = 0;
  while (offset < values.size()) {
    const auto start = offset;
    if (values.size() - offset < sizeof(std::uint32_t)) {
      return {};
    }
    const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
    offset += sizeof(std::uint32_t);
    if (values.size() - offset < size) {
      return {};
    }
    offset += size;
    offsets.emplace_back(start, offset);
  }
  return offsets;
}

ColumnPageData slice_column_page_data(const LeafColumn &column,
                                      const ColumnPageData &page_data,
                                      std::size_t begin, std::size_t end) {
  ColumnPageData out;
  out.definition_levels.insert(out.definition_levels.end(),
                               page_data.definition_levels.begin() + begin,
                               page_data.definition_levels.begin() + end);
  if (column.max_repetition_level > 0) {
    out.repetition_levels.insert(out.repetition_levels.end(),
                                 page_data.repetition_levels.begin() + begin,
                                 page_data.repetition_levels.begin() + end);
  }

  const auto kind = leaf_value_kind(column);
  std::size_t value_index = 0;
  std::size_t fixed_offset = 0;
  const auto fixed_width = fixed_value_width_for_leaf(column);
  auto byte_offsets = is_byte_array_kind(kind)
                          ? byte_array_value_offsets(page_data.values)
                          : std::vector<std::pair<std::size_t, std::size_t>>{};
  for (std::size_t i = 0; i < end; ++i) {
    const bool has_value =
        page_data.definition_levels[i] == column.max_definition_level;
    if (!has_value) {
      continue;
    }
    const bool in_slice = i >= begin;
    if (kind == jsonl::JsonlKind::kBool) {
      if (in_slice && value_index < page_data.bool_values.size()) {
        out.bool_values.push_back(page_data.bool_values[value_index]);
      }
      ++value_index;
      continue;
    }
    if (is_byte_array_kind(kind)) {
      if (in_slice && value_index < byte_offsets.size()) {
        const auto [start, stop] = byte_offsets[value_index];
        out.values.append(page_data.values.substr(start, stop - start));
      }
      ++value_index;
      continue;
    }
    if (fixed_width > 0) {
      if (in_slice && fixed_offset + fixed_width <= page_data.values.size()) {
        out.values.append(page_data.values.substr(fixed_offset, fixed_width));
      }
      fixed_offset += fixed_width;
    }
  }
  if (kind == jsonl::JsonlKind::kBool) {
    encode_bool_values_plain(&out.values, out.bool_values);
  }
  return out;
}

std::vector<ColumnPageData>
split_column_page_data(const LeafColumn &column,
                       const ColumnPageData &page_data,
                       std::int64_t target_page_bytes) {
  if (target_page_bytes <= 0 ||
      static_cast<std::int64_t>(estimate_column_page_data_bytes(page_data)) <=
          target_page_bytes ||
      page_data.definition_levels.size() <= 1) {
    return {page_data};
  }

  std::vector<ColumnPageData> out;
  const auto count = page_data.definition_levels.size();
  std::size_t begin = 0;
  while (begin < count) {
    std::size_t end = begin + 1;
    ColumnPageData best = slice_column_page_data(column, page_data, begin, end);
    while (end < count) {
      ColumnPageData candidate =
          slice_column_page_data(column, page_data, begin, end + 1);
      if (static_cast<std::int64_t>(
              estimate_column_page_data_bytes(candidate)) > target_page_bytes &&
          end > begin) {
        break;
      }
      best = std::move(candidate);
      ++end;
    }
    out.push_back(std::move(best));
    begin = end;
  }
  return out;
}

void merge_page_info(PageInfo &target, const PageInfo &page) {
  if (target.num_values == 0) {
    target = page;
    return;
  }
  if (target.dictionary_page_offset < 0 && page.dictionary_page_offset >= 0) {
    target.dictionary_page_offset = page.dictionary_page_offset;
  }
  target.dictionary_header_size += page.dictionary_header_size;
  target.dictionary_uncompressed_payload_size +=
      page.dictionary_uncompressed_payload_size;
  target.dictionary_compressed_payload_size +=
      page.dictionary_compressed_payload_size;
  target.header_size += page.header_size;
  target.uncompressed_payload_size += page.uncompressed_payload_size;
  target.compressed_payload_size += page.compressed_payload_size;
  target.num_values += page.num_values;
  target.null_count += page.null_count;
  target.dictionary_encoded =
      target.dictionary_encoded || page.dictionary_encoded;
  target.has_plain_encoding =
      target.has_plain_encoding || page.has_plain_encoding;
  target.has_delta_binary_packed_encoding =
      target.has_delta_binary_packed_encoding ||
      page.has_delta_binary_packed_encoding;
  target.has_delta_length_byte_array_encoding =
      target.has_delta_length_byte_array_encoding ||
      page.has_delta_length_byte_array_encoding;
  target.has_byte_stream_split_encoding =
      target.has_byte_stream_split_encoding ||
      page.has_byte_stream_split_encoding;
  target.min_value.reset();
  target.max_value.reset();
}

std::string encode_column_index(const std::vector<PageInfo> &pages) {
  std::vector<bool> null_pages;
  std::vector<std::string> min_values;
  std::vector<std::string> max_values;
  std::vector<std::int64_t> null_counts;
  null_pages.reserve(pages.size());
  min_values.reserve(pages.size());
  max_values.reserve(pages.size());
  null_counts.reserve(pages.size());
  for (const auto &page : pages) {
    const bool null_page =
        page.num_values > 0 && page.null_count == page.num_values;
    null_pages.push_back(null_page);
    min_values.push_back(page.min_value.value_or(std::string{}));
    max_values.push_back(page.max_value.value_or(std::string{}));
    null_counts.push_back(page.null_count);
  }

  std::string out;
  CompactWriter writer(out);
  writer.FieldListBool(1, null_pages);
  writer.FieldListString(2, min_values);
  writer.FieldListString(3, max_values);
  writer.FieldI32(4, 0); // UNORDERED
  writer.FieldListI64(5, null_counts);
  writer.Stop();
  return out;
}

bool page_is_null_page(const PageInfo &page) {
  return page.num_values > 0 && page.null_count == page.num_values;
}

bool can_encode_column_index(const std::vector<PageInfo> &pages) {
  for (const auto &page : pages) {
    if (page_is_null_page(page)) {
      continue;
    }
    if (!page.min_value || !page.max_value) {
      return false;
    }
  }
  return true;
}

std::string encode_offset_index(const std::vector<PageInfo> &pages) {
  std::string out;
  CompactWriter writer(out);
  writer.FieldListStruct(1, pages.size(), [&](CompactWriter &) {
    std::int64_t first_row_index = 0;
    for (const auto &page : pages) {
      CompactWriter location(out);
      location.FieldI64(1, page.offset);
      location.FieldI32(2,
                        static_cast<std::int32_t>(
                            page.header_size + page.compressed_payload_size));
      location.FieldI64(3, first_row_index);
      location.Stop();
      first_row_index += page.num_values;
    }
  });
  writer.Stop();
  return out;
}

sanitize::Status write_page_indexes(CountingOutput &out,
                                    const std::vector<LeafColumn> &columns,
                                    std::vector<RowGroupInfo> *row_groups) {
  if (!row_groups) {
    return sanitize::Status::Invalid("native Parquet writer: null row groups");
  }
  for (auto &row_group : *row_groups) {
    for (std::size_t i = 0; i < row_group.columns.size(); ++i) {
      auto &chunk = row_group.columns[i];
      if (i >= columns.size() || columns[i].max_repetition_level != 0 ||
          chunk.pages.empty()) {
        continue;
      }
      if (can_encode_column_index(chunk.pages)) {
        std::string column_index = encode_column_index(chunk.pages);
        if (column_index.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::int32_t>::max())) {
          return sanitize::Status::Invalid(
              "native Parquet writer: column index is too large");
        }
        chunk.column_index_offset = out.offset();
        chunk.column_index_length =
            static_cast<std::int32_t>(column_index.size());
        SAN_RETURN_NOT_OK(out.Write(column_index));
      }

      std::string offset_index = encode_offset_index(chunk.pages);
      if (offset_index.size() >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
        return sanitize::Status::Invalid(
            "native Parquet writer: offset index is too large");
      }
      chunk.offset_index_offset = out.offset();
      chunk.offset_index_length =
          static_cast<std::int32_t>(offset_index.size());
      SAN_RETURN_NOT_OK(out.Write(offset_index));
    }
  }
  return sanitize::Status::OK();
}

sanitize::Result<ColumnChunkInfo>
write_column_pages(CountingOutput &out, const LeafColumn &column,
                   const ColumnPageData &page_data, CompressionCodec codec,
                   std::int64_t target_page_bytes) {
  auto pages = split_column_page_data(column, page_data, target_page_bytes);
  ColumnChunkInfo chunk;
  const bool allow_dictionary = pages.size() == 1;
  for (const auto &page_data_part : pages) {
    SAN_ASSIGN_OR_RAISE(auto page,
                        write_column_page(out, column, page_data_part, codec,
                                          allow_dictionary));
    merge_page_info(chunk.aggregate, page);
    chunk.pages.push_back(std::move(page));
  }
  return chunk;
}

// Validates that a record batch has a root struct array with expected children.
sanitize::Status validate_batch(const ParquetNode &root,
                                const ArrowArray &array) {
  if (array.length < 0) {
    return sanitize::Status::Invalid(
        "native Parquet writer: negative batch length");
  }
  if (root.node_kind != NodeKind::kRoot) {
    return sanitize::Status::Invalid(
        "native Parquet writer: root is not struct");
  }
  if (array.n_children != static_cast<std::int64_t>(root.children.size()) ||
      (!root.children.empty() && !array.children)) {
    return sanitize::Status::Invalid(
        "native Parquet writer: root array/schema mismatch");
  }
  return sanitize::Status::OK();
}

// Writes a primitive schema element into the compact footer.
void write_primitive_schema_element(CompactWriter &writer,
                                    const ParquetNode &node) {
  writer.FieldI32(1, static_cast<std::int32_t>(node.physical_type));
  if (node.physical_type == PhysicalType::kFixedLenByteArray) {
    writer.FieldI32(2, node.fixed_binary_byte_width);
  }
  writer.FieldI32(3, node.required ? 0 : 1);
  writer.FieldString(4, node.name);
  if (node.has_converted_type) {
    writer.FieldI32(6, static_cast<std::int32_t>(node.converted_type));
  }
  if (node.has_decimal_metadata) {
    writer.FieldI32(7, node.decimal_scale);
    writer.FieldI32(8, node.decimal_precision);
  }
  if (node.has_null_logical_type) {
    writer.FieldStruct(10, [&](CompactWriter &logical) {
      logical.FieldStruct(11, [](CompactWriter &) {});
    });
  } else if (node.has_timestamp_logical_type) {
    writer.FieldStruct(10, [&](CompactWriter &logical) {
      logical.FieldStruct(8, [&](CompactWriter &timestamp) {
        timestamp.FieldBool(1, false);
        timestamp.FieldStruct(2, [&](CompactWriter &unit) {
          const std::int16_t unit_field =
              node.arrow_kind == jsonl::JsonlKind::kTimestampMillis   ? 1
              : node.arrow_kind == jsonl::JsonlKind::kTimestampMicros ? 2
                                                                      : 3;
          unit.FieldStruct(unit_field, [](CompactWriter &) {});
        });
      });
    });
  } else if (node.has_time_millis_logical_type) {
    writer.FieldStruct(10, [&](CompactWriter &logical) {
      logical.FieldStruct(7, [&](CompactWriter &time) {
        time.FieldBool(1, false);
        time.FieldStruct(2, [&](CompactWriter &unit) {
          unit.FieldStruct(1, [](CompactWriter &) {});
        });
      });
    });
  } else if (node.has_int_logical_type) {
    writer.FieldStruct(10, [&](CompactWriter &logical) {
      logical.FieldStruct(10, [&](CompactWriter &integer) {
        integer.FieldByte(1, node.int_bit_width);
        integer.FieldBool(2, node.int_is_signed);
      });
    });
  }
}

// Writes one schema element and all descendants in preorder.
void write_schema_elements(CompactWriter &, std::string &out,
                           const ParquetNode &node) {
  CompactWriter item(out);
  if (node.node_kind == NodeKind::kPrimitive) {
    write_primitive_schema_element(item, node);
    item.Stop();
    return;
  }
  item.FieldString(4, node.name);
  if (node.node_kind != NodeKind::kRoot) {
    item.FieldI32(3, node.required ? 0 : 1);
  }
  if (node.node_kind == NodeKind::kList) {
    item.FieldI32(5, 1);
    item.FieldI32(6, static_cast<std::int32_t>(ConvertedType::kList));
  } else if (node.node_kind == NodeKind::kMap) {
    item.FieldI32(5, 1);
    item.FieldI32(6, static_cast<std::int32_t>(ConvertedType::kMap));
  } else if (node.node_kind == NodeKind::kStruct ||
             node.node_kind == NodeKind::kRoot) {
    item.FieldI32(5, static_cast<std::int32_t>(node.children.size()));
  }
  item.Stop();
  if (node.node_kind == NodeKind::kList) {
    CompactWriter repeated(out);
    repeated.FieldI32(3, 2); // REPEATED
    repeated.FieldString(4, "list");
    repeated.FieldI32(5, 1);
    repeated.Stop();
    write_schema_elements(repeated, out, *node.element);
    return;
  }
  if (node.node_kind == NodeKind::kMap) {
    CompactWriter repeated(out);
    repeated.FieldI32(3, 2); // REPEATED
    repeated.FieldString(4, "key_value");
    repeated.FieldI32(5,
                      static_cast<std::int32_t>(node.element->children.size()));
    repeated.Stop();
    for (const auto &child : node.element->children) {
      write_schema_elements(repeated, out, child);
    }
    return;
  }
  for (const auto &child : node.children) {
    write_schema_elements(item, out, child);
  }
}

// Counts Parquet schema elements under one node.
std::size_t schema_element_count(const ParquetNode &node) {
  std::size_t count = 1;
  if (node.node_kind == NodeKind::kList) {
    return count + 1 + schema_element_count(*node.element);
  }
  if (node.node_kind == NodeKind::kMap) {
    for (const auto &child : node.element->children) {
      count += schema_element_count(child);
    }
    return count + 1;
  }
  for (const auto &child : node.children) {
    count += schema_element_count(child);
  }
  return count;
}

// Encodes the Parquet file footer for all written row groups.
std::string encode_footer(const ParquetNode &root,
                          const std::vector<LeafColumn> &columns,
                          const std::vector<RowGroupInfo> &row_groups,
                          std::int64_t total_rows, CompressionCodec codec) {
  std::string out;
  CompactWriter writer(out);
  writer.FieldI32(1, 1);
  writer.FieldListStruct(2, schema_element_count(root), [&](CompactWriter &w) {
    write_schema_elements(w, out, root);
  });
  writer.FieldI64(3, total_rows);
  writer.FieldListStruct(4, row_groups.size(), [&](CompactWriter &) {
    for (const auto &row_group : row_groups) {
      CompactWriter rg(out);
      rg.FieldListStruct(1, row_group.columns.size(), [&](CompactWriter &) {
        for (std::size_t i = 0; i < row_group.columns.size(); ++i) {
          const auto &chunk_info = row_group.columns[i];
          const auto &page = chunk_info.aggregate;
          const auto total_uncompressed_size =
              page.dictionary_header_size +
              page.dictionary_uncompressed_payload_size + page.header_size +
              page.uncompressed_payload_size;
          const auto total_compressed_size =
              page.dictionary_header_size +
              page.dictionary_compressed_payload_size + page.header_size +
              page.compressed_payload_size;
          CompactWriter chunk(out);
          chunk.FieldI64(2, page.first_page_offset);
          chunk.FieldStruct(3, [&](CompactWriter &meta) {
            meta.FieldI32(
                1, static_cast<std::int32_t>(columns[i].node->physical_type));
            std::vector<std::int32_t> encodings;
            if (page.has_plain_encoding || page.dictionary_encoded) {
              encodings.push_back(kEncodingPlain);
            }
            encodings.push_back(3);
            if (page.dictionary_encoded) {
              encodings.push_back(kEncodingRleDictionary);
            }
            if (page.has_delta_binary_packed_encoding) {
              encodings.push_back(kEncodingDeltaBinaryPacked);
            }
            if (page.has_delta_length_byte_array_encoding) {
              encodings.push_back(kEncodingDeltaLengthByteArray);
            }
            if (page.has_byte_stream_split_encoding) {
              encodings.push_back(kEncodingByteStreamSplit);
            }
            meta.FieldListI32(2, encodings);
            meta.FieldListString(3, columns[i].path);
            meta.FieldI32(4, static_cast<std::int32_t>(codec));
            meta.FieldI64(5, page.num_values);
            meta.FieldI64(6, total_uncompressed_size);
            meta.FieldI64(7, total_compressed_size);
            meta.FieldI64(9, page.offset);
            if (page.dictionary_page_offset >= 0) {
              meta.FieldI64(11, page.dictionary_page_offset);
            }
            meta.FieldStruct(12, [&](CompactWriter &statistics) {
              if (page.max_value && page.min_value) {
                statistics.FieldString(1, *page.max_value);
                statistics.FieldString(2, *page.min_value);
              }
              statistics.FieldI64(3, page.null_count);
              if (page.max_value && page.min_value) {
                statistics.FieldString(5, *page.max_value);
                statistics.FieldString(6, *page.min_value);
                statistics.FieldBool(7, true);
                statistics.FieldBool(8, true);
              }
            });
          });
          if (chunk_info.offset_index_offset >= 0) {
            chunk.FieldI64(4, chunk_info.offset_index_offset);
            chunk.FieldI32(5, chunk_info.offset_index_length);
          }
          if (chunk_info.column_index_offset >= 0) {
            chunk.FieldI64(6, chunk_info.column_index_offset);
            chunk.FieldI32(7, chunk_info.column_index_length);
          }
          chunk.Stop();
        }
      });
      std::int64_t row_group_size = 0;
      for (const auto &chunk_info : row_group.columns) {
        const auto &page = chunk_info.aggregate;
        row_group_size += page.dictionary_header_size +
                          page.dictionary_uncompressed_payload_size +
                          page.header_size + page.uncompressed_payload_size;
      }
      rg.FieldI64(2, row_group_size);
      rg.FieldI64(3, row_group.num_rows);
      rg.Stop();
    }
  });
  writer.FieldString(6, "schema-sanitizer native parquet writer");
  writer.Stop();
  return out;
}

// Returns the Arrow stream error string when available.
std::string stream_error_message(ArrowArrayStream *stream,
                                 std::string_view fallback) {
  if (stream && stream->get_last_error) {
    if (const char *message = stream->get_last_error(stream)) {
      if (*message) {
        return std::string(message);
      }
    }
  }
  return std::string(fallback);
}

} // namespace

sanitize::Status write_stream(ArrowArrayStream *stream, Output &out_file) {
  if (!stream) {
    return sanitize::Status::Invalid(
        "native Parquet writer: Arrow C stream is null");
  }

  sanitize::CSchemaGuard schema;
  const int schema_rc = stream->get_schema(stream, schema.get());
  if (schema_rc != 0) {
    return sanitize::Status::IOError(stream_error_message(
        stream, "native Parquet writer: get_schema failed"));
  }
  std::vector<LeafColumn> columns;
  SAN_ASSIGN_OR_RAISE(auto root, parse_supported_root_schema(schema.value()));
  std::size_t next_leaf_index = 0;
  assign_leaf_indexes(&root, &next_leaf_index);
  assign_repetition_levels(&root, 0, 0);
  collect_leaf_columns(root, {}, 0, 0, &columns);

  CountingOutput out(out_file);
  SAN_RETURN_NOT_OK(out.Write(kMagic));

  std::vector<RowGroupInfo> row_groups;
  std::int64_t total_rows = 0;
  const auto max_rows_per_row_group = configured_max_rows_per_row_group();
  const auto max_bytes_per_row_group = adaptive_max_bytes_per_row_group(
      columns, configured_max_bytes_per_row_group());
  const auto target_page_bytes = configured_target_page_bytes();
  SAN_ASSIGN_OR_RAISE(const auto compression_codec,
                      configured_compression_codec());
  while (true) {
    sanitize::CArrayGuard batch;
    const int next_rc = stream->get_next(stream, batch.get());
    if (next_rc != 0) {
      return sanitize::Status::IOError(stream_error_message(
          stream, "native Parquet writer: get_next failed"));
    }
    if (!batch.value().release) {
      break;
    }
    SAN_RETURN_NOT_OK(validate_batch(root, batch.value()));
    if (batch.value().length == 0) {
      continue;
    }
    for (std::int64_t row_offset = 0; row_offset < batch.value().length;) {
      auto row_count =
          std::min(max_rows_per_row_group, batch.value().length - row_offset);
      SAN_ASSIGN_OR_RAISE(auto page_data,
                          collect_batch_pages(root, columns, batch.value(),
                                              row_offset, row_count));
      while (row_count > 1 &&
             static_cast<std::int64_t>(estimate_row_group_page_data_bytes(
                 page_data)) > max_bytes_per_row_group) {
        row_count = std::max<std::int64_t>(1, row_count / 2);
        SAN_ASSIGN_OR_RAISE(page_data,
                            collect_batch_pages(root, columns, batch.value(),
                                                row_offset, row_count));
      }
      RowGroupInfo row_group;
      row_group.num_rows = row_count;
      row_group.columns.reserve(columns.size());
      for (std::size_t i = 0; i < columns.size(); ++i) {
        SAN_ASSIGN_OR_RAISE(auto page,
                            write_column_pages(out, columns[i], page_data[i],
                                               compression_codec,
                                               target_page_bytes));
        row_group.columns.push_back(page);
      }
      total_rows += row_count;
      row_groups.push_back(std::move(row_group));
      row_offset += row_count;
    }
  }

  SAN_RETURN_NOT_OK(write_page_indexes(out, columns, &row_groups));
  std::string footer =
      encode_footer(root, columns, row_groups, total_rows, compression_codec);
  SAN_RETURN_NOT_OK(out.Write(footer));
  if (footer.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: footer is too large");
  }
  std::string trailer;
  append_u32_le(trailer, static_cast<std::uint32_t>(footer.size()));
  trailer.append(kMagic);
  SAN_RETURN_NOT_OK(out.Write(trailer));
  return out.Flush();
}

} // namespace sanitize::internal::parquet_stream_writer

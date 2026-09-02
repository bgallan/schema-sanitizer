// Parses scalar and dispatches nested Arrow C schema nodes. These routines keep
// Arrow schema interpretation and buffer ownership explicit at the ABI
// boundary.

#include "api/python_abi3/arrow_direct/schema/parser_internal.hh"

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_formatters.hh"

#include <optional>
#include <string_view>
#include <utility>

namespace core_abi3_internal::arrow_schema_internal {
namespace {

/// Maps an Arrow integer format to the native signedness and width
/// classification.
std::optional<ArrowStorageKind> integer_storage_kind(std::string_view format) {
  if (format == "c") {
    return ArrowStorageKind::kInt8;
  }
  if (format == "C") {
    return ArrowStorageKind::kUInt8;
  }
  if (format == "s") {
    return ArrowStorageKind::kInt16;
  }
  if (format == "S") {
    return ArrowStorageKind::kUInt16;
  }
  if (format == "i") {
    return ArrowStorageKind::kInt32;
  }
  if (format == "I") {
    return ArrowStorageKind::kUInt32;
  }
  if (format == "l") {
    return ArrowStorageKind::kInt64;
  }
  return std::nullopt;
}

} // namespace

/// Maps an Arrow C Data format into logical type and materialization-node
/// metadata.
sanitize::Result<sanitize::LogicalType>
parse_arrow_type(const ArrowSchema *schema, ArrowInputNode *node,
                 const ArrowDirectOptions &options) {
  if (!schema || !schema->format || !node) {
    return sanitize::Status::Invalid("Arrow direct input has invalid schema");
  }
  const std::string_view format(schema->format);
  node->storage_kind = ArrowStorageKind::kNone;
  node->timestamp_target_units_per_second =
      timestamp_target_units_per_second(options.timestamp_precision);

  if (schema->dictionary) {
    auto storage_kind = integer_storage_kind(format);
    if (!storage_kind) {
      return sanitize::Status::Invalid(
          "unsupported Arrow dictionary index format: ", format);
    }
    node->kind = ArrowNodeKind::kDictionary;
    node->storage_kind = *storage_kind;
    node->children.clear();
    node->children.reserve(1);
    ArrowInputNode value_node;
    value_node.name = "dictionary";
    SAN_ASSIGN_OR_RAISE(
        auto value_type,
        parse_arrow_type(schema->dictionary, &value_node, options));
    value_node.logical_type = value_type;
    node->children.push_back(std::move(value_node));
    return value_type;
  }

  if (format == "n") {
    node->kind = ArrowNodeKind::kNull;
    return sanitize::LogicalType(sanitize::LogicalKind::kNull);
  }
  if (format == "b") {
    node->kind = ArrowNodeKind::kBool;
    return sanitize::LogicalType::Bool();
  }
  if (auto storage_kind = integer_storage_kind(format)) {
    node->kind = ArrowNodeKind::kInt;
    node->storage_kind = *storage_kind;
    return sanitize::LogicalType::Int64();
  }
  if (format == "L") {
    node->kind = ArrowNodeKind::kUInt64Text;
    node->storage_kind = ArrowStorageKind::kUInt64;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "f") {
    node->kind = ArrowNodeKind::kFloat;
    node->storage_kind = ArrowStorageKind::kFloat32;
    return sanitize::LogicalType::Float64();
  }
  if (format == "g") {
    node->kind = ArrowNodeKind::kFloat;
    node->storage_kind = ArrowStorageKind::kFloat64;
    return sanitize::LogicalType::Float64();
  }
  if (format == "u" || format == "U") {
    node->kind = ArrowNodeKind::kUtf8;
    node->storage_kind = format == "U" ? ArrowStorageKind::kOffset64
                                       : ArrowStorageKind::kOffset32;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "z" || format == "Z") {
    node->kind = ArrowNodeKind::kBinaryBase64;
    node->storage_kind = format == "Z" ? ArrowStorageKind::kOffset64
                                       : ArrowStorageKind::kOffset32;
    return sanitize::LogicalType::Utf8();
  }
  if (format.starts_with("d:") && parse_decimal_format(format, node)) {
    node->kind = ArrowNodeKind::kDecimalText;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tdD") {
    node->kind = ArrowNodeKind::kDate32;
    node->storage_kind = ArrowStorageKind::kInt32;
    return sanitize::LogicalType::Date32();
  }
  if (format == "tdm") {
    node->kind = ArrowNodeKind::kDate64;
    node->storage_kind = ArrowStorageKind::kInt64;
    return sanitize::LogicalType::Date32();
  }
  if (format == "tts") {
    node->kind = ArrowNodeKind::kTime32s;
    node->storage_kind = ArrowStorageKind::kInt32;
    return sanitize::LogicalType::Time32s();
  }
  if (format == "ttm") {
    node->kind = ArrowNodeKind::kTimeText;
    node->storage_kind = ArrowStorageKind::kTimeMilliseconds;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "ttu") {
    node->kind = ArrowNodeKind::kTimeText;
    node->storage_kind = ArrowStorageKind::kTimeMicroseconds;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "ttn") {
    node->kind = ArrowNodeKind::kTimeText;
    node->storage_kind = ArrowStorageKind::kTimeNanoseconds;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tDs") {
    node->kind = ArrowNodeKind::kDurationText;
    node->storage_kind = ArrowStorageKind::kDurationSeconds;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tDm") {
    node->kind = ArrowNodeKind::kDurationText;
    node->storage_kind = ArrowStorageKind::kDurationMilliseconds;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tDu") {
    node->kind = ArrowNodeKind::kDurationText;
    node->storage_kind = ArrowStorageKind::kDurationMicroseconds;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tDn") {
    node->kind = ArrowNodeKind::kDurationText;
    node->storage_kind = ArrowStorageKind::kDurationNanoseconds;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tiM") {
    node->kind = ArrowNodeKind::kIntervalText;
    node->storage_kind = ArrowStorageKind::kIntervalMonths;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tiD") {
    node->kind = ArrowNodeKind::kIntervalText;
    node->storage_kind = ArrowStorageKind::kIntervalDayTime;
    return sanitize::LogicalType::Utf8();
  }
  if (format == "tin") {
    node->kind = ArrowNodeKind::kIntervalText;
    node->storage_kind = ArrowStorageKind::kIntervalMonthDayNano;
    return sanitize::LogicalType::Utf8();
  }
  if (format.starts_with("ts")) {
    node->kind = ArrowNodeKind::kTimestamp;
    node->storage_kind = ArrowStorageKind::kInt64;
    node->timestamp_source_units_per_second =
        timestamp_source_units_per_second(format);
    return sanitize::LogicalType::TimestampNs();
  }
  if (format == "+s") {
    return parse_struct_type(schema, node, options);
  }
  if (format == "+l") {
    return parse_list_type(schema, node, ArrowNodeKind::kList, options);
  }
  if (format == "+L") {
    return parse_list_type(schema, node, ArrowNodeKind::kLargeList, options);
  }
  if (format.starts_with("+w:")) {
    return parse_fixed_size_list_type(schema, node, options);
  }
  if (format == "+m") {
    return parse_map_type(schema, node, options);
  }
  return sanitize::Status::Invalid("unsupported Arrow direct field format: ",
                                   format);
}

} // namespace core_abi3_internal::arrow_schema_internal

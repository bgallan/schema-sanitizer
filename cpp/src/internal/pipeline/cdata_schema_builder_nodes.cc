// Builds nested Arrow C Data schema nodes from logical schema types.

#include "internal/pipeline/cdata_schema_builder_internal.hh"

#include <cstring>
#include <string_view>

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal::cdata_schema_builder {
namespace {

// Converts timestamp precision to an Arrow C Data format string.
sanitize::Result<std::string_view>
timestamp_format(std::string_view precision) {
  if (precision == "TIMESTAMP_MILLIS") {
    return "tsm:";
  }
  if (precision == "TIMESTAMP_NANOS") {
    return "tsn:";
  }
  if (precision == "TIMESTAMP_MICROS") {
    return "tsu:";
  }
  return sanitize::Status::Invalid("unsupported timestamp_precision");
}

// Converts a logical type to an Arrow C Data format string.
sanitize::Result<std::string_view>
logical_type_to_c_schema_format(const sanitize::LogicalType &t,
                                std::string_view timestamp_precision) {
  switch (t.kind) {
  case LogicalKind::kNull:
    return "n";
  case LogicalKind::kBool:
    return "b";
  case LogicalKind::kInt64:
    return "l";
  case LogicalKind::kFloat64:
    return "g";
  case LogicalKind::kUtf8:
    return "u";
  case LogicalKind::kTimestampNs:
    return timestamp_format(timestamp_precision);
  case LogicalKind::kDate32:
    return "tdD";
  case LogicalKind::kTime32s:
    return "tts";
  case LogicalKind::kStruct:
    return "+s";
  case LogicalKind::kList:
    return "+l";
  }
  return sanitize::Status::Invalid(
      "logical_type_to_c_schema_format: unsupported logical kind");
}

// Builds child ArrowSchema nodes for nested logical types.
sanitize::Status
build_schema_node_children(const sanitize::LogicalType &logical_type,
                           std::string_view timestamp_precision,
                           ArrowSchema *out) {
  if (!out) {
    return sanitize::Status::Invalid(
        "build_schema_node_children: output schema is null");
  }

  switch (logical_type.kind) {
  case LogicalKind::kStruct: {
    SAN_RETURN_NOT_OK(allocate_child_slots(out, logical_type.fields.size(),
                                           "build_schema_node_children"));
    for (std::size_t i = 0; i < logical_type.fields.size(); ++i) {
      const auto &child = logical_type.fields[i];
      SAN_ASSIGN_OR_RAISE(
          auto *child_schema,
          append_child_schema(out, i, "build_schema_node_children"));
      const sanitize::LogicalType child_type =
          child.type ? *child.type : sanitize::LogicalType(LogicalKind::kNull);
      auto st = build_schema_node(child.name, child.nullable, child_type, "",
                                  timestamp_precision, child_schema);
      if (!st.ok()) {
        return st;
      }
    }
    return sanitize::Status::OK();
  }
  case LogicalKind::kList: {
    SAN_RETURN_NOT_OK(
        allocate_child_slots(out, 1, "build_schema_node_children"));
    SAN_ASSIGN_OR_RAISE(
        auto *child_schema,
        append_child_schema(out, 0, "build_schema_node_children"));
    const sanitize::LogicalType child_type =
        logical_type.value ? *logical_type.value
                           : sanitize::LogicalType::Utf8();
    return build_schema_node("item", true, child_type, "", timestamp_precision,
                             child_schema);
  }
  default:
    out->n_children = 0;
    out->children = nullptr;
    return sanitize::Status::OK();
  }
}

} // namespace

sanitize::Status build_schema_node(std::string name, bool nullable,
                                   const sanitize::LogicalType &logical_type,
                                   std::string_view format_override,
                                   std::string_view timestamp_precision,
                                   ArrowSchema *out) {
  if (!out) {
    return sanitize::Status::Invalid(
        "build_schema_node: output schema is null");
  }
  std::memset(out, 0, sizeof(*out));

  std::string format;
  if (!format_override.empty()) {
    format.assign(format_override.data(), format_override.size());
  } else {
    std::string_view format_view;
    SAN_ASSIGN_OR_RAISE(format_view, logical_type_to_c_schema_format(
                                         logical_type, timestamp_precision));
    format.assign(format_view.data(), format_view.size());
  }

  SAN_RETURN_NOT_OK(assign_schema_state(out, std::move(name), std::move(format),
                                        "build_schema_node"));
  out->metadata = nullptr;
  out->flags = nullable ? ARROW_FLAG_NULLABLE : 0;
  out->dictionary = nullptr;
  out->release = &exported_schema_release;

  auto child_st =
      build_schema_node_children(logical_type, timestamp_precision, out);
  if (!child_st.ok()) {
    if (out->release) {
      out->release(out);
    }
    return child_st;
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal::cdata_schema_builder

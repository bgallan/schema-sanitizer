// Builds Arrow C Data schemas from logical schema descriptions.
// The implementation preserves Arrow ownership and error contracts without
// depending on the Arrow C++ library.

#include "internal/arrow_c/cdata_schema_builder.hh"
#include "internal/arrow_c/cdata_schema_builder_internal.hh"

#include <cstring>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal {

namespace cdata_schema_builder {

sanitize::Status assign_schema_state(ArrowSchema *out, std::string name,
                                     std::string format, const char *context) {
  auto *state = new (std::nothrow) ExportedSchemaState();
  if (!state) {
    return sanitize::Status::OutOfMemory(context, ": OOM state");
  }
  try {
    state->name = std::move(name);
    state->format = std::move(format);
  } catch (const std::bad_alloc &) {
    delete state;
    return sanitize::Status::OutOfMemory(context, ": OOM state data");
  }

  out->format = state->format.c_str();
  out->name = state->name.c_str();
  out->private_data = state;
  return sanitize::Status::OK();
}

void exported_schema_release(ArrowSchema *schema) {
  if (!schema || !schema->release) {
    return;
  }
  if (schema->children) {
    for (int64_t i = 0; i < schema->n_children; ++i) {
      ArrowSchema *child = schema->children[i];
      if (!child) {
        continue;
      }
      if (child->release) {
        child->release(child);
      }
      delete child;
      schema->children[i] = nullptr;
    }
    delete[] schema->children;
    schema->children = nullptr;
  }
  delete static_cast<ExportedSchemaState *>(schema->private_data);
  schema->format = nullptr;
  schema->name = nullptr;
  schema->metadata = nullptr;
  schema->flags = 0;
  schema->n_children = 0;
  schema->children = nullptr;
  schema->dictionary = nullptr;
  schema->private_data = nullptr;
  schema->release = nullptr;
}

sanitize::Status allocate_child_slots(ArrowSchema *out, std::size_t child_count,
                                      const char *context) {
  out->n_children = 0;
  out->children = nullptr;
  if (child_count == 0) {
    return sanitize::Status::OK();
  }
  out->children = new (std::nothrow) ArrowSchema *[child_count]();
  if (!out->children) {
    return sanitize::Status::OutOfMemory(context, ": OOM children");
  }
  return sanitize::Status::OK();
}

sanitize::Result<ArrowSchema *>
append_child_schema(ArrowSchema *out, std::size_t index, const char *context) {
  auto *child = new (std::nothrow) ArrowSchema();
  if (!child) {
    return sanitize::Status::OutOfMemory(context, ": OOM child schema");
  }
  out->children[index] = child;
  out->n_children = static_cast<int64_t>(index + 1);
  return child;
}

} // namespace cdata_schema_builder

sanitize::Status
export_fields_as_struct_schema(const std::vector<CDataFieldLayout> &fields,
                               ArrowSchema *out,
                               std::string_view timestamp_precision) {
  if (!out) {
    return sanitize::Status::Invalid(
        "export_fields_as_struct_schema: out is null");
  }
  std::memset(out, 0, sizeof(*out));
  SAN_RETURN_NOT_OK(cdata_schema_builder::assign_schema_state(
      out, "", "+s", "export_fields_as_struct_schema"));
  out->metadata = nullptr;
  out->flags = 0;
  out->n_children = 0;
  out->children = nullptr;
  out->dictionary = nullptr;
  out->release = &cdata_schema_builder::exported_schema_release;

  if (fields.empty()) {
    return sanitize::Status::OK();
  }

  auto children_st = cdata_schema_builder::allocate_child_slots(
      out, fields.size(), "export_fields_as_struct_schema");
  if (!children_st.ok()) {
    out->release(out);
    return children_st;
  }

  for (std::size_t i = 0; i < fields.size(); ++i) {
    const auto &field = fields[i];
    auto child_result = cdata_schema_builder::append_child_schema(
        out, i, "export_fields_as_struct_schema");
    if (!child_result.ok()) {
      out->release(out);
      return child_result.status();
    }
    ArrowSchema *child = *child_result;
    auto st = cdata_schema_builder::build_schema_node(
        field.name, field.nullable, field.logical_type, field.format_override,
        timestamp_precision, child);
    if (!st.ok()) {
      out->release(out);
      return st;
    }
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal

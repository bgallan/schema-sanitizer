// Private helpers for Arrow C Data schema export.

#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "internal/pipeline/cdata_schema_builder.hh"
#include "sanitize/core/status.hh"

struct ArrowSchema;

namespace sanitize::internal::cdata_schema_builder {

struct ExportedSchemaState {
  std::string format;
  std::string name;
};

// Owns field name/format strings for one ArrowSchema node.
sanitize::Status assign_schema_state(ArrowSchema *out, std::string name,
                                     std::string format, const char *context);

// Releases an exported ArrowSchema tree and its owned metadata.
void exported_schema_release(ArrowSchema *schema);

// Allocates a nullable child-pointer array for an ArrowSchema.
sanitize::Status allocate_child_slots(ArrowSchema *out, std::size_t child_count,
                                      const char *context);

// Allocates and registers one child schema slot.
sanitize::Result<ArrowSchema *>
append_child_schema(ArrowSchema *out, std::size_t index, const char *context);

// Builds one ArrowSchema node for a logical field.
sanitize::Status build_schema_node(std::string name, bool nullable,
                                   const sanitize::LogicalType &logical_type,
                                   std::string_view format_override,
                                   std::string_view timestamp_precision,
                                   ArrowSchema *out);

} // namespace sanitize::internal::cdata_schema_builder

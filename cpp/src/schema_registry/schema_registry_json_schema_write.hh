// Declares canonical schema JSON serialization for schema registries.

#pragma once

#include <string>

#include "sanitize/core/logical_schema.hh"

namespace sanitize::schema_registry_internal {

// Appends the canonical_schema object for a logical schema.
void append_canonical_schema_json(std::string &out,
                                  const LogicalSchema &schema);

} // namespace sanitize::schema_registry_internal

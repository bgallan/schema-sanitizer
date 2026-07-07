// Maps diagnostic codes to stable text identifiers.

#include "sanitize/core/diagnostics.hh"

#include <string>

#include "internal/json/json_write.hh"

namespace sanitize {

std::string IngestDiagnostics::to_json() const {
  std::string out;
  out.reserve(512);

  out.push_back('{');
  bool first = true;

  internal::json_write::append_int_field(out, first,
                                         "diagnostics_schema_version", 1);

  // Core counters
  internal::json_write::append_int_field(out, first, "inferred_rows",
                                         inferred_rows);
  internal::json_write::append_int_field(out, first, "inferred_bytes",
                                         inferred_bytes);
  internal::json_write::append_int_field(out, first, "arrow_schema_depth",
                                         arrow_schema_depth);
  internal::json_write::append_int_field(out, first, "parquet_schema_depth",
                                         parquet_schema_depth);
  internal::json_write::append_int_field(out, first, "materialized_rows",
                                         materialized_rows);
  internal::json_write::append_int_field(out, first, "batches", batches);

  // Feature counters
  internal::json_write::append_int_field(out, first, "flattened_fields",
                                         flattened_fields);
  internal::json_write::append_int_field(out, first, "scalar_wrappings",
                                         scalar_wrappings);
  internal::json_write::append_int_field(out, first, "direct_arrow_input",
                                         direct_arrow_input);

  // Error handling
  internal::json_write::append_int_field(out, first, "skipped_rows",
                                         skipped_rows);

  out.push_back('}');
  return out;
}

} // namespace sanitize

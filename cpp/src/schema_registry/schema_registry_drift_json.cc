// Implements JSON serialization for schema-registry drift events.

#include "schema_registry/schema_registry_internal.hh"

#include "internal/json_encoding/token_writer.hh"

#include <string>
#include <string_view>
#include <vector>

namespace sanitize::schema_registry_internal {
namespace {

/// Appends one schema drift audit event object.
void append_drift_event(std::string &out, const DriftEvent &event,
                        std::string_view detected_at) {
  out.push_back('{');
  bool first = true;
  internal::json_encoding::append_string_field(out, first, "detected_at",
                                               detected_at);
  internal::json_encoding::append_string_field(out, first, "source_path",
                                               event.source_path);
  internal::json_encoding::append_string_field(out, first, "output_name",
                                               event.output_name);
  internal::json_encoding::append_string_field(out, first, "drift_type",
                                               event.drift_type);
  internal::json_encoding::append_key(out, first, "previous_schema");
  if (event.previous_schema) {
    internal::json_encoding::append_string(out, *event.previous_schema);
  } else {
    out += "null";
  }
  internal::json_encoding::append_string_field(out, first, "new_schema",
                                               event.new_schema);
  out.push_back('}');
}

} // namespace

std::string drift_events_json(const std::vector<DriftEvent> &events,
                              std::string_view detected_at) {
  std::string out;
  out.push_back('[');
  for (std::size_t i = 0; i < events.size(); ++i) {
    if (i != 0)
      out.push_back(',');
    append_drift_event(out, events[i], detected_at);
  }
  out.push_back(']');
  return out;
}

} // namespace sanitize::schema_registry_internal

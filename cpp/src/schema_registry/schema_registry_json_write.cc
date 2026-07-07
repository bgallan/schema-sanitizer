// Implements JSON serialization for schema-registry documents.

#include "schema_registry/schema_registry_internal.hh"

#include "internal/json/json_write.hh"
#include "schema_registry/schema_registry_json_schema_write.hh"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize::schema_registry_internal {
namespace {

struct RegistryVariant {
  std::string source_path;
  std::string output_name;
  std::string schema;
  TopLevelKind top_level_kind = TopLevelKind::kScalar;
};

/// Parses the previous registry generation counter, defaulting to generation 1.
int64_t parse_schema_generation(std::string_view registry_json) {
  constexpr std::string_view kKey = "\"schema_generation\"";
  const std::size_t key_pos = registry_json.find(kKey);
  if (key_pos == std::string_view::npos)
    return 1;
  std::size_t pos = registry_json.find(':', key_pos + kKey.size());
  if (pos == std::string_view::npos)
    return 1;
  ++pos;
  while (pos < registry_json.size() &&
         std::isspace(static_cast<unsigned char>(registry_json[pos]))) {
    ++pos;
  }
  int64_t value = 1;
  const char *begin = registry_json.data() + pos;
  const char *end = registry_json.data() + registry_json.size();
  auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc() || ptr == begin || value < 1)
    return 1;
  return value;
}

/// Appends one schema drift audit event object.
void append_drift_event(std::string &out, const DriftEvent &event,
                        std::string_view detected_at) {
  out.push_back('{');
  bool first = true;
  internal::json_write::append_string_field(out, first, "detected_at",
                                            detected_at);
  internal::json_write::append_string_field(out, first, "source_path",
                                            event.source_path);
  internal::json_write::append_string_field(out, first, "output_name",
                                            event.output_name);
  internal::json_write::append_string_field(out, first, "drift_type",
                                            event.drift_type);
  internal::json_write::append_key(out, first, "previous_schema");
  if (event.previous_schema) {
    internal::json_write::append_string(out, *event.previous_schema);
  } else {
    out += "null";
  }
  internal::json_write::append_string_field(out, first, "new_schema",
                                            event.new_schema);
  out.push_back('}');
}

/// Appends one version record for a source-path variant family.
void append_registry_version(std::string &out, std::string_view output_name,
                             std::string_view schema,
                             bool is_most_compatible_current_version) {
  out.push_back('{');
  bool first = true;
  internal::json_write::append_string_field(out, first, "output_name",
                                            output_name);
  internal::json_write::append_string_field(out, first, "schema", schema);
  internal::json_write::append_key(out, first,
                                   "is_most_compatible_current_version");
  out += is_most_compatible_current_version ? "true" : "false";
  out.push_back('}');
}

/// Collects all variant records from a nested logical schema.
void collect_registry_variants(std::vector<RegistryVariant> &records,
                               const std::vector<LogicalField> &fields,
                               std::string_view source_parent) {
  for (const auto &field : fields) {
    const std::string source_segment = source_segment_for_output(field.name);
    const std::string source_path = join_path(source_parent, source_segment);

    records.push_back(RegistryVariant{
        .source_path = source_path,
        .output_name = field.name,
        .schema = field_type_string(field),
        .top_level_kind =
            field.type ? top_level_kind(*field.type) : TopLevelKind::kScalar});

    if (!field.type)
      continue;
    if (field.type->kind == LogicalKind::kStruct) {
      collect_registry_variants(records, field.type->fields, source_path);
    } else if (field.type->kind == LogicalKind::kList && field.type->value &&
               field.type->value->kind == LogicalKind::kStruct) {
      collect_registry_variants(records, field.type->value->fields,
                                source_path + "[]");
    }
  }
}

/// Appends grouped variant-family records keyed by source path.
void append_registry_variants_json(std::string &out,
                                   std::vector<RegistryVariant> records) {
  std::stable_sort(
      records.begin(), records.end(),
      [](const RegistryVariant &left, const RegistryVariant &right) {
        return left.source_path < right.source_path;
      });

  bool first_path = true;
  for (std::size_t i = 0; i < records.size();) {
    const std::string &source_path = records[i].source_path;
    if (!first_path)
      out.push_back(',');
    first_path = false;
    internal::json_write::append_string(out, source_path);
    out += ":{\"versions\":[";

    std::size_t group_end = i;
    while (group_end < records.size() &&
           records[group_end].source_path == source_path) {
      ++group_end;
    }

    std::size_t current_index = i;
    auto compatibility_rank = [](TopLevelKind kind) -> int {
      switch (kind) {
      case TopLevelKind::kList:
        return 3;
      case TopLevelKind::kStruct:
        return 2;
      case TopLevelKind::kScalar:
      default:
        return 1;
      }
    };
    for (std::size_t candidate = i + 1; candidate < group_end; ++candidate) {
      if (compatibility_rank(records[candidate].top_level_kind) >=
          compatibility_rank(records[current_index].top_level_kind)) {
        current_index = candidate;
      }
    }

    bool first_version = true;
    while (i < group_end) {
      if (!first_version)
        out.push_back(',');
      first_version = false;
      append_registry_version(out, records[i].output_name, records[i].schema,
                              i == current_index);
      ++i;
    }

    out += "]}";
  }
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

std::string registry_json(const LogicalSchema &schema,
                          std::string_view previous_registry_json,
                          std::string_view field_name_policy, bool has_drifts) {
  const int64_t generation =
      parse_schema_generation(previous_registry_json) + (has_drifts ? 1 : 0);

  std::string out;
  out.push_back('{');
  bool first = true;
  internal::json_write::append_string_field(out, first, "field_name_policy",
                                            field_name_policy);
  internal::json_write::append_int_field(out, first, "registry_version", 1);
  internal::json_write::append_int_field(out, first, "schema_generation",
                                         generation);
  internal::json_write::append_key(out, first, "canonical_schema");
  append_canonical_schema_json(out, schema);
  internal::json_write::append_key(out, first, "variants");
  out.push_back('{');
  std::vector<RegistryVariant> records;
  collect_registry_variants(records, schema.fields, "");
  append_registry_variants_json(out, std::move(records));
  out.push_back('}');
  out.push_back('}');
  return out;
}

} // namespace sanitize::schema_registry_internal

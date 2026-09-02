// Implements recursive native schema-registry merge and variant evolution.
// Field-family indexes preserve historical shapes while deterministic
// promotion, wrapping, and drift events reconcile new observations with
// canonical state.

#include "sanitize/schema_registry/schema_registry.hh"

#include "internal/planning/schema_evolution.hh"
#include "internal/planning/variant_field_names.hh"
#include "internal/string_lookup.hh"
#include "sanitize/metadata/file_metadata.hh"
#include "schema_registry/schema_registry_internal.hh"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize {
namespace {
using schema_registry_internal::DriftEvent;

using internal::BorrowedStringLookupMap;

struct VariantFamilyIndex {
  std::vector<std::size_t> positions;
  int next_version = 2;
};

struct FieldMergeIndex {
  BorrowedStringLookupMap<std::size_t> by_name;
  BorrowedStringLookupMap<VariantFamilyIndex> families;
};

/// Registers an exact field name and its semantic version family position.
void register_field(FieldMergeIndex &index, const LogicalField &field,
                    std::size_t position) {
  index.by_name.insert_or_assign(std::string_view(field.name), position);
  const auto parsed = internal::parse_versioned_field_name(field.name);
  const std::string_view family_base = parsed ? parsed->base : field.name;
  auto [family_it, _] = index.families.try_emplace(family_base);
  family_it->second.positions.push_back(position);
  if (parsed) {
    family_it->second.next_version =
        std::max(family_it->second.next_version, parsed->version + 1);
  }
}

/// Builds exact-name and version-family indexes for the current registry
/// fields.
FieldMergeIndex build_field_merge_index(const std::vector<LogicalField> &fields,
                                        std::size_t expected_size) {
  FieldMergeIndex index;
  index.by_name.reserve(expected_size);
  index.families.reserve(expected_size);
  for (std::size_t i = 0; i < fields.size(); ++i) {
    register_field(index, fields[i], i);
  }
  return index;
}

/// Returns the first unused semantic version name for an incoming field type.
std::string next_variant_name(const FieldMergeIndex &index,
                              std::string_view base_name,
                              const LogicalType &incoming_type) {
  int version = 2;
  if (auto it = index.families.find(base_name); it != index.families.end()) {
    version = it->second.next_version;
  }
  const std::string semantic_type =
      schema_registry_internal::variant_semantic_type(incoming_type);
  while (true) {
    std::string candidate =
        internal::make_versioned_field_name(base_name, version, semantic_type);
    if (!index.by_name.contains(std::string_view(candidate)))
      return candidate;
    ++version;
  }
}

/// Returns whether a logical type is neither a struct nor a list.
bool is_scalar_type(const LogicalType &type) noexcept {
  return type.kind != LogicalKind::kStruct && type.kind != LogicalKind::kList;
}

/// Finds a field position by exact output name.
std::optional<std::size_t>
find_field_index(const std::vector<LogicalField> &fields,
                 std::string_view name) noexcept {
  const auto field =
      std::ranges::find_if(fields, [name](const LogicalField &candidate) {
        return candidate.name == name;
      });
  if (field == fields.end())
    return std::nullopt;
  return static_cast<std::size_t>(std::distance(fields.begin(), field));
}

/// Creates a nullable owned field with a deep-copied logical type.
LogicalField make_nullable_field(std::string name, const LogicalType &type) {
  LogicalField field;
  field.name = std::move(name);
  field.nullable = true;
  field.type = std::make_unique<LogicalType>(type);
  return field;
}

/// Reconciles two logical types or reports that a new variant is required.
std::optional<LogicalType>
merge_types(const LogicalType &base_type, const LogicalType &incoming_type,
            std::string_view source_path, std::vector<DriftEvent> &drifts,
            std::string_view detected_at, std::string_view default_key_name);

/// Recursively merges into the newest compatible member of a version family.
bool merge_into_existing_family_variant(
    std::vector<LogicalField> &fields, const FieldMergeIndex &index,
    std::string_view base_name, std::optional<std::size_t> skipped_position,
    const LogicalType &incoming_type, std::string_view source_path,
    std::vector<DriftEvent> &drifts, std::string_view detected_at,
    std::string_view default_key_name) {
  auto family_it = index.families.find(base_name);
  if (family_it == index.families.end())
    return false;

  // Reprocessing must reuse an exact historical shape before considering a
  // newer family member that would need nested evolution.
  for (auto position_it = family_it->second.positions.rbegin();
       position_it != family_it->second.positions.rend(); ++position_it) {
    const std::size_t position = *position_it;
    if (position >= fields.size() ||
        (skipped_position && position == *skipped_position)) {
      continue;
    }
    const auto &candidate = fields[position];
    if (candidate.type && schema_registry_internal::logical_type_equal(
                              *candidate.type, incoming_type)) {
      return true;
    }
  }

  // The registry marks the newest equally compatible variant as current.
  // Probe in reverse order so incremental and past-date reprocessing use the
  // same deterministic target without cloning an already versioned ancestor.
  for (auto position_it = family_it->second.positions.rbegin();
       position_it != family_it->second.positions.rend(); ++position_it) {
    const std::size_t position = *position_it;
    if (position >= fields.size() ||
        (skipped_position && position == *skipped_position)) {
      continue;
    }
    auto &candidate = fields[position];
    if (!candidate.type)
      continue;

    std::vector<DriftEvent> candidate_drifts;
    auto merged = merge_types(*candidate.type, incoming_type, source_path,
                              candidate_drifts, detected_at, default_key_name);
    if (!merged)
      continue;

    if (!schema_registry_internal::logical_type_equal(*candidate.type,
                                                      *merged)) {
      if (candidate_drifts.empty()) {
        drifts.push_back(DriftEvent{
            .source_path = std::string(source_path),
            .output_name = candidate.name,
            .drift_type = "type_promoted",
            .previous_schema =
                schema_registry_internal::field_type_string(candidate),
            .new_schema =
                schema_registry_internal::logical_type_string(*merged)});
      }
      candidate.type = std::make_unique<LogicalType>(std::move(*merged));
    }
    drifts.insert(drifts.end(),
                  std::make_move_iterator(candidate_drifts.begin()),
                  std::make_move_iterator(candidate_drifts.end()));
    return true;
  }
  return false;
}

/// Merges a scalar observation into a struct's configured wrapper field.
std::optional<LogicalType> merge_struct_with_wrapped_scalar(
    const LogicalType &base_type, const LogicalType &incoming_type,
    std::string_view source_path, std::vector<DriftEvent> &drifts,
    std::string_view detected_at, std::string_view default_key_name) {
  if (base_type.kind != LogicalKind::kStruct || !is_scalar_type(incoming_type))
    return std::nullopt;

  LogicalType out = base_type;
  const std::string default_key(default_key_name.empty() ? "default_key"
                                                         : default_key_name);
  const std::string default_key_path =
      schema_registry_internal::join_path(source_path, default_key);
  auto field_index = find_field_index(out.fields, default_key);
  if (!field_index) {
    out.fields.push_back(make_nullable_field(default_key, incoming_type));
    drifts.push_back(DriftEvent{
        .source_path = default_key_path,
        .output_name = default_key,
        .drift_type = "newly_added",
        .previous_schema = std::nullopt,
        .new_schema =
            schema_registry_internal::field_type_string(out.fields.back())});
    return out;
  }

  LogicalField &field = out.fields[*field_index];
  if (!field.type) {
    field.type = std::make_unique<LogicalType>(incoming_type);
    return out;
  }
  const std::size_t drift_count_before_child = drifts.size();
  auto merged_child = merge_types(*field.type, incoming_type, default_key_path,
                                  drifts, detected_at, default_key_name);
  if (!merged_child)
    return std::nullopt;
  if (!schema_registry_internal::logical_type_equal(*field.type,
                                                    *merged_child)) {
    if (drifts.size() == drift_count_before_child) {
      drifts.push_back(DriftEvent{
          .source_path = default_key_path,
          .output_name = field.name,
          .drift_type = "type_promoted",
          .previous_schema = schema_registry_internal::field_type_string(field),
          .new_schema =
              schema_registry_internal::logical_type_string(*merged_child)});
    }
    field.type = std::make_unique<LogicalType>(std::move(*merged_child));
  }
  return out;
}

/// Merges incoming fields into canonical fields and records every durable
/// drift.
std::vector<LogicalField>
merge_fields(const std::vector<LogicalField> &base_fields,
             const std::vector<LogicalField> &incoming_fields,
             std::string_view parent_path, std::vector<DriftEvent> &drifts,
             std::string_view detected_at, std::string_view default_key_name) {
  std::vector<LogicalField> out = base_fields;
  const std::size_t maximum_field_count =
      base_fields.size() + incoming_fields.size();
  out.reserve(maximum_field_count);
  drifts.reserve(drifts.size() + incoming_fields.size());
  FieldMergeIndex index = build_field_merge_index(out, maximum_field_count);

  for (const auto &incoming : incoming_fields) {
    const std::string source_path = schema_registry_internal::join_path(
        parent_path,
        schema_registry_internal::source_segment_for_output(incoming.name));
    auto base_it = index.by_name.find(incoming.name);
    const std::optional<std::size_t> base_position =
        base_it == index.by_name.end()
            ? std::nullopt
            : std::optional<std::size_t>(base_it->second);
    LogicalField *base = base_position ? &out[*base_position] : nullptr;
    if (!base) {
      const std::string_view incoming_family_base =
          internal::versioned_field_base(incoming.name);
      if (!incoming_family_base.empty() && incoming.type &&
          index.families.contains(incoming_family_base)) {
        if (merge_into_existing_family_variant(
                out, index, incoming_family_base, std::nullopt, *incoming.type,
                source_path, drifts, detected_at, default_key_name)) {
          continue;
        }

        LogicalField variant = incoming;
        variant.name =
            next_variant_name(index, incoming_family_base, *incoming.type);
        variant.nullable = true;
        out.push_back(variant);
        register_field(index, out.back(), out.size() - 1);
        drifts.push_back(DriftEvent{
            .source_path = source_path,
            .output_name = out.back().name,
            .drift_type = "new_version_generated",
            .previous_schema = std::nullopt,
            .new_schema =
                schema_registry_internal::field_type_string(incoming)});
        continue;
      }

      out.push_back(incoming);
      register_field(index, out.back(), out.size() - 1);
      drifts.push_back(DriftEvent{
          .source_path = source_path,
          .output_name = incoming.name,
          .drift_type = "newly_added",
          .previous_schema = std::nullopt,
          .new_schema = schema_registry_internal::field_type_string(incoming)});
      continue;
    }

    if (!base->type || !incoming.type)
      continue;

    std::vector<DriftEvent> base_drifts;
    auto merged = merge_types(*base->type, *incoming.type, source_path,
                              base_drifts, detected_at, default_key_name);
    if (merged) {
      if (!schema_registry_internal::logical_type_equal(*base->type, *merged)) {
        if (base_drifts.empty()) {
          base_drifts.push_back(DriftEvent{
              .source_path = source_path,
              .output_name = base->name,
              .drift_type = "type_promoted",
              .previous_schema =
                  schema_registry_internal::field_type_string(*base),
              .new_schema =
                  schema_registry_internal::logical_type_string(*merged)});
        }
        base->type = std::make_unique<LogicalType>(std::move(*merged));
      }
      drifts.insert(drifts.end(), std::make_move_iterator(base_drifts.begin()),
                    std::make_move_iterator(base_drifts.end()));
      continue;
    }

    const std::string_view family_base =
        internal::variant_family_base(incoming.name);
    if (merge_into_existing_family_variant(
            out, index, family_base, base_position, *incoming.type, source_path,
            drifts, detected_at, default_key_name)) {
      continue;
    }

    const std::string previous_schema =
        schema_registry_internal::field_type_string(*base);
    LogicalField variant = incoming;
    variant.name = next_variant_name(index, family_base, *incoming.type);
    variant.nullable = true;
    out.push_back(variant);
    register_field(index, out.back(), out.size() - 1);
    drifts.push_back(DriftEvent{
        .source_path = source_path,
        .output_name = out.back().name,
        .drift_type = "new_version_generated",
        .previous_schema = previous_schema,
        .new_schema = schema_registry_internal::field_type_string(incoming)});
  }

  return out;
}

/// Recursively promotes compatible types while preserving incompatible
/// variants.
std::optional<LogicalType>
merge_types(const LogicalType &base_type, const LogicalType &incoming_type,
            std::string_view source_path, std::vector<DriftEvent> &drifts,
            std::string_view detected_at, std::string_view default_key_name) {
  if (is_scalar_type(base_type) && is_scalar_type(incoming_type)) {
    if (incoming_type.kind == LogicalKind::kNull ||
        base_type.kind == incoming_type.kind) {
      return base_type;
    }
    if (base_type.kind == LogicalKind::kFloat64 &&
        incoming_type.kind == LogicalKind::kInt64) {
      return base_type;
    }
    if (base_type.kind == LogicalKind::kInt64 &&
        incoming_type.kind == LogicalKind::kFloat64) {
      return incoming_type;
    }
    if (base_type.kind == LogicalKind::kNull)
      return incoming_type;
    return std::nullopt;
  }

  if (base_type.kind == LogicalKind::kStruct &&
      incoming_type.kind == LogicalKind::kStruct) {
    return LogicalType::Struct(
        merge_fields(base_type.fields, incoming_type.fields, source_path,
                     drifts, detected_at, default_key_name));
  }

  if (base_type.kind == LogicalKind::kList && base_type.value) {
    const LogicalType *incoming_element = &incoming_type;
    if (incoming_type.kind == LogicalKind::kList) {
      if (!incoming_type.value)
        return base_type;
      incoming_element = incoming_type.value.get();
    }
    auto merged_value = merge_types(*base_type.value, *incoming_element,
                                    std::string(source_path) + "[]", drifts,
                                    detected_at, default_key_name);
    if (!merged_value)
      return std::nullopt;
    return LogicalType::List(std::move(*merged_value));
  }

  if (base_type.kind == LogicalKind::kStruct && is_scalar_type(incoming_type)) {
    return merge_struct_with_wrapped_scalar(base_type, incoming_type,
                                            source_path, drifts, detected_at,
                                            default_key_name);
  }

  return std::nullopt;
}
} // namespace

namespace schema_registry_internal {

std::vector<LogicalField> merge_registry_fields(
    const std::vector<LogicalField> &base_fields,
    const std::vector<LogicalField> &incoming_fields,
    std::string_view parent_path, std::vector<DriftEvent> &drifts,
    std::string_view detected_at, std::string_view default_key_name) {
  return merge_fields(base_fields, incoming_fields, parent_path, drifts,
                      detected_at, default_key_name);
}

} // namespace schema_registry_internal

} // namespace sanitize

// Implements native schema-registry merge entry points.

#include "sanitize/schema_registry/schema_registry.hh"

#include "internal/planning/schema_evolution.hh"
#include "internal/planning/variant_field_names.hh"
#include "sanitize/metadata/file_metadata.hh"
#include "schema_registry/schema_registry_internal.hh"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace sanitize {
namespace {

using schema_registry_internal::DriftEvent;

struct TransparentStringHash {
  using is_transparent = void;

  // Hashes owned and borrowed string keys identically.
  [[nodiscard]] std::size_t operator()(std::string_view value) const noexcept {
    return std::hash<std::string_view>{}(value);
  }

  [[nodiscard]] std::size_t
  operator()(const std::string &value) const noexcept {
    return (*this)(std::string_view(value));
  }

  [[nodiscard]] std::size_t operator()(const char *value) const noexcept {
    return (*this)(std::string_view(value));
  }
};

struct TransparentStringEqual {
  using is_transparent = void;

  // Compares owned and borrowed string keys without materializing new strings.
  [[nodiscard]] bool operator()(std::string_view lhs,
                                std::string_view rhs) const noexcept {
    return lhs == rhs;
  }
};

template <typename T>
using StringLookupMap =
    std::unordered_map<std::string, T, TransparentStringHash,
                       TransparentStringEqual>;

struct FieldMergeIndex {
  StringLookupMap<std::size_t> by_name;
  StringLookupMap<std::vector<std::size_t>> variants_by_base;
  StringLookupMap<int> next_variant_version;
};

void register_field(FieldMergeIndex &index, const LogicalField &field,
                    std::size_t position) {
  index.by_name[field.name] = position;
  auto base = schema_registry_internal::variant_base_name(field.name);
  const std::string family_base = base.value_or(field.name);
  index.variants_by_base[family_base].push_back(position);
  auto parsed_version = schema_registry_internal::variant_version(field.name);
  if (!parsed_version)
    return;
  const int next_version = *parsed_version + 1;
  auto &current_next = index.next_variant_version[family_base];
  current_next = std::max(current_next, next_version);
}

FieldMergeIndex
build_field_merge_index(const std::vector<LogicalField> &fields) {
  FieldMergeIndex index;
  index.by_name.reserve(fields.size());
  index.variants_by_base.reserve(fields.size());
  index.next_variant_version.reserve(fields.size());
  for (std::size_t i = 0; i < fields.size(); ++i) {
    register_field(index, fields[i], i);
  }
  return index;
}

std::string next_variant_name(const FieldMergeIndex &index,
                              std::string_view base_name,
                              const LogicalType &incoming_type) {
  int version = 2;
  if (auto it = index.next_variant_version.find(base_name);
      it != index.next_variant_version.end()) {
    version = it->second;
  }
  const std::string semantic_type =
      schema_registry_internal::variant_semantic_type(incoming_type);
  while (true) {
    std::string candidate = std::string(base_name) + "_v" +
                            std::to_string(version) + "_" + semantic_type;
    if (!index.by_name.contains(std::string_view(candidate)))
      return candidate;
    ++version;
  }
}

bool is_scalar_type(const LogicalType &type) noexcept {
  return type.kind != LogicalKind::kStruct && type.kind != LogicalKind::kList;
}

bool is_scalar_integer_type(const LogicalField &field) noexcept {
  return field.type && field.type->kind == LogicalKind::kInt64;
}

bool is_scalar_float_type(const LogicalField &field) noexcept {
  return field.type && field.type->kind == LogicalKind::kFloat64;
}

bool is_scalar_numeric_type(const LogicalField &field) noexcept {
  return is_scalar_integer_type(field) || is_scalar_float_type(field);
}

bool is_unversioned_field(const LogicalField &field) {
  return !schema_registry_internal::variant_base_name(field.name).has_value();
}

std::string family_base_name(const LogicalField &field) {
  auto base = schema_registry_internal::variant_base_name(field.name);
  return base.value_or(field.name);
}

std::string canonical_versioned_name_for_type(const LogicalField &field) {
  const auto parsed =
      sanitize::internal::parse_versioned_field_name(field.name);
  if (!parsed || !field.type)
    return field.name;
  return std::string(parsed->base) + "_v" + std::to_string(parsed->version) +
         "_" + schema_registry_internal::variant_semantic_type(*field.type);
}

void normalize_integer_float_type(LogicalType &type);

struct NumericFamilyPlan {
  bool has_float = false;
  std::optional<std::size_t> numeric_keep_position;
};

void normalize_integer_float_fields(std::vector<LogicalField> &fields) {
  for (auto &field : fields) {
    if (field.type)
      normalize_integer_float_type(*field.type);
  }

  StringLookupMap<std::vector<std::size_t>> positions_by_family;
  positions_by_family.reserve(fields.size());
  for (std::size_t i = 0; i < fields.size(); ++i) {
    positions_by_family[family_base_name(fields[i])].push_back(i);
  }

  StringLookupMap<NumericFamilyPlan> plans;
  plans.reserve(positions_by_family.size());
  for (const auto &[family, positions] : positions_by_family) {
    NumericFamilyPlan plan;
    std::optional<std::size_t> first_float;
    for (std::size_t position : positions) {
      const LogicalField &field = fields[position];
      if (is_scalar_float_type(field)) {
        plan.has_float = true;
        if (!first_float)
          first_float = position;
      }
    }
    if (!plan.has_float) {
      plans.emplace(family, plan);
      continue;
    }

    for (std::size_t position : positions) {
      const LogicalField &field = fields[position];
      if (is_unversioned_field(field) && is_scalar_numeric_type(field)) {
        plan.numeric_keep_position = position;
        break;
      }
    }
    if (!plan.numeric_keep_position)
      plan.numeric_keep_position = first_float;
    plans.emplace(family, plan);
  }

  std::vector<LogicalField> out;
  out.reserve(fields.size());
  StringLookupMap<std::size_t> emitted_by_name;
  for (std::size_t i = 0; i < fields.size(); ++i) {
    LogicalField field = std::move(fields[i]);
    const std::string family = family_base_name(field);
    const auto plan_it = plans.find(family);
    if (plan_it != plans.end() && plan_it->second.has_float &&
        is_scalar_numeric_type(field)) {
      if (!plan_it->second.numeric_keep_position ||
          i != *plan_it->second.numeric_keep_position) {
        continue;
      }
      if (is_scalar_integer_type(field)) {
        field.type = std::make_unique<LogicalType>(LogicalType::Float64());
      }
    }

    field.name = canonical_versioned_name_for_type(field);
    if (auto duplicate = emitted_by_name.find(field.name);
        duplicate != emitted_by_name.end()) {
      LogicalField &previous = out[duplicate->second];
      if (previous.type && field.type &&
          schema_registry_internal::logical_type_equal(*previous.type,
                                                       *field.type)) {
        continue;
      }
    }
    emitted_by_name[field.name] = out.size();
    out.push_back(std::move(field));
  }
  fields = std::move(out);
}

void normalize_integer_float_type(LogicalType &type) {
  if (type.kind == LogicalKind::kStruct) {
    normalize_integer_float_fields(type.fields);
  } else if (type.kind == LogicalKind::kList && type.value) {
    normalize_integer_float_type(*type.value);
  }
}

std::optional<std::size_t>
find_field_index(const std::vector<LogicalField> &fields,
                 std::string_view name) noexcept {
  for (std::size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].name == name)
      return i;
  }
  return std::nullopt;
}

LogicalField make_nullable_field(std::string name, const LogicalType &type) {
  LogicalField field;
  field.name = std::move(name);
  field.nullable = true;
  field.type = std::make_unique<LogicalType>(type);
  return field;
}

std::optional<LogicalType>
merge_types(const LogicalType &base_type, const LogicalType &incoming_type,
            std::string_view source_path, std::vector<DriftEvent> &drifts,
            std::string_view detected_at, std::string_view default_key_name);

// Recursively merges into the newest compatible member of a version family.
bool merge_into_existing_family_variant(
    std::vector<LogicalField> &fields, const FieldMergeIndex &index,
    std::string_view base_name, std::optional<std::size_t> skipped_position,
    const LogicalType &incoming_type, std::string_view source_path,
    std::vector<DriftEvent> &drifts, std::string_view detected_at,
    std::string_view default_key_name) {
  auto family_it = index.variants_by_base.find(base_name);
  if (family_it == index.variants_by_base.end())
    return false;

  // Reprocessing must reuse an exact historical shape before considering a
  // newer family member that would need nested evolution.
  for (auto position_it = family_it->second.rbegin();
       position_it != family_it->second.rend(); ++position_it) {
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
  for (auto position_it = family_it->second.rbegin();
       position_it != family_it->second.rend(); ++position_it) {
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

std::vector<LogicalField>
merge_fields(const std::vector<LogicalField> &base_fields,
             const std::vector<LogicalField> &incoming_fields,
             std::string_view parent_path, std::vector<DriftEvent> &drifts,
             std::string_view detected_at, std::string_view default_key_name) {
  std::vector<LogicalField> out = base_fields;
  FieldMergeIndex index = build_field_merge_index(out);

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
      auto incoming_family_base =
          schema_registry_internal::variant_base_name(incoming.name);
      if (incoming_family_base && incoming.type &&
          index.variants_by_base.contains(*incoming_family_base)) {
        if (merge_into_existing_family_variant(
                out, index, *incoming_family_base, std::nullopt, *incoming.type,
                source_path, drifts, detected_at, default_key_name)) {
          continue;
        }

        LogicalField variant = incoming;
        variant.name =
            next_variant_name(index, *incoming_family_base, *incoming.type);
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

    const std::string family_base =
        schema_registry_internal::variant_base_name(incoming.name)
            .value_or(incoming.name);
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

void normalize_integer_float_schema(LogicalSchema &schema) {
  normalize_integer_float_fields(schema.fields);
}

} // namespace schema_registry_internal

Result<bool>
schema_registry_has_canonical_schema(std::string_view registry_json) {
  SAN_ASSIGN_OR_RAISE(
      auto schema,
      schema_registry_internal::canonical_schema_from_registry_json(
          registry_json));
  return schema && !schema->fields.empty();
}

Result<SchemaRegistryMergeResult> merge_schema_registry_with_previous(
    const SchemaRegistryMergeInput &input,
    std::optional<LogicalSchema> previous_schema) {
  if (input.field_name_policy.empty()) {
    return Status::Invalid("schema registry merge: field_name_policy is empty");
  }
  if (input.default_key_name.empty()) {
    return Status::Invalid("schema registry merge: default_key_name is empty");
  }

  std::string detected_at = input.detected_at;
  if (detected_at.empty()) {
    SAN_ASSIGN_OR_RAISE(detected_at, current_utc_iso_timestamp());
  }

  std::vector<DriftEvent> drifts;
  LogicalSchema schema;
  if (!previous_schema || previous_schema->fields.empty()) {
    schema = input.inferred_schema;
    schema_registry_internal::normalize_integer_float_schema(schema);
    for (const auto &field : schema.fields) {
      drifts.push_back(DriftEvent{
          .source_path = field.name,
          .output_name = field.name,
          .drift_type = "newly_added",
          .previous_schema = std::nullopt,
          .new_schema = schema_registry_internal::field_type_string(field)});
    }
  } else {
    schema_registry_internal::normalize_integer_float_schema(*previous_schema);
    LogicalSchema inferred_schema = input.inferred_schema;
    schema_registry_internal::normalize_integer_float_schema(inferred_schema);
    schema.fields =
        merge_fields(previous_schema->fields, inferred_schema.fields, "",
                     drifts, detected_at, input.default_key_name);
  }
  schema_registry_internal::normalize_integer_float_schema(schema);
  if (input.field_order == FieldOrderPolicy::kAlphabetically) {
    schema =
        internal::reorder_schema_fields(schema, nullptr, input.field_order);
  }

  SchemaRegistryMergeResult out;
  out.schema = std::move(schema);
  out.drifts_json =
      schema_registry_internal::drift_events_json(drifts, detected_at);
  out.registry_json = schema_registry_internal::registry_json(
      out.schema, input.registry_json, input.field_name_policy,
      !drifts.empty());
  out.detected_at = std::move(detected_at);
  return out;
}

Result<SchemaRegistryMergeResult>
merge_schema_registry(const SchemaRegistryMergeInput &input) {
  SAN_ASSIGN_OR_RAISE(
      auto previous_schema,
      schema_registry_internal::canonical_schema_from_registry_json(
          input.registry_json));
  return merge_schema_registry_with_previous(input, std::move(previous_schema));
}

Result<SchemaRegistryMergeResult> merge_schema_registry_with_previous_schema(
    const SchemaRegistryMergeInput &input,
    const LogicalSchema &previous_schema) {
  return merge_schema_registry_with_previous(input, previous_schema);
}

} // namespace sanitize

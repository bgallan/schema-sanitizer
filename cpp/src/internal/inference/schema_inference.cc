// Derives logical schemas from collected inference statistics.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#include "internal/inference/schema.hh"

#include <bit>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/parsing/string_scalar.hh"
#include "internal/planning/field_name_sanitizer.hh"
#include "sanitize/core/logical_schema.hh"

namespace sanitize::internal {

namespace {

/// Selects a logical scalar type from observed scalar kinds.
sanitize::LogicalType scalar_type_from_mask(uint32_t mask) {
  if (mask == 0)
    return sanitize::LogicalType::Utf8();
  const uint32_t numeric_mask = K_INT | K_FLOAT;
  if ((mask & ~numeric_mask) == 0 && (mask & K_FLOAT))
    return sanitize::LogicalType::Float64();
  if (std::popcount(mask) == 1) {
    if (mask & K_BOOL)
      return sanitize::LogicalType::Bool();
    if (mask & K_TS)
      return sanitize::LogicalType::TimestampNs();
    if (mask & K_DATE)
      return sanitize::LogicalType::Date32();
    if (mask & K_TIME)
      return sanitize::LogicalType::Time32s();
    if (mask & K_INT)
      return sanitize::LogicalType::Int64();
    if (mask & K_FLOAT)
      return sanitize::LogicalType::Float64();
    return sanitize::LogicalType::Utf8();
  }

  // Mixed scalar kinds always fall back to utf8.
  return sanitize::LogicalType::Utf8();
}

/// Reports whether unsupported direct list nesting requires a string element
/// fallback.
bool list_elem_requires_string_fallback(const StatsNode &st,
                                        bool direct_list_element = true) {
  // BigQuery and Arrow can represent list<struct<field: list<T>>>. The shape
  // we reject is list<list<T>>, where the list field's own element is another
  // list. Nested repeated struct fields are typed independently.
  if (st.is_list)
    return direct_list_element;

  if (!st.is_struct)
    return false;

  for (StrId k : st.key_order) {
    StatsNode *ch = st.find_child(k);
    if (ch && ch->has_evidence &&
        list_elem_requires_string_fallback(*ch, false))
      return true;
  }
  return false;
}

/// Derives one logical type recursively from inference statistics.
sanitize::LogicalType type_from_stats(const InferenceContext &ctx,
                                      const StatsNode &st,
                                      const sanitize::PreparedOptions &opts) {
  if (st.is_list) {
    if (!st.elem) {
      return sanitize::LogicalType::List(sanitize::LogicalType::Utf8());
    }
    // Fixed safety policy: typed lists are allowed for scalar lists and struct
    // lists, including nested repeated fields. Only list<list<T>> at this list
    // boundary falls back to list<string>.
    if (list_elem_requires_string_fallback(*st.elem)) {
      return sanitize::LogicalType::List(sanitize::LogicalType::Utf8());
    }
    auto elem_t = type_from_stats(ctx, *st.elem, opts);
    return sanitize::LogicalType::List(std::move(elem_t));
  }

  if (st.is_struct) {
    std::pmr::vector<std::string_view> raw_names(ctx.memory_resource());
    raw_names.reserve(st.key_order.size());
    for (StrId k : st.key_order) {
      StatsNode *child = st.find_child(k);
      if (child && child->has_evidence)
        raw_names.push_back(ctx.strings.str(k));
    }
    const std::vector<std::string> clean_names =
        clean_sibling_field_names(raw_names, opts, ctx.memory_resource());

    std::vector<sanitize::LogicalField> fields;
    fields.reserve(raw_names.size());
    std::size_t clean_index = 0;
    for (StrId k : st.key_order) {
      StatsNode *ch = st.find_child(k);
      if (!ch || !ch->has_evidence)
        continue;
      sanitize::LogicalField lf;
      lf.name = clean_names[clean_index++];
      lf.nullable = true;
      lf.type = std::make_unique<sanitize::LogicalType>(
          type_from_stats(ctx, *ch, opts));
      fields.push_back(std::move(lf));
    }
    return sanitize::LogicalType::Struct(std::move(fields));
  }

  return scalar_type_from_mask(st.scalar_kind_mask);
}

} // namespace

sanitize::LogicalSchema
infer_logical_schema(const InferenceContext &ctx,
                     const sanitize::PreparedOptions &opts) {
  sanitize::LogicalSchema schema;
  std::pmr::vector<std::string_view> raw_names(ctx.memory_resource());
  raw_names.reserve(ctx.root.key_order.size());
  for (StrId k : ctx.root.key_order) {
    StatsNode *child = ctx.root.find_child(k);
    if (child && child->has_evidence)
      raw_names.push_back(ctx.strings.str(k));
  }
  std::vector<std::string> clean_names =
      clean_sibling_field_names(raw_names, opts, ctx.memory_resource());
  for (std::size_t i = 0; i < raw_names.size(); ++i) {
    if (is_reserved_etl_column_name(raw_names[i]))
      clean_names[i] = raw_names[i];
  }

  schema.fields.reserve(raw_names.size());
  std::size_t clean_index = 0;
  for (StrId k : ctx.root.key_order) {
    StatsNode *ch = ctx.root.find_child(k);
    if (!ch || !ch->has_evidence)
      continue;
    sanitize::LogicalField lf;
    lf.name = clean_names[clean_index++];
    lf.nullable = true;
    lf.type = std::make_unique<sanitize::LogicalType>(
        type_from_stats(ctx, *ch, opts));
    schema.fields.push_back(std::move(lf));
  }
  return schema;
}

} // namespace sanitize::internal

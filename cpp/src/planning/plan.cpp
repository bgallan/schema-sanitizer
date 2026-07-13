// Compiles logical schemas into column plans and struct lookup metadata.

#include "internal/planning/plan_compile.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/planning/struct_layout.hh"
#include "internal/planning/variant_field_names.hh"
#include "internal/string_lookup.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/detail/hash.hh"
#include "sanitize/detail/intern.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize {

// Compiles one logical field into recursive materialization metadata.
static ColumnPlan compile_column(CompiledPlan *plan,
                                 const sanitize::LogicalField &lf,
                                 detail::PathId parent_path,
                                 bool is_list_elem) {

  ColumnPlan p;
  p.name = lf.name;
  p.name_hash = sanitize::detail::hash_key64(p.name);
  p.nullable = lf.nullable;
  p.logical_type =
      lf.type ? *lf.type : sanitize::LogicalType(sanitize::LogicalKind::kNull);

  if (is_list_elem) {
    // parent_path already encodes "[]".
    p.path_id = parent_path;
  } else {
    p.path_id = plan->paths.child(parent_path, plan->strings.intern(p.name));
  }

  if (lf.type && lf.type->kind == sanitize::LogicalKind::kStruct) {
    p.layout = std::make_unique<StructLayout>(
        internal::make_struct_layout(lf.type->fields));
    p.children.reserve(lf.type->fields.size());

    for (const auto &child : lf.type->fields) {
      p.children.push_back(compile_column(plan, child, p.path_id, false));
    }
  } else if (lf.type && lf.type->kind == sanitize::LogicalKind::kList &&
             lf.type->value) {
    sanitize::LogicalField elem;
    elem.name = "item";
    elem.nullable = true;
    elem.type = std::make_unique<sanitize::LogicalType>(*lf.type->value);

    detail::PathId elem_path = plan->paths.child(p.path_id, plan->list_marker);
    p.value = std::make_unique<ColumnPlan>(
        compile_column(plan, elem, elem_path, true));
  }

  return p;
}

// Precomputes version-family sibling indices for one sibling vector.
static void annotate_variant_siblings(std::vector<ColumnPlan> *columns) {
  if (!columns)
    return;

  std::vector<std::vector<int32_t>> families;
  families.reserve(columns->size());
  internal::BorrowedStringLookupMap<std::size_t> family_indices;
  family_indices.reserve(columns->size());

  for (std::size_t i = 0; i < columns->size(); ++i) {
    const std::string_view family_base =
        internal::variant_family_base((*columns)[i].name);
    const auto [it, inserted] =
        family_indices.try_emplace(family_base, families.size());
    if (inserted) {
      families.emplace_back();
    }
    families[it->second].push_back(static_cast<int32_t>(i));
  }

  for (const auto &family : families) {
    const bool has_siblings = family.size() > 1;
    for (const int32_t index : family) {
      auto &column = (*columns)[static_cast<std::size_t>(index)];
      column.has_variant_sibling = has_siblings;
      column.variant_sibling_indices = family;
    }
  }

  for (auto &column : *columns) {
    annotate_variant_siblings(&column.children);
    if (column.value) {
      annotate_variant_siblings(&column.value->children);
    }
  }
}

CompiledPlan::CompiledPlan() { list_marker = strings.intern("[]"); }

sanitize::Result<CompiledPlan>
compile_plan(const LogicalSchema &logical_schema) {
  CompiledPlan plan;

  plan.root_layout = internal::make_struct_layout(logical_schema.fields);
  plan.columns.reserve(logical_schema.fields.size());
  for (const auto &f : logical_schema.fields) {
    plan.columns.push_back(
        compile_column(&plan, f, detail::PathInterner::root(), false));
  }
  annotate_variant_siblings(&plan.columns);
  return plan;
}

} // namespace sanitize

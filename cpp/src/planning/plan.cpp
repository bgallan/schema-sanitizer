// Compiles logical schemas into column plans and struct lookup metadata.

#include "internal/planning/plan_compile.hh"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/variant_field_names.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/detail/intern.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize {

// Builds an open-addressed field-name dispatch table.
static void
build_dispatch_table(StructLayout::DispatchTable *dt,
                     const std::vector<StructLayout::KeyEntry> &entries) {
  if (!dt)
    return;
  if (entries.empty()) {
    dt->mask = 0;
    dt->hashes.clear();
    dt->keys.clear();
    dt->values.clear();
    return;
  }

  // Load factor ~= 0.5 for very low probe count.
  const auto want = static_cast<uint32_t>(entries.size() * 2u);
  const auto cap = std::bit_ceil(want);

  dt->mask = cap - 1;
  dt->hashes.assign(cap, 0);
  dt->keys.assign(cap, std::string_view{});
  dt->values.assign(cap, FieldIndex{});

  for (const auto &e : entries) {
    const uint64_t h = StructLayout::DispatchTable::hash_key(e.key);
    auto pos = static_cast<uint32_t>(h) & dt->mask;
    while (dt->hashes[pos] != 0) {
      pos = (pos + 1) & dt->mask;
    }
    dt->hashes[pos] = h;
    dt->keys[pos] = e.key;
    dt->values[pos] = e.fi;
  }
}

// Creates lookup metadata for a struct field list.
static StructLayout
make_layout(const std::vector<sanitize::LogicalField> &fields) {
  constexpr std::size_t kSortedThreshold =
      32; // binary-search for small structs

  StructLayout l;
  l.names.reserve(fields.size());
  l.alias_names.reserve(fields.size() * 2u);

  std::vector<StructLayout::KeyEntry> entries;
  std::vector<StructLayout::KeyEntry> alias_entries;
  entries.reserve(fields.size());
  alias_entries.reserve(fields.size());

  auto try_add_to = [](std::vector<StructLayout::KeyEntry> *target,
                       std::string_view key, FieldIndex fi) {
    for (const auto &e : *target) {
      if (e.key == key)
        return; // skip collisions/duplicates
    }
    target->push_back(StructLayout::KeyEntry{.key = key, .fi = fi});
  };
  auto contains_key = [](const std::vector<StructLayout::KeyEntry> &target,
                         std::string_view key) {
    for (const auto &e : target) {
      if (e.key == key)
        return true;
    }
    return false;
  };

  for (std::size_t i = 0; i < fields.size(); ++i) {
    const auto &n = fields[i].name;
    l.names.push_back(n);
    std::string_view sv(l.names.back());
    try_add_to(&entries, sv, FieldIndex{static_cast<int32_t>(i)});
  }

  for (std::size_t i = 0; i < l.names.size(); ++i) {
    const std::string_view clean(l.names[i]);
    const FieldIndex fi{static_cast<int32_t>(i)};
    const std::string_view unflattened =
        internal::unflattened_output_name(clean);
    if (!unflattened.empty() && !contains_key(entries, unflattened)) {
      l.alias_names.emplace_back(unflattened);
      try_add_to(&alias_entries, std::string_view(l.alias_names.back()), fi);
    }
    const std::string_view version_base = internal::variant_family_base(clean);
    if (!version_base.empty() && version_base != clean &&
        !contains_key(entries, version_base)) {
      l.alias_names.emplace_back(version_base);
      try_add_to(&alias_entries, std::string_view(l.alias_names.back()), fi);
    }
  }

  constexpr std::size_t kMaxDispatchEntries =
      (std::numeric_limits<uint32_t>::max() / 4u) + 1u;
  if (entries.size() <= kSortedThreshold ||
      entries.size() > kMaxDispatchEntries) {
    l.sorted = std::move(entries);
    std::ranges::sort(l.sorted, {}, &StructLayout::KeyEntry::key);
  } else {
    build_dispatch_table(&l.table, entries);
  }

  if (!alias_entries.empty()) {
    if (alias_entries.size() <= kSortedThreshold ||
        alias_entries.size() > kMaxDispatchEntries) {
      l.alias_sorted = std::move(alias_entries);
      std::ranges::sort(l.alias_sorted, {}, &StructLayout::KeyEntry::key);
    } else {
      build_dispatch_table(&l.alias_table, alias_entries);
    }
  }

  return l;
}

// Compiles one logical field into recursive materialization metadata.
static ColumnPlan compile_column(CompiledPlan *plan,
                                 const sanitize::LogicalField &lf,
                                 detail::PathId parent_path,
                                 bool is_list_elem) {

  ColumnPlan p;
  p.name = lf.name;
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
    p.layout = std::make_unique<StructLayout>(make_layout(lf.type->fields));
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
  for (auto &column : *columns) {
    column.variant_family_base =
        std::string(internal::variant_family_base(column.name));
    column.has_variant_sibling = false;
    column.variant_sibling_indices.clear();
  }

  for (std::size_t i = 0; i < columns->size(); ++i) {
    auto &column = (*columns)[i];
    for (std::size_t j = 0; j < columns->size(); ++j) {
      if (!internal::in_variant_family((*columns)[j].name,
                                       column.variant_family_base))
        continue;
      column.variant_sibling_indices.push_back(static_cast<int32_t>(j));
      if (i != j)
        column.has_variant_sibling = true;
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

  plan.root_layout = make_layout(logical_schema.fields);
  plan.columns.reserve(logical_schema.fields.size());
  for (const auto &f : logical_schema.fields) {
    plan.columns.push_back(
        compile_column(&plan, f, detail::PathInterner::root(), false));
  }
  annotate_variant_siblings(&plan.columns);
  return plan;
}

} // namespace sanitize

// Builds field-name lookup metadata for compiled struct plans.

#include "internal/planning/struct_layout.hh"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/variant_field_names.hh"

namespace sanitize::internal {
namespace {

void build_dispatch_table(StructLayout::DispatchTable *table,
                          const std::vector<StructLayout::KeyEntry> &entries) {
  if (!table)
    return;
  if (entries.empty()) {
    table->mask = 0;
    table->hashes.clear();
    table->keys.clear();
    table->values.clear();
    return;
  }

  const auto capacity =
      std::bit_ceil(static_cast<uint32_t>(entries.size() * 2u));
  table->mask = capacity - 1;
  table->hashes.assign(capacity, 0);
  table->keys.assign(capacity, std::string_view{});
  table->values.assign(capacity, FieldIndex{});

  for (const auto &entry : entries) {
    const uint64_t hash = StructLayout::DispatchTable::hash_key(entry.key);
    auto position = static_cast<uint32_t>(hash) & table->mask;
    while (table->hashes[position] != 0)
      position = (position + 1) & table->mask;
    table->hashes[position] = hash;
    table->keys[position] = entry.key;
    table->values[position] = entry.fi;
  }
}

} // namespace

StructLayout make_struct_layout(const std::vector<LogicalField> &fields) {
  constexpr std::size_t kSortedThreshold = 32;
  constexpr std::size_t kMaxDispatchEntries =
      (std::numeric_limits<uint32_t>::max() / 4u) + 1u;

  StructLayout layout;
  layout.names.reserve(fields.size());
  layout.alias_names.reserve(fields.size() * 2u);

  std::vector<StructLayout::KeyEntry> entries;
  std::vector<StructLayout::KeyEntry> aliases;
  entries.reserve(fields.size());
  aliases.reserve(fields.size());

  auto add_unique = [](std::vector<StructLayout::KeyEntry> *target,
                       std::string_view key, FieldIndex index) {
    for (const auto &entry : *target) {
      if (entry.key == key)
        return;
    }
    target->push_back(StructLayout::KeyEntry{.key = key, .fi = index});
  };
  auto contains = [](const std::vector<StructLayout::KeyEntry> &target,
                     std::string_view key) {
    return std::ranges::any_of(
        target, [key](const auto &entry) { return entry.key == key; });
  };

  for (std::size_t index = 0; index < fields.size(); ++index) {
    layout.names.push_back(fields[index].name);
    add_unique(&entries, layout.names.back(),
               FieldIndex{static_cast<int32_t>(index)});
  }

  for (std::size_t index = 0; index < layout.names.size(); ++index) {
    const std::string_view clean(layout.names[index]);
    const FieldIndex field_index{static_cast<int32_t>(index)};
    const auto unflattened = unflattened_output_name(clean);
    if (!unflattened.empty() && !contains(entries, unflattened)) {
      layout.alias_names.emplace_back(unflattened);
      add_unique(&aliases, layout.alias_names.back(), field_index);
    }
    const auto version_base = variant_family_base(clean);
    if (!version_base.empty() && version_base != clean &&
        !contains(entries, version_base)) {
      layout.alias_names.emplace_back(version_base);
      add_unique(&aliases, layout.alias_names.back(), field_index);
    }
  }

  if (entries.size() <= kSortedThreshold ||
      entries.size() > kMaxDispatchEntries) {
    layout.sorted = std::move(entries);
    std::ranges::sort(layout.sorted, {}, &StructLayout::KeyEntry::key);
  } else {
    build_dispatch_table(&layout.table, entries);
  }

  if (!aliases.empty()) {
    if (aliases.size() <= kSortedThreshold ||
        aliases.size() > kMaxDispatchEntries) {
      layout.alias_sorted = std::move(aliases);
      std::ranges::sort(layout.alias_sorted, {}, &StructLayout::KeyEntry::key);
    } else {
      build_dispatch_table(&layout.alias_table, aliases);
    }
  }
  return layout;
}

} // namespace sanitize::internal

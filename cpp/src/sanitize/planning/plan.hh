// Defines compiled column plans and struct dispatch lookup tables.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/detail/hash.hh"
#include "sanitize/detail/intern.hh"

namespace sanitize {

struct FieldIndex {
  int32_t index = -1;
};

// A schema-compiled key->field mapping for STRUCT/Object access.
//
// Strategy:
// - For small structs: keep a sorted small-vector (binary search; no hashing).
// - For large structs: build a custom open-addressing dispatch table.
//
// This removes std::unordered_map lookups from the hot path.
struct StructLayout {
  std::vector<std::string> names;

  struct KeyEntry {
    std::string_view key;
    FieldIndex fi;
  };

  // Used for small layouts (sorted by key).
  std::vector<KeyEntry> sorted;

  struct DispatchTable {
    uint32_t mask = 0;
    std::vector<uint64_t> hashes;       // 0 => empty
    std::vector<std::string_view> keys; // only valid when hashes[i] != 0
    std::vector<FieldIndex> values;     // only valid when hashes[i] != 0

    // Computes the hash for key.
    static uint64_t hash_key(std::string_view s) noexcept {
      return detail::hash_key64(s);
    }

    // Returns whether the feature is enabled.
    [[nodiscard]] bool enabled() const noexcept { return !hashes.empty(); }

    // Finds a field index in the dispatch table.
    [[nodiscard]] const FieldIndex *find(std::string_view key,
                                         uint64_t prehash) const noexcept {
      if (hashes.empty())
        return nullptr;
      const uint64_t h = prehash ? prehash : hash_key(key);
      auto pos = static_cast<uint32_t>(h) & mask;
      for (;;) {
        const uint64_t slot = hashes[pos];
        if (slot == 0)
          return nullptr;
        if (slot == h && keys[pos] == key)
          return &values[pos];
        pos = (pos + 1) & mask;
      }
    }
  };

  // Used for large layouts.
  DispatchTable table;
  std::vector<std::string> alias_names;
  std::vector<KeyEntry> alias_sorted;
  DispatchTable alias_table;

  // Finds a field index using the layout's selected lookup strategy.
  [[nodiscard]] const FieldIndex *find(std::string_view key,
                                       uint64_t prehash = 0) const noexcept {
    if (table.enabled())
      return table.find(key, prehash);

    // Binary search on sorted entries.
    auto it =
        std::ranges::lower_bound(sorted, key, std::less<>{}, &KeyEntry::key);
    if (it != sorted.end() && it->key == key)
      return &it->fi;
    return nullptr;
  }

  // Finds a field index using precomputed alternate source-name aliases.
  [[nodiscard]] const FieldIndex *
  find_alias(std::string_view key, uint64_t prehash = 0) const noexcept {
    if (alias_table.enabled())
      return alias_table.find(key, prehash);

    auto it = std::ranges::lower_bound(alias_sorted, key, std::less<>{},
                                       &KeyEntry::key);
    if (it != alias_sorted.end() && it->key == key)
      return &it->fi;
    return nullptr;
  }
};

struct ColumnPlan {
  std::string name;
  uint64_t name_hash = 0;
  LogicalType logical_type;
  bool nullable = true;

  detail::PathId path_id = 0;

  // Schema-registry version-family metadata, precomputed for sibling routing.
  bool has_variant_sibling = false;
  std::vector<int32_t> variant_sibling_indices;

  // Child field layout for STRUCT columns.
  std::unique_ptr<StructLayout> layout;
  std::vector<ColumnPlan> children;

  // Element plan for LIST columns.
  std::unique_ptr<ColumnPlan> value;
};

struct CompiledPlan {
  detail::StringInterner strings;
  detail::PathInterner paths;
  detail::StrId list_marker = 0;

  StructLayout root_layout;
  std::vector<ColumnPlan> columns;

  // Creates a CompiledPlan.
  CompiledPlan();
};

} // namespace sanitize

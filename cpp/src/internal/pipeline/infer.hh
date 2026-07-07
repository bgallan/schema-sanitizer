// Declares inference shape/statistics state and row scanners.

#pragma once

#include <cstdint>
#include <memory_resource>
#include <unordered_map>
#include <vector>

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/detail/intern.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

using detail::PathId;
using detail::PathInterner;
using detail::StrId;
using detail::StringInterner;

struct Shape {
  bool seen_list = false;
  bool seen_struct = false;
};

struct StatsNode {
  uint32_t scalar_kind_mask = 0;
  bool is_struct = false;
  bool is_list = false;
  bool has_evidence = false;

  std::vector<StrId> key_order;

  // Child map without std::unordered_map in the hot path:
  // - small: linear scan of entries
  // - large: open-addressing dispatch table (load factor ~0.5)
  struct ChildEntry {
    StrId key = 0;
    StatsNode *child = nullptr;
    uint64_t hash = 0; // non-zero
  };

  // Computes the open-addressing hash for an interned string id.
  static uint64_t hash_strid(StrId x) noexcept {
    // 64-bit mix (SplitMix64 style). Reserve 0 as empty sentinel.
    auto z = static_cast<uint64_t>(x) + uint64_t{0x9e3779b97f4a7c15ull};
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    z = z ^ (z >> 31);
    return z == 0 ? 1ull : z;
  }

  struct DispatchTable {
    uint32_t mask = 0;
    uint32_t size = 0;
    std::vector<uint64_t> hashes; // 0 => empty
    std::vector<StrId> keys;
    std::vector<StatsNode *> values;

    // Returns whether the feature is enabled.
    [[nodiscard]] bool enabled() const noexcept { return !hashes.empty(); }

    // Finds a child node in the dispatch table.
    [[nodiscard]] StatsNode *find(StrId key, uint64_t h) const noexcept {
      if (hashes.empty())
        return nullptr;
      auto pos = static_cast<uint32_t>(h) & mask;
      for (;;) {
        uint64_t slot = hashes[pos];
        if (slot == 0)
          return nullptr;
        if (slot == h && keys[pos] == key)
          return values[pos];
        pos = (pos + 1) & mask;
      }
    }

    // Inserts an entry into the open-addressing dispatch table.
    void insert(StrId key, uint64_t h, StatsNode *value);
    // Builds an open-addressing table from child entries.
    void build_from(const std::vector<ChildEntry> &entries);
  };

  std::vector<ChildEntry> children;
  DispatchTable table;

  StatsNode *elem = nullptr;

  // Finds an existing named child node.
  [[nodiscard]] StatsNode *find_child(StrId key) const noexcept {
    const uint64_t h = hash_strid(key);
    if (table.enabled())
      return table.find(key, h);
    for (const auto &e : children) {
      if (e.key == key)
        return e.child;
    }
    return nullptr;
  }

  // Returns or creates the statistics node for a named child.
  StatsNode *child(StrId key, std::pmr::monotonic_buffer_resource *arena);
  // Returns or creates the statistics node for list elements.
  StatsNode *list_elem(std::pmr::monotonic_buffer_resource *arena);
};

struct InferenceContext {
  StringInterner strings;
  PathInterner paths;
  std::pmr::monotonic_buffer_resource arena;

  // Special component for list elements.
  StrId list_marker{};
  // Interned key used when scalar values are wrapped as structs.
  StrId default_key_id{};

  // Dense by PathId (PathInterner produces sequential ids).
  std::vector<Shape> shapes;
  StatsNode root;

  // Caches synthetic "<key>_flattened" interned ids for over-depth fields.
  std::unordered_map<StrId, StrId> flattened_key_cache;

  // Creates an inference context.
  InferenceContext();
  // Stores the per-run default key in the interner.
  void set_default_key(std::string_view key);

  // Ensures shape storage exists for a path id.
  void ensure_shape(PathId id) {
    if (id >= shapes.size())
      shapes.resize(static_cast<std::size_t>(id) + 1);
  }

  // Returns the shape entry for a path, growing storage as needed.
  Shape &shape(PathId id) {
    ensure_shape(id);
    return shapes[static_cast<std::size_t>(id)];
  }
};

// Scans one row for structural shape information.
sanitize::Status scan_shapes_row(InferenceContext *ctx, const RowRef &row,
                                 const PreparedOptions &opts,
                                 IngestDiagnostics *diag);

// Updates scalar statistics for one row.
sanitize::Status update_stats_row(InferenceContext *ctx, const RowRef &row,
                                  const PreparedOptions &opts,
                                  IngestDiagnostics *diag);

// Infers logical schema.
sanitize::LogicalSchema infer_logical_schema(const InferenceContext &ctx,
                                             const PreparedOptions &opts);

} // namespace sanitize::internal

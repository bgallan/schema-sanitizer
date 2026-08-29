// Declares inference shape and statistics state.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#pragma once

#include <cstdint>
#include <limits>
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
  /// Creates a node whose variable-size storage is owned by the inference
  /// arena.
  explicit StatsNode(std::pmr::memory_resource *resource)
      : key_order(resource), children(resource), table(resource) {}

  uint32_t scalar_kind_mask = 0;
  bool is_struct = false;
  bool is_list = false;
  bool has_evidence = false;

  std::pmr::vector<StrId> key_order;

  // Child map without std::unordered_map in the hot path:
  // - small: linear scan of canonical child entries
  // - large: open-addressing slots into those same entries
  struct ChildEntry {
    StrId key = 0;
    StatsNode *child = nullptr;
    uint64_t hash = 0; // non-zero
  };

  /// Computes the open-addressing hash for an interned string id.
  static uint64_t hash_strid(StrId x) noexcept {
    // 64-bit mix (SplitMix64 style). Reserve 0 as empty hash.
    auto z = static_cast<uint64_t>(x) + uint64_t{0x9e3779b97f4a7c15ull};
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    z = z ^ (z >> 31);
    return z == 0 ? 1ull : z;
  }

  struct DispatchTable {
    static constexpr uint32_t kEmptySlot = std::numeric_limits<uint32_t>::max();

    /// Creates a dispatch table backed by the inference arena.
    explicit DispatchTable(std::pmr::memory_resource *resource)
        : slots(resource) {}

    uint32_t mask = 0;
    uint32_t size = 0;
    std::pmr::vector<uint32_t> slots;

    /// Reports whether open-addressing slots have been initialized.
    [[nodiscard]] bool enabled() const noexcept { return !slots.empty(); }

    /// Finds a child node through an index into the canonical entries vector.
    [[nodiscard]] StatsNode *
    find(StrId key, uint64_t hash,
         const std::pmr::vector<ChildEntry> &entries) const noexcept {
      if (slots.empty())
        return nullptr;
      auto pos = static_cast<uint32_t>(hash) & mask;
      for (;;) {
        const uint32_t entry_index = slots[pos];
        if (entry_index == kEmptySlot)
          return nullptr;
        const auto &entry = entries[entry_index];
        if (entry.hash == hash && entry.key == key)
          return entry.child;
        pos = (pos + 1) & mask;
      }
    }

    /// Inserts the newest canonical child entry into the dispatch table.
    void insert(const std::pmr::vector<ChildEntry> &entries,
                uint32_t entry_index);
    /// Rebuilds open-addressing slots from canonical child entries.
    void build_from(const std::pmr::vector<ChildEntry> &entries);
    /// Disables the optional dispatch acceleration without affecting children.
    void disable() noexcept {
      mask = 0;
      size = 0;
      slots.clear();
    }
  };

  std::pmr::vector<ChildEntry> children;
  DispatchTable table;

  StatsNode *elem = nullptr;

  /// Finds an existing named child node.
  [[nodiscard]] StatsNode *find_child(StrId key) const noexcept {
    const uint64_t hash = hash_strid(key);
    if (table.enabled())
      return table.find(key, hash, children);
    for (const auto &entry : children) {
      if (entry.key == key)
        return entry.child;
    }
    return nullptr;
  }

  /// Returns or creates the statistics node for a named child.
  StatsNode *child(StrId key, std::pmr::monotonic_buffer_resource *arena);
  /// Returns or creates the statistics node for list elements.
  StatsNode *list_elem(std::pmr::monotonic_buffer_resource *arena);
};

struct InferenceContext {
  // One monotonic owner keeps all inference-only containers inside the
  // operation memory pool and releases them together at the end of inference.
  std::pmr::monotonic_buffer_resource arena;
  StringInterner strings;
  PathInterner paths;

  // Special component for list elements.
  StrId list_marker{};
  // Interned key used when scalar values are wrapped as structs.
  StrId default_key_id{};

  // Dense by PathId (PathInterner produces sequential ids).
  std::pmr::vector<Shape> shapes;
  StatsNode root;

  // Caches synthetic "<key>_flattened" interned ids for over-depth fields.
  std::pmr::unordered_map<StrId, StrId> flattened_key_cache;

  /// Creates an inference context using the supplied PMR upstream.
  explicit InferenceContext(
      std::pmr::memory_resource *upstream = std::pmr::get_default_resource());
  /// Returns the polymorphic resource that owns this inference state's
  /// allocations.
  [[nodiscard]] std::pmr::memory_resource *memory_resource() const noexcept {
    return const_cast<std::pmr::monotonic_buffer_resource *>(&arena);
  }
  /// Stores the per-run default key in the interner.
  void set_default_key(std::string_view key);

  /// Ensures shape storage exists for a path id.
  void ensure_shape(PathId id) {
    if (id >= shapes.size())
      shapes.resize(static_cast<std::size_t>(id) + 1);
  }

  /// Returns the shape entry for a path, growing storage as needed.
  Shape &shape(PathId id) {
    ensure_shape(id);
    return shapes[static_cast<std::size_t>(id)];
  }
};

} // namespace sanitize::internal

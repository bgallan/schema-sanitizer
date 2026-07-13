// Stats storage and lookup helpers for inference.

#include "internal/inference/statistics/state.hh"

#include <bit>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>

namespace sanitize::internal {
namespace {

// Allocates an inference stats node and all of its dynamic storage from the
// monotonic arena.  No per-node heap ownership remains after the context dies.
StatsNode *make_node(std::pmr::monotonic_buffer_resource *arena) {
  std::pmr::polymorphic_allocator<StatsNode> allocator(arena);
  StatsNode *node = allocator.allocate(1);
  return std::construct_at(node, arena);
}

} // namespace

void StatsNode::DispatchTable::build_from(
    const std::pmr::vector<ChildEntry> &entries) {
  constexpr std::size_t kMaxDispatchEntries =
      (std::numeric_limits<uint32_t>::max() / 4u) + 1u;
  if (entries.empty() || entries.size() > kMaxDispatchEntries) {
    mask = 0;
    size = 0;
    slots.clear();
    return;
  }

  const auto wanted = static_cast<uint32_t>(entries.size() * 2u);
  const auto capacity = std::bit_ceil(wanted);
  mask = capacity - 1;
  size = static_cast<uint32_t>(entries.size());
  slots.assign(capacity, kEmptySlot);

  for (uint32_t entry_index = 0; entry_index < size; ++entry_index) {
    const auto &entry = entries[entry_index];
    auto pos = static_cast<uint32_t>(entry.hash) & mask;
    while (slots[pos] != kEmptySlot) {
      pos = (pos + 1) & mask;
    }
    slots[pos] = entry_index;
  }
}

void StatsNode::DispatchTable::insert(
    const std::pmr::vector<ChildEntry> &entries, uint32_t entry_index) {
  if (!enabled() || entry_index >= entries.size()) {
    build_from(entries);
    return;
  }

  // Rehash before crossing a 0.5 load factor.  Rebuilding reads directly from
  // the canonical child vector instead of materializing a second entry list.
  if ((static_cast<std::size_t>(size) + 1u) * 2u >= slots.size()) {
    build_from(entries);
    return;
  }

  const auto &entry = entries[entry_index];
  auto pos = static_cast<uint32_t>(entry.hash) & mask;
  while (slots[pos] != kEmptySlot) {
    pos = (pos + 1) & mask;
  }
  slots[pos] = entry_index;
  ++size;
}

StatsNode *StatsNode::child(StrId key,
                            std::pmr::monotonic_buffer_resource *arena) {
  if (StatsNode *existing = find_child(key))
    return existing;
  StatsNode *node = make_node(arena);
  const uint64_t hash = hash_strid(key);
  children.push_back(ChildEntry{.key = key, .child = node, .hash = hash});
  key_order.push_back(key);

  constexpr std::size_t kThreshold = 64;
  if (!table.enabled() && children.size() >= kThreshold) {
    table.build_from(children);
  } else if (table.enabled()) {
    table.insert(children, static_cast<uint32_t>(children.size() - 1u));
  }
  return node;
}

StatsNode *StatsNode::list_elem(std::pmr::monotonic_buffer_resource *arena) {
  if (elem)
    return elem;
  elem = make_node(arena);
  return elem;
}

InferenceContext::InferenceContext() : root(&arena) {
  list_marker = strings.intern("[]");
  default_key_id = strings.intern("default_key");
  root.is_struct = true;
}

void InferenceContext::set_default_key(std::string_view key) {
  default_key_id = strings.intern(key);
}

} // namespace sanitize::internal

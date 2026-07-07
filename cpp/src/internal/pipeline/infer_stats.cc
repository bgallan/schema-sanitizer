// Stats storage and lookup helpers for inference.

#include "internal/pipeline/infer.hh"

#include <bit>
#include <cstdint>
#include <limits>
#include <memory_resource>
#include <vector>

namespace sanitize::internal {
namespace {

// Allocates an inference stats node from the monotonic arena.
static StatsNode *make_node(std::pmr::monotonic_buffer_resource *arena) {
  void *mem = arena->allocate(sizeof(StatsNode), alignof(StatsNode));
  return new (mem) StatsNode();
}

} // namespace

void StatsNode::DispatchTable::build_from(
    const std::vector<ChildEntry> &entries) {
  constexpr std::size_t kMaxDispatchEntries =
      (std::numeric_limits<uint32_t>::max() / 4u) + 1u;
  auto clear_table = [this]() {
    mask = 0;
    size = 0;
    hashes.clear();
    keys.clear();
    values.clear();
  };
  if (entries.empty()) {
    clear_table();
    return;
  }
  if (entries.size() > kMaxDispatchEntries) {
    // Keep linear child lookup rather than overflowing the uint32_t table
    // addressing used by this hot-path dispatch table.
    clear_table();
    return;
  }

  const auto want = static_cast<uint32_t>(entries.size() * 2u);
  const auto cap = std::bit_ceil(want);
  mask = cap - 1;
  size = 0;
  hashes.assign(cap, 0);
  keys.assign(cap, 0);
  values.assign(cap, nullptr);

  for (const auto &entry : entries) {
    auto pos = static_cast<uint32_t>(entry.hash) & mask;
    while (hashes[pos] != 0) {
      pos = (pos + 1) & mask;
    }
    hashes[pos] = entry.hash;
    keys[pos] = entry.key;
    values[pos] = entry.child;
    ++size;
  }
}

void StatsNode::DispatchTable::insert(StrId key, uint64_t hash,
                                      StatsNode *value) {
  if (!enabled()) {
    // Start with a small table once we cross the threshold.
    build_from(std::vector<ChildEntry>{
        ChildEntry{.key = key, .child = value, .hash = hash}});
    return;
  }

  // If load becomes too high, rebuild with 2x capacity.
  if (size >= (std::numeric_limits<uint32_t>::max() / 2u) ||
      (static_cast<std::size_t>(size) + 1u) * 2u >= hashes.size()) {
    std::vector<ChildEntry> entries;
    entries.reserve(static_cast<std::size_t>(size) + 1u);
    for (std::size_t i = 0; i < hashes.size(); ++i) {
      if (hashes[i] == 0)
        continue;
      entries.push_back(
          ChildEntry{.key = keys[i], .child = values[i], .hash = hashes[i]});
    }
    entries.push_back(ChildEntry{.key = key, .child = value, .hash = hash});
    build_from(entries);
    return;
  }

  auto pos = static_cast<uint32_t>(hash) & mask;
  while (hashes[pos] != 0) {
    pos = (pos + 1) & mask;
  }
  hashes[pos] = hash;
  keys[pos] = key;
  values[pos] = value;
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
    table.insert(key, hash, node);
  }
  return node;
}

StatsNode *StatsNode::list_elem(std::pmr::monotonic_buffer_resource *arena) {
  if (elem)
    return elem;
  elem = make_node(arena);
  return elem;
}

InferenceContext::InferenceContext() {
  list_marker = strings.intern("[]");
  default_key_id = strings.intern("default_key");
  root.is_struct = true;
}

void InferenceContext::set_default_key(std::string_view key) {
  default_key_id = strings.intern(key);
}

} // namespace sanitize::internal

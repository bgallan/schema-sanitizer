// Implements inference statistics storage and lookup helpers. The code keeps
// bounded shape discovery and scalar evidence consistent across serial and
// parallel scans.

#include "internal/inference/statistics/state.hh"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <stdexcept>

namespace sanitize::internal {
namespace {

/// Allocates an inference statistics node and its dynamic storage from the
/// monotonic arena.
StatsNode *make_node(std::pmr::monotonic_buffer_resource *arena) {
  std::pmr::polymorphic_allocator<StatsNode> allocator(arena);
  StatsNode *node = allocator.allocate(1);
  return std::construct_at(node, arena);
}

/// Grows a statistics vector geometrically before its next append would
/// reallocate.
template <class T> void ensure_append_capacity(std::pmr::vector<T> *values) {
  if (!values || values->size() < values->capacity()) {
    return;
  }
  if (values->size() == values->max_size()) {
    throw std::length_error("inference vector capacity exhausted");
  }
  const auto current = values->capacity();
  const auto maximum = values->max_size();
  const auto doubled = current > maximum / 2U
                           ? maximum
                           : std::max<std::size_t>(8U, current * 2U);
  values->reserve(std::max(values->size() + 1U, doubled));
}

} // namespace

void StatsNode::DispatchTable::build_from(
    const std::pmr::vector<ChildEntry> &entries) {
  constexpr std::size_t kMaxDispatchEntries =
      (std::numeric_limits<std::uint32_t>::max() / 4U) + 1U;
  if (entries.empty() || entries.size() > kMaxDispatchEntries) {
    disable();
    return;
  }

  const auto wanted = static_cast<std::uint32_t>(entries.size() * 2U);
  const auto capacity = std::bit_ceil(wanted);
  std::pmr::vector<std::uint32_t> replacement(slots.get_allocator().resource());
  replacement.assign(capacity, kEmptySlot);

  const auto replacement_mask = capacity - 1U;
  const auto replacement_size = static_cast<std::uint32_t>(entries.size());
  for (std::uint32_t entry_index = 0; entry_index < replacement_size;
       ++entry_index) {
    const auto &entry = entries[entry_index];
    auto position = static_cast<std::uint32_t>(entry.hash) & replacement_mask;
    while (replacement[position] != kEmptySlot) {
      position = (position + 1U) & replacement_mask;
    }
    replacement[position] = entry_index;
  }

  slots.swap(replacement);
  mask = replacement_mask;
  size = replacement_size;
}

void StatsNode::DispatchTable::insert(
    const std::pmr::vector<ChildEntry> &entries, std::uint32_t entry_index) {
  if (!enabled() || entry_index >= entries.size()) {
    build_from(entries);
    return;
  }

  // Rehash before crossing a 0.5 load factor. Rebuilding reads directly from
  // the canonical child vector instead of materializing a second entry list.
  if ((static_cast<std::size_t>(size) + 1U) * 2U >= slots.size()) {
    build_from(entries);
    return;
  }

  const auto &entry = entries[entry_index];
  auto position = static_cast<std::uint32_t>(entry.hash) & mask;
  while (slots[position] != kEmptySlot) {
    position = (position + 1U) & mask;
  }
  slots[position] = entry_index;
  ++size;
}

StatsNode *StatsNode::child(StrId key,
                            std::pmr::monotonic_buffer_resource *arena) {
  if (StatsNode *existing = find_child(key)) {
    return existing;
  }

  // Grow both canonical vectors before allocating the node. Once these
  // reserves succeed, appending their trivially copyable entries cannot leave
  // only one side updated if a later allocation fails.
  ensure_append_capacity(&children);
  ensure_append_capacity(&key_order);
  StatsNode *node = make_node(arena);
  const std::uint64_t hash = hash_strid(key);
  children.push_back(ChildEntry{.key = key, .child = node, .hash = hash});
  key_order.push_back(key);

  constexpr std::size_t kThreshold = 64;
  try {
    if (!table.enabled() && children.size() >= kThreshold) {
      table.build_from(children);
    } else if (table.enabled()) {
      table.insert(children, static_cast<std::uint32_t>(children.size() - 1U));
    }
  } catch (const std::bad_alloc &) {
    // The table is an optional acceleration structure. Preserve the canonical
    // children and fall back to linear lookup rather than exposing a partially
    // rebuilt table after an allocation failure.
    table.disable();
  } catch (const std::length_error &) {
    table.disable();
  }
  return node;
}

StatsNode *StatsNode::list_elem(std::pmr::monotonic_buffer_resource *arena) {
  if (elem) {
    return elem;
  }
  elem = make_node(arena);
  return elem;
}

InferenceContext::InferenceContext(std::pmr::memory_resource *upstream)
    : arena(upstream ? upstream : std::pmr::get_default_resource()),
      strings(&arena), paths(&arena), shapes(&arena), root(&arena),
      flattened_key_cache(&arena) {
  list_marker = strings.intern("[]");
  default_key_id = strings.intern("default_key");
  root.is_struct = true;
}

void InferenceContext::set_default_key(std::string_view key) {
  default_key_id = strings.intern(key);
}

} // namespace sanitize::internal

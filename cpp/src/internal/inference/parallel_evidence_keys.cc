// Implements compact packet-local inference key storage.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#include "internal/inference/parallel_evidence.hh"

#include <cstdint>
#include <functional>
#include <limits>
#include <memory_resource>
#include <new>
#include <string_view>
#include <vector>

namespace sanitize::internal {

std::uint32_t
InferenceEvidenceKeys::Hash(std::string_view value) const noexcept {
  const auto wide = std::hash<std::string_view>{}(value);
  auto hash = static_cast<std::uint32_t>(wide);
  if constexpr (sizeof(wide) > sizeof(std::uint32_t)) {
    hash ^= static_cast<std::uint32_t>(wide >> 32U);
  }
  return hash == 0U ? 1U : hash;
}

/// Returns a non-owning view of the interned key bytes at the requested index.
std::string_view
InferenceEvidenceKeys::View(std::uint32_t index) const noexcept {
  if (index >= entries_.size()) {
    return {};
  }
  const auto &entry = entries_[index];
  const auto offset = static_cast<std::size_t>(entry.offset);
  const auto size = static_cast<std::size_t>(entry.size);
  if (offset > bytes_.size() || size > bytes_.size() - offset) {
    return {};
  }
  if (size == 0U) {
    return {};
  }
  return std::string_view(bytes_.data() + offset, size);
}

/// Returns the stored key bytes for an interned identifier after bounds
/// validation.
StrId InferenceEvidenceKeys::Resolve(std::uint32_t index,
                                     StringInterner *strings) const {
  if (index >= entries_.size() || strings == nullptr) {
    return 0;
  }
  auto &entry = entries_[index];
  if (entry.resolved_id == std::numeric_limits<StrId>::max()) {
    entry.resolved_id = strings->intern(View(index));
  }
  return entry.resolved_id;
}

void InferenceEvidenceKeys::InsertSlot(
    std::pmr::vector<std::uint32_t> *slots,
    std::uint32_t entry_index) const noexcept {
  const auto mask = slots->size() - 1U;
  auto position = static_cast<std::size_t>(entries_[entry_index].hash) & mask;
  while ((*slots)[position] != 0U) {
    position = (position + 1U) & mask;
  }
  (*slots)[position] = entry_index + 1U;
}

sanitize::Status
InferenceEvidenceKeys::EnsureCapacity(std::size_t required_entries) {
  if (required_entries > std::numeric_limits<std::size_t>::max() / 2U) {
    return sanitize::Status::OutOfMemory(
        "inference evidence key table is too large");
  }
  const auto required_slots = required_entries * 2U;
  std::size_t desired = 16U;
  while (desired < required_slots) {
    if (desired > std::numeric_limits<std::size_t>::max() / 2U) {
      return sanitize::Status::OutOfMemory(
          "inference evidence key table is too large");
    }
    desired *= 2U;
  }
  if (slots_.size() >= desired) {
    return sanitize::Status::OK();
  }
  try {
    std::pmr::vector<std::uint32_t> replacement(slots_.get_allocator());
    replacement.assign(desired, 0U);
    for (std::uint32_t index = 0; index < entries_.size(); ++index) {
      InsertSlot(&replacement, index);
    }
    slots_.swap(replacement);
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "inference evidence key table allocation failed");
  }
  return sanitize::Status::OK();
}

std::uint32_t InferenceEvidenceKeys::Find(std::string_view value,
                                          std::uint32_t hash) const noexcept {
  if (slots_.empty()) {
    return std::numeric_limits<std::uint32_t>::max();
  }
  const auto mask = slots_.size() - 1U;
  auto position = static_cast<std::size_t>(hash) & mask;
  while (true) {
    const auto slot = slots_[position];
    if (slot == 0U) {
      return std::numeric_limits<std::uint32_t>::max();
    }
    const auto index = slot - 1U;
    const auto &entry = entries_[index];
    if (entry.hash == hash && entry.size == value.size() &&
        View(index) == value) {
      return index;
    }
    position = (position + 1U) & mask;
  }
}

/// Returns a stable packet-local identifier for the key, reusing existing
/// storage when possible.
sanitize::Result<std::uint32_t>
InferenceEvidenceKeys::Intern(std::string_view value) {
  const auto hash = Hash(value);
  const auto existing = Find(value, hash);
  if (existing != std::numeric_limits<std::uint32_t>::max()) {
    return existing;
  }
  if (entries_.size() >= std::numeric_limits<std::uint32_t>::max() ||
      value.size() > std::numeric_limits<std::uint32_t>::max() ||
      bytes_.size() >
          std::numeric_limits<std::uint32_t>::max() - value.size()) {
    return sanitize::Status::OutOfMemory(
        "inference evidence key storage exceeds 32-bit bounds");
  }
  SAN_RETURN_NOT_OK(EnsureCapacity(entries_.size() + 1U));
  const auto offset = bytes_.size();
  try {
    bytes_.insert(bytes_.end(), value.begin(), value.end());
    entries_.push_back(Entry{
        .hash = hash,
        .offset = static_cast<std::uint32_t>(offset),
        .size = static_cast<std::uint32_t>(value.size()),
    });
  } catch (const std::bad_alloc &) {
    bytes_.resize(offset);
    return sanitize::Status::OutOfMemory(
        "inference evidence key storage allocation failed");
  }
  const auto index = static_cast<std::uint32_t>(entries_.size() - 1U);
  InsertSlot(&slots_, index);
  return index;
}

} // namespace sanitize::internal

// Provides compact string interning for planning and inference state.

#pragma once

#include <cstdint>
#include <deque>
#include <limits>
#include <memory_resource>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace sanitize::detail {

using StrId = std::uint32_t;
using PathId = std::uint32_t;

class StringInterner {
public:
  // Creates a StringInterner whose dynamic storage uses resource.
  explicit StringInterner(
      std::pmr::memory_resource *resource = std::pmr::get_default_resource())
      : storage_(resource ? resource : std::pmr::get_default_resource()),
        map_(resource ? resource : std::pmr::get_default_resource()) {
    storage_.emplace_back(); // reserve 0 for "" (root)
    map_.emplace(std::string_view(storage_.back()), 0);
  }

  // Interns a string and returns its stable identifier.
  StrId intern(std::string_view value) {
    if (const auto found = map_.find(value); found != map_.end()) {
      return found->second;
    }
    if (storage_.size() >=
        static_cast<std::size_t>(std::numeric_limits<StrId>::max())) {
      throw std::length_error("StringInterner identifier space exhausted");
    }
    const auto id = static_cast<StrId>(storage_.size());
    storage_.emplace_back(value);
    const std::string_view stored(storage_.back());
    try {
      const auto [position, inserted] = map_.emplace(stored, id);
      if (!inserted) {
        storage_.pop_back();
        return position->second;
      }
    } catch (...) {
      storage_.pop_back();
      throw;
    }
    return id;
  }

  // Returns the string for an interned identifier.
  [[nodiscard]] std::string_view str(StrId id) const noexcept {
    if (id >= storage_.size()) {
      return {};
    }
    return std::string_view(storage_[id]);
  }

private:
  // deque keeps references to existing strings stable as new keys are added
  // and avoids one separately allocated string object per interned value.
  std::pmr::deque<std::pmr::string> storage_;
  std::pmr::unordered_map<std::string_view, StrId> map_;
};

class PathInterner {
public:
  // Creates a PathInterner whose dynamic storage uses resource.
  explicit PathInterner(
      std::pmr::memory_resource *resource = std::pmr::get_default_resource())
      : nodes_(resource ? resource : std::pmr::get_default_resource()),
        map_(resource ? resource : std::pmr::get_default_resource()) {
    nodes_.push_back(Node{.parent = 0, .comp = 0});
  }

  // Returns the root identifier.
  [[nodiscard]] static PathId root() noexcept { return 0; }

  // Interns a child path and returns its stable identifier.
  PathId child(PathId parent, StrId comp) {
    const std::uint64_t key = (static_cast<std::uint64_t>(parent) << 32U) |
                              static_cast<std::uint64_t>(comp);
    if (const auto found = map_.find(key); found != map_.end()) {
      return found->second;
    }
    if (nodes_.size() >=
        static_cast<std::size_t>(std::numeric_limits<PathId>::max())) {
      throw std::length_error("PathInterner identifier space exhausted");
    }
    const auto id = static_cast<PathId>(nodes_.size());
    nodes_.push_back(Node{.parent = parent, .comp = comp});
    try {
      const auto [position, inserted] = map_.emplace(key, id);
      if (!inserted) {
        nodes_.pop_back();
        return position->second;
      }
    } catch (...) {
      nodes_.pop_back();
      throw;
    }
    return id;
  }

private:
  struct Node {
    PathId parent;
    StrId comp;
  };

  std::pmr::vector<Node> nodes_;
  std::pmr::unordered_map<std::uint64_t, PathId> map_;
};

} // namespace sanitize::detail

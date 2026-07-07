// Provides compact string interning for planning and inference state.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace sanitize::detail {

using StrId = uint32_t;
using PathId = uint32_t;

class StringInterner {
public:
  // Creates a StringInterner.
  StringInterner() {
    // reserve 0 for "" (root)
    storage_.push_back(std::make_unique<std::string>());
    map_.emplace(std::string_view(*storage_.back()), 0);
  }

  // Interns a string and returns its stable identifier.
  StrId intern(std::string_view s) {
    auto it = map_.find(s);
    if (it != map_.end())
      return it->second;
    auto id = static_cast<StrId>(storage_.size());
    storage_.push_back(std::make_unique<std::string>(s));
    std::string_view sv(*storage_.back());
    map_.emplace(sv, id);
    return id;
  }

  // Returns the string for an interned identifier.
  [[nodiscard]] std::string_view str(StrId id) const {
    if (id >= storage_.size())
      return {};
    return std::string_view(*storage_[id]);
  }

private:
  std::unordered_map<std::string_view, StrId, std::hash<std::string_view>,
                     std::equal_to<>>
      map_;
  std::vector<std::unique_ptr<std::string>> storage_;
};

class PathInterner {
public:
  // Creates a PathInterner.
  PathInterner() { nodes_.push_back(Node{.parent = 0, .comp = 0}); }

  // Returns the root identifier.
  [[nodiscard]] static PathId root() { return 0; }

  // Interns a child path and returns its stable identifier.
  PathId child(PathId parent, StrId comp) {
    uint64_t k =
        (static_cast<uint64_t>(parent) << 32) | static_cast<uint64_t>(comp);
    auto it = map_.find(k);
    if (it != map_.end())
      return it->second;
    auto id = static_cast<PathId>(nodes_.size());
    nodes_.push_back(Node{.parent = parent, .comp = comp});
    map_.emplace(k, id);
    return id;
  }

private:
  struct Node {
    PathId parent;
    StrId comp;
  };

  std::vector<Node> nodes_;
  std::unordered_map<uint64_t, PathId> map_;
};

} // namespace sanitize::detail

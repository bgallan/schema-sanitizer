// Declares root-field filtering for materialized JSON text rows.

#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

class JsonRootFieldFilter {
public:
  // Installs the compiled root layout and field-name policy.
  void reset(const CompiledPlan *plan, std::string_view field_name_policy);

  // Returns whether a source key can address any planned root field.
  [[nodiscard]] bool accepts(std::string_view key, uint64_t key_hash) const;

private:
  struct StringViewHash {
    using is_transparent = void;

    // Hashes string-like keys without forcing allocation for string_view
    // probes.
    [[nodiscard]] std::size_t operator()(std::string_view value) const noexcept;

    // Hashes owned string keys stored in the fallback map.
    [[nodiscard]] std::size_t
    operator()(const std::string &value) const noexcept;
  };

  static constexpr std::size_t kVectorCacheLimit = 32;

  const CompiledPlan *plan_ = nullptr;
  std::string field_name_policy_;
  mutable std::vector<std::pair<std::string, bool>> cache_;
  mutable std::unordered_map<std::string, bool, StringViewHash, std::equal_to<>>
      cache_map_;
};

} // namespace sanitize::internal

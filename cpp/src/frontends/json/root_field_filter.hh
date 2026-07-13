// Declares root-field filtering for materialized JSON text rows.

#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/string_lookup.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

class JsonRootFieldFilter {
public:
  // Installs the compiled root layout and field-name policy.
  void reset(const CompiledPlan *plan, std::string_view field_name_policy);

  // Returns whether a source key can address any planned root field.
  [[nodiscard]] bool accepts(std::string_view key, uint64_t key_hash) const;

private:
  static constexpr std::size_t kVectorCacheLimit = 32;

  const CompiledPlan *plan_ = nullptr;
  std::string field_name_policy_;
  mutable std::vector<std::pair<std::string, bool>> cache_;
  mutable StringLookupMap<bool> cache_map_;
};

} // namespace sanitize::internal

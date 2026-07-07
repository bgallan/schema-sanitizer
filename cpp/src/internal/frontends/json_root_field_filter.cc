// Implements cached root-field filtering for JSON text frontends.

#include "internal/frontends/json_root_field_filter.hh"

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

#include "internal/planning/planned_name_matcher.hh"

namespace sanitize::internal {

std::size_t JsonRootFieldFilter::StringViewHash::operator()(
    std::string_view value) const noexcept {
  return std::hash<std::string_view>{}(value);
}

std::size_t JsonRootFieldFilter::StringViewHash::operator()(
    const std::string &value) const noexcept {
  return (*this)(std::string_view(value));
}

void JsonRootFieldFilter::reset(const CompiledPlan *plan,
                                std::string_view field_name_policy) {
  plan_ = plan;
  field_name_policy_.assign(field_name_policy.data(), field_name_policy.size());
  cache_.clear();
  cache_map_.clear();
  cache_.reserve(kVectorCacheLimit);
}

bool JsonRootFieldFilter::accepts(std::string_view key,
                                  uint64_t key_hash) const {
  if (!plan_) {
    return true;
  }
  if (!cache_map_.empty()) {
    const auto iter = cache_map_.find(key);
    if (iter != cache_map_.end()) {
      return iter->second;
    }
  }
  for (const auto &entry : cache_) {
    if (entry.first == key) {
      return entry.second;
    }
  }

  const bool matched = matches_planned_field(
      plan_->root_layout, key, key_hash, std::string_view(field_name_policy_));
  if (cache_.size() < kVectorCacheLimit) {
    cache_.emplace_back(std::string(key), matched);
  } else {
    if (cache_map_.empty()) {
      cache_map_.reserve(kVectorCacheLimit * 2);
      for (const auto &entry : cache_) {
        cache_map_.emplace(entry.first, entry.second);
      }
    }
    cache_map_.emplace(std::string(key), matched);
  }
  return matched;
}

} // namespace sanitize::internal

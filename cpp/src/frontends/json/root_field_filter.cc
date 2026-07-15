// Implements cached root-field filtering for JSON text frontends.

#include "frontends/json/root_field_filter.hh"

#include <cstddef>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "internal/planning/planned_name_matcher.hh"

namespace sanitize::internal {

void JsonRootFieldFilter::reset(const CompiledPlan *plan,
                                std::string_view field_name_policy) {
  plan_ = plan;
  field_name_policy_.assign(field_name_policy.data(), field_name_policy.size());
  std::vector<std::pair<std::string, bool>> empty_vector;
  cache_.swap(empty_vector);
  cache_.reserve(kVectorCacheLimit);
  StringLookupMap<bool> empty_map;
  cache_map_.swap(empty_map);
  cache_key_bytes_ = 0;
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
  if (cache_key_bytes_ >= kCacheKeyByteLimit ||
      key.size() > kCacheKeyByteLimit - cache_key_bytes_) {
    return matched;
  }

  try {
    if (cache_map_.empty() && cache_.size() < kVectorCacheLimit) {
      cache_.emplace_back(std::string(key), matched);
      cache_key_bytes_ += key.size();
      return matched;
    }

    if (cache_map_.empty()) {
      StringLookupMap<bool> promoted;
      promoted.reserve(kVectorCacheLimit * 2U);
      for (const auto &entry : cache_) {
        promoted.emplace(entry.first, entry.second);
      }
      cache_map_.swap(promoted);
      std::vector<std::pair<std::string, bool>> empty;
      cache_.swap(empty);
    }

    if (cache_map_.size() < kMapCacheEntryLimit) {
      const auto [iter, inserted] =
          cache_map_.emplace(std::string(key), matched);
      (void)iter;
      if (inserted) {
        cache_key_bytes_ += key.size();
      }
    }
  } catch (const std::bad_alloc &) {
    // Filtering remains correct when the optional cache cannot grow.
  } catch (const std::length_error &) {
    // Oversized keys are evaluated without being retained by the cache.
  }
  return matched;
}

} // namespace sanitize::internal

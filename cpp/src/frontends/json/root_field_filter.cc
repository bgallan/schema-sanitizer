// Implements cached root-field filtering for JSON text frontends. The pipeline
// preserves source offsets and ownership while enforcing plan order and memory
// bounds.

#include "frontends/json/root_field_filter.hh"

#include <cstddef>
#include <memory>
#include <memory_resource>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>

#include "internal/planning/planned_name_matcher.hh"

namespace sanitize::internal {

/// Initializes an empty root-field cache ready for the first compiled plan.
JsonRootFieldFilter::JsonRootFieldFilter() { rebuild_cache(); }

void JsonRootFieldFilter::set_memory_pool(std::shared_ptr<void> pool) noexcept {
  memory_pool_ = std::move(pool);
  rebuild_cache();
}

void JsonRootFieldFilter::rebuild_cache() noexcept {
  cache_key_bytes_ = 0;
  try {
    auto state = std::make_unique<CacheState>(memory_pool_);
    state->cache.reserve(kVectorCacheLimit);
    cache_state_ = std::move(state);
  } catch (const std::bad_alloc &) {
    cache_state_.reset();
  } catch (const std::length_error &) {
    cache_state_.reset();
  }
}

void JsonRootFieldFilter::reset(const CompiledPlan *plan,
                                std::string_view field_name_policy) {
  plan_ = plan;
  field_name_policy_.assign(field_name_policy.data(), field_name_policy.size());
  rebuild_cache();
}

bool JsonRootFieldFilter::accepts(std::string_view key,
                                  uint64_t key_hash) const {
  if (!plan_) {
    return true;
  }
  auto *state = cache_state_.get();
  if (state && !state->cache_map.empty()) {
    const auto iter = state->cache_map.find(key);
    if (iter != state->cache_map.end()) {
      return iter->second;
    }
  }
  if (state) {
    for (const auto &entry : state->cache) {
      if (entry.first == key) {
        return entry.second;
      }
    }
  }

  const bool matched = matches_planned_field(
      plan_->root_layout, key, key_hash, std::string_view(field_name_policy_));
  if (!state || cache_key_bytes_ >= kCacheKeyByteLimit ||
      key.size() > kCacheKeyByteLimit - cache_key_bytes_) {
    return matched;
  }

  try {
    if (state->cache_map.empty() && state->cache.size() < kVectorCacheLimit) {
      state->cache.emplace_back(std::pmr::string(key, &state->resource),
                                matched);
      cache_key_bytes_ += key.size();
      return matched;
    }

    if (state->cache_map.empty()) {
      CacheMap promoted(&state->resource);
      promoted.reserve(kVectorCacheLimit * 2U);
      for (const auto &entry : state->cache) {
        promoted.emplace(entry.first, entry.second);
      }
      state->cache_map.swap(promoted);
      std::pmr::vector<CacheEntry> empty(&state->resource);
      state->cache.swap(empty);
    }

    if (state->cache_map.size() < kMapCacheEntryLimit) {
      const auto [iter, inserted] = state->cache_map.emplace(
          std::pmr::string(key, &state->resource), matched);
      (void)iter;
      if (inserted) {
        cache_key_bytes_ += key.size();
      }
    }
  } catch (const std::bad_alloc &) {
    // Filtering remains correct when the operation-budget cache cannot grow.
  } catch (const std::length_error &) {
    // Oversized keys are evaluated without being retained by the cache.
  }
  return matched;
}

} // namespace sanitize::internal

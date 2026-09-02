// Declares root-field filtering for materialized JSON text rows. The pipeline
// preserves source offsets and ownership while enforcing plan order and memory
// bounds.

#pragma once

#include <cstddef>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "internal/memory/pool_resource.hh"
#include "internal/string_lookup.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

class JsonRootFieldFilter {
public:
  JsonRootFieldFilter();

  /// Rebinds optional cache allocations to the operation-wide memory pool.
  void set_memory_pool(std::shared_ptr<void> pool) noexcept;

  /// Installs the compiled root layout and field-name policy.
  void reset(const CompiledPlan *plan, std::string_view field_name_policy);

  /// Returns whether a source key can address any planned root field.
  [[nodiscard]] bool accepts(std::string_view key, uint64_t key_hash) const;

private:
  static constexpr std::size_t kVectorCacheLimit = 32;
  static constexpr std::size_t kMapCacheEntryLimit = 4096;
  static constexpr std::size_t kCacheKeyByteLimit = 1U << 20;

  using CacheEntry = std::pair<std::pmr::string, bool>;
  using CacheMap =
      std::pmr::unordered_map<std::pmr::string, bool, TransparentStringHash,
                              std::equal_to<>>;

  struct CacheState {

    /// Creates pool-backed caches for root-field lookups in one compiled plan.
    explicit CacheState(std::shared_ptr<void> pool)
        : resource(std::move(pool)), cache(&resource), cache_map(&resource) {}

    PoolResource resource;
    std::pmr::vector<CacheEntry> cache;
    CacheMap cache_map;
  };

  /// Recreates empty pool-backed caches, disabling caching if allocation fails.
  void rebuild_cache() noexcept;

  const CompiledPlan *plan_ = nullptr;
  std::string field_name_policy_;
  std::shared_ptr<void> memory_pool_;
  mutable std::unique_ptr<CacheState> cache_state_;
  mutable std::size_t cache_key_bytes_ = 0;
};

} // namespace sanitize::internal

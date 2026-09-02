// Declares the std::pmr adapter for native memory pools. Constructors
// control pool lifetime and optional exact-block recycling for
// container metadata.

#pragma once

#include <cstddef>
#include <memory>
#include <memory_resource>

namespace sanitize::internal {

// std::pmr::memory_resource adapter backed by the internal MemoryPool.
//
// Alignment is best-effort; the default pool provides at least 64-byte
// alignment.
class PoolResource final : public std::pmr::memory_resource {
public:
  /// Adapts a raw pool handle without extending its lifetime or
  /// recycling blocks.
  explicit PoolResource(void *pool_handle = nullptr);
  /// Keeps the supplied pool alive and disables exact-block recycling.
  explicit PoolResource(std::shared_ptr<void> pool_keepalive);
  /// Keeps the supplied pool alive and optionally recycles exact-size blocks.
  PoolResource(std::shared_ptr<void> pool_keepalive, bool recycle_exact_blocks);
  /// Configures pool lifetime and a bounded exact-block recycling cache.
  PoolResource(std::shared_ptr<void> pool_keepalive, bool recycle_exact_blocks,
               std::size_t max_cached_bytes);
  /// Flushes cached blocks before releasing the backing pool lifetime.
  ~PoolResource() override;

  /// Returns the opaque pool handle used by this resource.
  [[nodiscard]] void *pool() const noexcept { return pool_handle_; }

protected:
  /// Allocates through the backing pool or its bounded exact-size cache.
  void *do_allocate(std::size_t bytes, std::size_t alignment) override;
  /// Caches an eligible block or returns it directly to the backing pool.
  void do_deallocate(void *p, std::size_t bytes,
                     std::size_t alignment) noexcept override;
  /// Compares uncached resources by their underlying pool handle.
  [[nodiscard]] bool
  do_is_equal(const std::pmr::memory_resource &other) const noexcept override;

private:
  struct CacheState;

  std::shared_ptr<void> pool_keepalive_;
  void *pool_handle_ = nullptr;
  std::unique_ptr<CacheState> cache_;
};

} // namespace sanitize::internal

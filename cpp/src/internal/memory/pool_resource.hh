// Declares std::pmr resource adapters for native memory pools.

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
  // Creates a PoolResource.
  explicit PoolResource(void *pool_handle = nullptr);
  explicit PoolResource(std::shared_ptr<void> pool_keepalive);

  // Returns the opaque pool handle used by this resource.
  [[nodiscard]] void *pool() const noexcept { return pool_handle_; }

protected:
  // Allocates a block through the underlying MemoryPool.
  void *do_allocate(std::size_t bytes, std::size_t alignment) override;
  // Returns a block to the underlying MemoryPool.
  void do_deallocate(void *p, std::size_t bytes,
                     std::size_t alignment) noexcept override;
  // Compares resources by their underlying pool handle.
  [[nodiscard]] bool
  do_is_equal(const std::pmr::memory_resource &other) const noexcept override;

private:
  std::shared_ptr<void> pool_keepalive_;
  void *pool_handle_ = nullptr;
};

} // namespace sanitize::internal

// Adapts the project memory pool interface to std::pmr resources.

#include "internal/memory/pool_resource.hh"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <utility>

#include "internal/memory/memory_pool.hh"

namespace sanitize::internal {

PoolResource::PoolResource(void *pool_handle)
    : pool_handle_(pool_handle ? pool_handle
                               : static_cast<void *>(default_memory_pool())) {}

PoolResource::PoolResource(std::shared_ptr<void> pool_keepalive)
    : pool_keepalive_(std::move(pool_keepalive)),
      pool_handle_(pool_keepalive_
                       ? pool_keepalive_.get()
                       : static_cast<void *>(default_memory_pool())) {}

void *PoolResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  if (bytes == 0) {
    return nullptr;
  }
  if (bytes > static_cast<std::size_t>(std::numeric_limits<int64_t>::max()) ||
      alignment >
          static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
    throw std::bad_alloc();
  }
  auto *pool = memory_pool_from_handle(pool_handle_);
  uint8_t *out = nullptr;
  const auto st = pool->Allocate(static_cast<int64_t>(bytes),
                                 static_cast<int64_t>(alignment), &out);
  if (!st.ok()) {
    throw std::bad_alloc();
  }
  return out;
}

void PoolResource::do_deallocate(void *p, std::size_t bytes,
                                 std::size_t alignment) noexcept {
  if (!p) {
    return;
  }
  if (bytes > static_cast<std::size_t>(std::numeric_limits<int64_t>::max()) ||
      alignment >
          static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
    return;
  }
  auto *pool = memory_pool_from_handle(pool_handle_);
  pool->Free(reinterpret_cast<uint8_t *>(p), static_cast<int64_t>(bytes),
             static_cast<int64_t>(alignment));
}

bool PoolResource::do_is_equal(
    const std::pmr::memory_resource &other) const noexcept {
  if (this == &other) {
    return true;
  }
  const auto *o = dynamic_cast<const PoolResource *>(&other);
  return o && o->pool_handle_ == pool_handle_;
}

} // namespace sanitize::internal

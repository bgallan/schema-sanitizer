// Declares native memory pool handles and allocation accounting.

#pragma once

#include <cstdint>
#include <string>

#include "sanitize/core/status.hh"

namespace sanitize::internal {

// Minimal allocator interface modeled after Arrow's MemoryPool API surface
// needed by schema-sanitizer internals.
class MemoryPool {
public:
  // Destroys the MemoryPool.
  virtual ~MemoryPool() = default;

  // Allocates the object state.
  virtual sanitize::Status Allocate(int64_t size, int64_t alignment,
                                    uint8_t **out) = 0;

  // Convenience overload used by existing call sites.
  sanitize::Status Allocate(int64_t size, uint8_t **out) {
    return Allocate(size, 64, out);
  }

  // Frees the object state.
  virtual void Free(uint8_t *buffer, int64_t size, int64_t alignment) = 0;

  // Convenience overload used by existing call sites.
  void Free(uint8_t *buffer, int64_t size) { Free(buffer, size, 64); }

  // Returns the number of bytes currently owned by the pool.
  [[nodiscard]] virtual int64_t bytes_allocated() const = 0;
  // Returns the peak allocation size, or -1 when it is not tracked.
  [[nodiscard]] virtual int64_t max_memory() const { return -1; }
  // Returns a stable human-readable allocator backend name.
  [[nodiscard]] virtual std::string backend_name() const = 0;
};

// Default process-wide pool.
MemoryPool *default_memory_pool() noexcept;

// Resolves an optional opaque pool handle to a usable pool.
inline MemoryPool *memory_pool_from_handle(void *handle) noexcept {
  return handle ? static_cast<MemoryPool *>(handle) : default_memory_pool();
}

} // namespace sanitize::internal

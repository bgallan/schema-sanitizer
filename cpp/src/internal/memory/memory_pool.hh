// Declares native memory pool handles and allocation accounting.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "sanitize/core/status.hh"

namespace sanitize::internal {

// Minimal allocator interface modeled after Arrow's MemoryPool API surface
// needed by schema-sanitizer internals.
class MemoryPool {
public:
  virtual ~MemoryPool() = default;

  virtual sanitize::Status Allocate(int64_t size, int64_t alignment,
                                    uint8_t **out) = 0;

  sanitize::Status Allocate(int64_t size, uint8_t **out) {
    return Allocate(size, 64, out);
  }

  virtual void Free(uint8_t *buffer, int64_t size,
                    int64_t alignment) noexcept = 0;

  void Free(uint8_t *buffer, int64_t size) noexcept { Free(buffer, size, 64); }

  [[nodiscard]] virtual int64_t bytes_allocated() const = 0;
  [[nodiscard]] virtual int64_t max_memory() const { return -1; }
  [[nodiscard]] virtual int64_t allocation_count() const { return -1; }
  [[nodiscard]] virtual int64_t invalid_free_count() const { return 0; }
  [[nodiscard]] virtual int64_t size_mismatch_count() const { return 0; }
  [[nodiscard]] virtual int64_t corruption_count() const { return 0; }
  [[nodiscard]] virtual int64_t limit_bytes() const { return -1; }
  [[nodiscard]] virtual bool wipes_memory_on_free() const noexcept {
    return false;
  }
  [[nodiscard]] virtual std::string backend_name() const = 0;
};

// Returns whether best-effort cleanup of sensitive scratch memory is enabled.
[[nodiscard]] bool secure_memory_cleanup_enabled() noexcept;

// Overwrites a caller-owned byte range using volatile stores.
void secure_zero_memory(void *data, std::size_t size) noexcept;

// Default process-wide pool.
MemoryPool *default_memory_pool() noexcept;

// Returns a non-owning shared handle to the process-wide pool.
std::shared_ptr<MemoryPool> shared_default_memory_pool();

// Creates an accounting pool layered over parent. A positive limit rejects an
// allocation before it reaches the system allocator when the operation quota
// would be exceeded. The returned pool stores allocation sizes in private
// headers, so deallocation accounting does not trust caller-supplied sizes.
std::shared_ptr<MemoryPool>
make_tracking_memory_pool(std::shared_ptr<MemoryPool> parent, int64_t limit,
                          std::string backend_name);

inline MemoryPool *memory_pool_from_handle(void *handle) noexcept {
  return handle ? static_cast<MemoryPool *>(handle) : default_memory_pool();
}

} // namespace sanitize::internal

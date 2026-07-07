// Provides the process-wide memory pool used by native allocation paths.

#include "internal/memory/memory_pool.hh"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>

#if defined(_WIN32)
#include <malloc.h>
#endif

#include "sanitize/core/status.hh"

namespace sanitize::internal {

namespace {

// Normalizes a requested allocation alignment to a usable power of two.
static bool normalize_alignment(int64_t alignment, std::size_t *out) noexcept {
  if (!out) {
    return false;
  }
  if (alignment <= 0) {
    *out = alignof(std::max_align_t);
    return true;
  }
  auto a = static_cast<std::size_t>(alignment);
  // Require power of two.
  if ((a & (a - 1)) != 0) {
    const int digits = std::numeric_limits<std::size_t>::digits;
    const std::size_t max_power = std::size_t{1} << (digits - 1);
    if (a > max_power) {
      return false;
    }
    // Round up to next power of two.
    std::size_t p = 1;
    while (p < a)
      p <<= 1;
    a = p;
  }
  a = std::max<std::size_t>(a, alignof(std::max_align_t));
  *out = a;
  return true;
}

// Allocates aligned memory without using C++17 aligned operator new. Apple only
// exposes those operators for macOS 10.13+, while abi3 wheels may target older
// deployment versions.
static void *allocate_aligned(std::size_t size,
                              std::size_t alignment) noexcept {
#if defined(_WIN32)
  return _aligned_malloc(size, alignment);
#else
  void *ptr = nullptr;
  if (posix_memalign(&ptr, alignment, size) != 0) {
    return nullptr;
  }
  return ptr;
#endif
}

// Frees memory returned by allocate_aligned.
static void free_aligned(void *ptr) noexcept {
#if defined(_WIN32)
  _aligned_free(ptr);
#else
  std::free(ptr);
#endif
}

class DefaultMemoryPool final : public MemoryPool {
public:
  // Allocates the object state.
  sanitize::Status Allocate(int64_t size, int64_t alignment,
                            uint8_t **out) override {
    if (!out) {
      return sanitize::Status::Invalid("Allocate: out is null");
    }
    if (size < 0) {
      return sanitize::Status::Invalid("Allocate: negative size");
    }
    if (size == 0) {
      *out = nullptr;
      return sanitize::Status::OK();
    }

    std::size_t a = alignof(std::max_align_t);
    if (!normalize_alignment(alignment, &a)) {
      *out = nullptr;
      return sanitize::Status::OutOfMemory("Allocate: alignment out of range");
    }
    void *p = allocate_aligned(static_cast<std::size_t>(size), a);
    if (!p) {
      *out = nullptr;
      return sanitize::Status::OutOfMemory("Allocate: out of memory");
    }
    *out = static_cast<uint8_t *>(p);
    bytes_allocated_.fetch_add(size, std::memory_order_relaxed);
    return sanitize::Status::OK();
  }

  // Frees the object state.
  void Free(uint8_t *buffer, int64_t size, int64_t alignment) override {
    if (!buffer || size <= 0) {
      return;
    }
    std::size_t a = alignof(std::max_align_t);
    if (!normalize_alignment(alignment, &a)) {
      return;
    }
    free_aligned(buffer);
    bytes_allocated_.fetch_sub(size, std::memory_order_relaxed);
  }

  // Returns the current allocation count without synchronization overhead.
  [[nodiscard]] int64_t bytes_allocated() const override {
    return bytes_allocated_.load(std::memory_order_relaxed);
  }

  // Identifies the built-in process-wide allocator.
  [[nodiscard]] std::string backend_name() const override {
    return {"schema_sanitizer::DefaultMemoryPool"};
  }

private:
  std::atomic<int64_t> bytes_allocated_{0};
};

DefaultMemoryPool g_default_pool;

} // namespace

MemoryPool *default_memory_pool() noexcept { return &g_default_pool; }

} // namespace sanitize::internal

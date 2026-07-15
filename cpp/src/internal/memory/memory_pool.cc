// Provides hardened process-wide and operation-scoped memory pools.

#include "internal/memory/memory_pool.hh"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include "sanitize/core/status.hh"

namespace sanitize::internal {
namespace {

constexpr std::uint64_t kDefaultAllocationMagic = 0x53414E4D454D3031ULL;
constexpr std::uint64_t kTrackingAllocationMagic = 0x53414E4D454D3032ULL;
constexpr std::size_t kAllocationGuardBytes = 16;
constexpr std::uint8_t kAllocationGuardPattern = 0xA5;

#include "internal/memory/memory_pool_registry.cc.inc"

struct alignas(std::max_align_t) DefaultAllocationHeader {
  void *raw = nullptr;
  std::uint64_t magic = kDefaultAllocationMagic;
  std::uint64_t reserved = 0;
  std::int64_t size = 0;
  std::size_t alignment = 0;
};

struct alignas(std::max_align_t) TrackingAllocationHeader {
  std::uint8_t *upstream = nullptr;
  std::uint64_t magic = kTrackingAllocationMagic;
  std::int64_t upstream_size = 0;
  std::int64_t requested_size = 0;
  std::int64_t upstream_alignment = 0;
};

static_assert(sizeof(DefaultAllocationHeader) % alignof(std::max_align_t) == 0);
static_assert(sizeof(TrackingAllocationHeader) % alignof(std::max_align_t) ==
              0);

bool normalize_alignment(int64_t alignment, std::size_t *out) noexcept {
  if (!out) {
    return false;
  }
  if (alignment <= 0) {
    *out = alignof(std::max_align_t);
    return true;
  }
  if (static_cast<std::uint64_t>(alignment) >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return false;
  }
  auto a = static_cast<std::size_t>(alignment);
  if ((a & (a - 1)) != 0) {
    const int digits = std::numeric_limits<std::size_t>::digits;
    const std::size_t max_power = std::size_t{1} << (digits - 1);
    if (a > max_power) {
      return false;
    }
    std::size_t p = 1;
    while (p < a) {
      p <<= 1;
    }
    a = p;
  }
  *out = std::max<std::size_t>(a, alignof(std::max_align_t));
  return true;
}

bool checked_allocation_size(std::size_t requested, std::size_t alignment,
                             std::size_t header_size,
                             std::size_t *total) noexcept {
  if (!total || alignment == 0 ||
      requested > std::numeric_limits<std::size_t>::max() - header_size) {
    return false;
  }
  const auto with_header = requested + header_size;
  if (with_header >
          std::numeric_limits<std::size_t>::max() - (alignment - 1U) ||
      with_header + (alignment - 1U) >
          std::numeric_limits<std::size_t>::max() - kAllocationGuardBytes) {
    return false;
  }
  *total = with_header + alignment - 1U + kAllocationGuardBytes;
  return true;
}

void write_allocation_guard(std::uint8_t *buffer, std::size_t size) noexcept {
  std::memset(buffer + size, kAllocationGuardPattern, kAllocationGuardBytes);
}

bool allocation_guard_is_valid(const std::uint8_t *buffer,
                               std::size_t size) noexcept {
  for (std::size_t index = 0; index < kAllocationGuardBytes; ++index) {
    if (buffer[size + index] != kAllocationGuardPattern) {
      return false;
    }
  }
  return true;
}

std::uintptr_t align_up(std::uintptr_t value, std::size_t alignment) noexcept {
  return (value + alignment - 1U) &
         ~(static_cast<std::uintptr_t>(alignment) - 1U);
}

void update_peak(std::atomic<int64_t> *peak, int64_t current) noexcept {
  auto observed = peak->load(std::memory_order_relaxed);
  while (current > observed && !peak->compare_exchange_weak(
                                   observed, current, std::memory_order_relaxed,
                                   std::memory_order_relaxed)) {
  }
}

class DefaultMemoryPool final : public MemoryPool {
public:
  sanitize::Status Allocate(int64_t size, int64_t alignment,
                            uint8_t **out) override {
    if (!out) {
      return sanitize::Status::Invalid("Allocate: out is null");
    }
    *out = nullptr;
    if (size < 0) {
      return sanitize::Status::Invalid("Allocate: negative size");
    }
    if (size == 0) {
      return sanitize::Status::OK();
    }
    if (static_cast<std::uint64_t>(size) >
        static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
      return sanitize::Status::OutOfMemory("Allocate: size out of range");
    }

    std::size_t normalized_alignment = alignof(std::max_align_t);
    if (!normalize_alignment(alignment, &normalized_alignment)) {
      return sanitize::Status::OutOfMemory("Allocate: alignment out of range");
    }
    std::size_t total = 0;
    if (!checked_allocation_size(static_cast<std::size_t>(size),
                                 normalized_alignment,
                                 sizeof(DefaultAllocationHeader), &total) ||
        total > static_cast<std::size_t>(
                    std::numeric_limits<std::int64_t>::max())) {
      return sanitize::Status::OutOfMemory("Allocate: size overflow");
    }
    void *raw = std::malloc(total);
    if (!raw) {
      return sanitize::Status::OutOfMemory("Allocate: out of memory");
    }
    const auto base =
        reinterpret_cast<std::uintptr_t>(raw) + sizeof(DefaultAllocationHeader);
    const auto aligned = align_up(base, normalized_alignment);
    auto *header = reinterpret_cast<DefaultAllocationHeader *>(
        aligned - sizeof(DefaultAllocationHeader));
    *header = DefaultAllocationHeader{.raw = raw,
                                      .magic = kDefaultAllocationMagic,
                                      .reserved = 0,
                                      .size = size,
                                      .alignment = normalized_alignment};
    *out = reinterpret_cast<std::uint8_t *>(aligned);
    write_allocation_guard(*out, static_cast<std::size_t>(size));
    if (!live_allocations_.register_allocation(
            *out, LiveAllocationRecord{
                      .upstream = static_cast<std::uint8_t *>(raw),
                      .upstream_size = static_cast<std::int64_t>(total),
                      .requested_size = size,
                      .alignment =
                          static_cast<std::int64_t>(normalized_alignment)})) {
      header->magic = 0;
      header->raw = nullptr;
      std::free(raw);
      *out = nullptr;
      return sanitize::Status::OutOfMemory(
          "Allocate: unable to register hardened allocation ownership");
    }
    const auto current =
        bytes_allocated_.fetch_add(size, std::memory_order_relaxed) + size;
    allocation_count_.fetch_add(1, std::memory_order_relaxed);
    update_peak(&max_memory_, current);
    return sanitize::Status::OK();
  }

  void Free(uint8_t *buffer, int64_t size,
            int64_t alignment) noexcept override {
    if (!buffer) {
      return;
    }
    LiveAllocationRecord record;
    const bool registry_enabled = hardened_allocation_registry_enabled();
    if (!live_allocations_.claim_allocation(buffer, &record)) {
      invalid_free_count_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    auto *header = reinterpret_cast<DefaultAllocationHeader *>(
        reinterpret_cast<std::uintptr_t>(buffer) -
        sizeof(DefaultAllocationHeader));
    if (registry_enabled) {
      if (header->magic != kDefaultAllocationMagic ||
          header->raw != record.upstream ||
          header->size != record.requested_size ||
          static_cast<std::int64_t>(header->alignment) != record.alignment) {
        corruption_count_.fetch_add(1, std::memory_order_relaxed);
      }
    } else if (header->magic != kDefaultAllocationMagic || !header->raw ||
               header->size <= 0) {
      invalid_free_count_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    const auto expected_size =
        registry_enabled ? record.requested_size : header->size;
    if (size > 0 && size != expected_size) {
      size_mismatch_count_.fetch_add(1, std::memory_order_relaxed);
    }
    std::size_t normalized_alignment = 0;
    if (alignment > 0 &&
        normalize_alignment(alignment, &normalized_alignment) &&
        static_cast<std::int64_t>(normalized_alignment) !=
            (registry_enabled ? record.alignment
                              : static_cast<std::int64_t>(header->alignment))) {
      size_mismatch_count_.fetch_add(1, std::memory_order_relaxed);
    }
    void *raw = registry_enabled ? record.upstream : header->raw;
    const auto actual = registry_enabled ? record.requested_size : header->size;
    if (!allocation_guard_is_valid(buffer, static_cast<std::size_t>(actual))) {
      corruption_count_.fetch_add(1, std::memory_order_relaxed);
    }
    if (secure_memory_cleanup_enabled()) {
      secure_zero_memory(buffer, static_cast<std::size_t>(actual));
    }
    header->magic = 0;
    header->raw = nullptr;
    bytes_allocated_.fetch_sub(actual, std::memory_order_relaxed);
    allocation_count_.fetch_sub(1, std::memory_order_relaxed);
    std::free(raw);
  }

  [[nodiscard]] int64_t bytes_allocated() const override {
    return bytes_allocated_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t max_memory() const override {
    return max_memory_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t allocation_count() const override {
    return allocation_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t invalid_free_count() const override {
    return invalid_free_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t size_mismatch_count() const override {
    return size_mismatch_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t corruption_count() const override {
    return corruption_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] bool wipes_memory_on_free() const noexcept override {
    return secure_memory_cleanup_enabled();
  }
  [[nodiscard]] std::string backend_name() const override {
    return {"schema_sanitizer::DefaultMemoryPool"};
  }

private:
  std::atomic<int64_t> bytes_allocated_{0};
  std::atomic<int64_t> max_memory_{0};
  std::atomic<int64_t> allocation_count_{0};
  std::atomic<int64_t> invalid_free_count_{0};
  std::atomic<int64_t> size_mismatch_count_{0};
  std::atomic<int64_t> corruption_count_{0};
  LiveAllocationRegistry live_allocations_;
};

#include "internal/memory/tracking_memory_pool.cc.inc"

DefaultMemoryPool g_default_pool;

} // namespace

bool secure_memory_cleanup_enabled() noexcept { return true; }

void secure_zero_memory(void *data, std::size_t size) noexcept {
  if (!data) {
    return;
  }
  auto *cursor = static_cast<volatile std::uint8_t *>(data);
  while (size-- > 0) {
    *cursor++ = 0;
  }
}

MemoryPool *default_memory_pool() noexcept { return &g_default_pool; }

std::shared_ptr<MemoryPool> shared_default_memory_pool() {
  return std::shared_ptr<MemoryPool>(&g_default_pool, [](MemoryPool *) {});
}

std::shared_ptr<MemoryPool>
make_tracking_memory_pool(std::shared_ptr<MemoryPool> parent, int64_t limit,
                          std::string backend_name) {
  if (!parent) {
    parent = shared_default_memory_pool();
  }
  return std::make_shared<TrackingMemoryPool>(std::move(parent), limit,
                                              std::move(backend_name));
}

} // namespace sanitize::internal

// Adapts the project memory pool interface to std::pmr resources.

#include "internal/memory/pool_resource.hh"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <new>
#include <utility>

#include "internal/memory/memory_pool.hh"

namespace sanitize::internal {

namespace {

void *allocate_direct(void *pool_handle, std::size_t bytes,
                      std::size_t alignment) {
  if (bytes == 0) {
    return nullptr;
  }
  if (bytes > static_cast<std::size_t>(std::numeric_limits<int64_t>::max()) ||
      alignment >
          static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
    throw std::bad_alloc();
  }
  auto *pool = memory_pool_from_handle(pool_handle);
  uint8_t *out = nullptr;
  const auto status = pool->Allocate(static_cast<int64_t>(bytes),
                                     static_cast<int64_t>(alignment), &out);
  if (!status.ok()) {
    throw std::bad_alloc();
  }
  return out;
}

void deallocate_direct(void *pool_handle, void *pointer, std::size_t bytes,
                       std::size_t alignment) noexcept {
  if (!pointer ||
      bytes > static_cast<std::size_t>(std::numeric_limits<int64_t>::max()) ||
      alignment >
          static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
    return;
  }
  memory_pool_from_handle(pool_handle)
      ->Free(reinterpret_cast<uint8_t *>(pointer), static_cast<int64_t>(bytes),
             static_cast<int64_t>(alignment));
}

} // namespace

struct PoolResource::CacheState {
  static constexpr std::size_t kEntryCount = 64;
  static constexpr std::size_t kMaxBlockBytes = 1U << 20;
  static constexpr std::size_t kMaxCachedBytes = 4U << 20;

  enum class State : std::uint8_t { kEmpty, kActive, kCached };
  struct Entry {
    void *pointer = nullptr;
    std::size_t bytes = 0;
    std::size_t alignment = 0;
    State state = State::kEmpty;
  };

  explicit CacheState(void *handle) : pool_handle(handle) {}

  ~CacheState() {
    for (auto &entry : entries) {
      if (entry.state == State::kEmpty || !entry.pointer) {
        continue;
      }
      deallocate_direct(pool_handle, entry.pointer, entry.bytes,
                        entry.alignment);
      entry = Entry{};
    }
  }

  void *allocate(std::size_t bytes, std::size_t alignment) {
    {
      std::lock_guard lock(mutex);
      if (!poisoned) {
        for (auto &entry : entries) {
          if (entry.state == State::kCached && entry.bytes == bytes &&
              entry.alignment == alignment) {
            entry.state = State::kActive;
            cached_bytes -= entry.bytes;
            return entry.pointer;
          }
        }
      }
    }

    void *pointer = allocate_direct(pool_handle, bytes, alignment);
    if (bytes > kMaxBlockBytes) {
      return pointer;
    }
    std::lock_guard lock(mutex);
    for (auto &entry : entries) {
      if (entry.state == State::kEmpty) {
        entry = Entry{.pointer = pointer,
                      .bytes = bytes,
                      .alignment = alignment,
                      .state = State::kActive};
        return pointer;
      }
    }
    return pointer;
  }

  bool deallocate(void *pointer, std::size_t bytes,
                  std::size_t alignment) noexcept {
    Entry released;
    bool free_upstream = false;
    {
      std::lock_guard lock(mutex);
      for (auto &entry : entries) {
        if (entry.pointer != pointer || entry.state == State::kEmpty) {
          continue;
        }
        if (entry.state != State::kActive) {
          poisoned = true;
          return true;
        }
        if (entry.bytes != bytes || entry.alignment != alignment) {
          poisoned = true;
        }
        // A cached block remains owned exclusively by this worker resource.
        // Its logical bytes are fully initialized by Arrow builders before
        // export; defer secure wiping until the block leaves this private
        // cache to avoid doubling memory traffic between adjacent packets.
        if (!poisoned && cached_bytes <= kMaxCachedBytes - entry.bytes) {
          entry.state = State::kCached;
          cached_bytes += entry.bytes;
          return true;
        }
        released = entry;
        entry = Entry{};
        free_upstream = true;
        break;
      }
    }
    if (free_upstream) {
      deallocate_direct(pool_handle, released.pointer, released.bytes,
                        released.alignment);
      return true;
    }
    return false;
  }

  void *pool_handle = nullptr;
  std::mutex mutex;
  std::array<Entry, kEntryCount> entries{};
  std::size_t cached_bytes = 0;
  bool poisoned = false;
};

PoolResource::PoolResource(void *pool_handle)
    : pool_handle_(pool_handle ? pool_handle
                               : static_cast<void *>(default_memory_pool())) {}

PoolResource::PoolResource(std::shared_ptr<void> pool_keepalive)
    : PoolResource(std::move(pool_keepalive), false) {}

PoolResource::PoolResource(std::shared_ptr<void> pool_keepalive,
                           bool recycle_exact_blocks)
    : pool_keepalive_(std::move(pool_keepalive)),
      pool_handle_(pool_keepalive_
                       ? pool_keepalive_.get()
                       : static_cast<void *>(default_memory_pool())) {
  if (recycle_exact_blocks) {
    cache_ = std::make_unique<CacheState>(pool_handle_);
  }
}

PoolResource::~PoolResource() = default;

void *PoolResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  return cache_ ? cache_->allocate(bytes, alignment)
                : allocate_direct(pool_handle_, bytes, alignment);
}

void PoolResource::do_deallocate(void *p, std::size_t bytes,
                                 std::size_t alignment) noexcept {
  if (cache_ && cache_->deallocate(p, bytes, alignment)) {
    return;
  }
  deallocate_direct(pool_handle_, p, bytes, alignment);
}

bool PoolResource::do_is_equal(
    const std::pmr::memory_resource &other) const noexcept {
  if (this == &other) {
    return true;
  }
  const auto *o = dynamic_cast<const PoolResource *>(&other);
  if (!o || cache_ || o->cache_) {
    return false;
  }
  return o->pool_handle_ == pool_handle_;
}

} // namespace sanitize::internal

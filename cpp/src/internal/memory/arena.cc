// Implements stable bump allocation for short-lived ingestion data.
// Ordinary blocks are reused across resets while exceptional blocks return
// to the pool.

#include "internal/memory/arena.hh"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <string_view>

#include "internal/memory/memory_pool.hh"

namespace sanitize::internal {

namespace {

/// Adds two block sizes or throws when the result cannot be represented.
std::size_t checked_add(std::size_t a, std::size_t b) {
  if (a > std::numeric_limits<std::size_t>::max() - b) {
    throw std::bad_alloc();
  }
  return a + b;
}

/// Rounds a positive alignment up to a representable power of two.
std::size_t normalize_alignment(std::size_t align) {
  if (align == 0) {
    return 1;
  }
  if ((align & (align - 1)) == 0) {
    return align;
  }
  const int digits = std::numeric_limits<std::size_t>::digits;
  const std::size_t max_power = std::size_t{1} << (digits - 1);
  if (align > max_power) {
    throw std::bad_alloc();
  }
  std::size_t out = 1;
  while (out < align) {
    out <<= 1;
  }
  return out;
}
} // namespace

BumpArena::BumpArena(void *pool_handle, std::size_t block_size)
    : pool_handle_(pool_handle ? pool_handle
                               : static_cast<void *>(default_memory_pool())),
      metadata_resource_(pool_handle_),
      block_size_(std::max<std::size_t>(4096, block_size)),
      blocks_(&metadata_resource_) {}

BumpArena::~BumpArena() {
  auto *pool = memory_pool_from_handle(pool_handle_);
  for (auto &b : blocks_) {
    if (b.data) {
      pool->Free(b.data, static_cast<int64_t>(b.size));
    }
  }
}

void BumpArena::reset() noexcept {
  // Retain enough ordinary capacity for reuse, but release exceptional blocks
  // allocated by one unusually large record. The previous arena kept its
  // lifetime high-water mark forever.
  const auto retained_limit =
      block_size_ > std::numeric_limits<std::size_t>::max() / 4
          ? block_size_
          : block_size_ * 4;
  auto *pool = memory_pool_from_handle(pool_handle_);
  std::size_t retained = 0;
  std::size_t write_index = 0;
  for (auto &block : blocks_) {
    const bool keep = block.data && block.size <= retained_limit &&
                      retained <= retained_limit - block.size;
    if (!keep) {
      if (block.data) {
        pool->Free(block.data, static_cast<int64_t>(block.size));
      }
      continue;
    }
    if (secure_memory_cleanup_enabled() && block.used > 0) {
      secure_zero_memory(block.data, block.used);
    }
    block.used = 0;
    retained += block.size;
    if (write_index != static_cast<std::size_t>(&block - blocks_.data())) {
      blocks_[write_index] = block;
    }
    ++write_index;
  }
  blocks_.resize(write_index);
  block_index_ = 0;
}

void *BumpArena::alloc(std::size_t n, std::size_t align) {
  if (n == 0) {
    return nullptr;
  }
  align = normalize_alignment(align);

  if (blocks_.empty()) {
    add_block(std::max(block_size_, checked_add(n, align)));
  }

  for (;;) {
    if (block_index_ >= blocks_.size()) {
      if (block_size_ > std::numeric_limits<std::size_t>::max() / 2) {
        throw std::bad_alloc();
      }
      add_block(std::max(block_size_ * 2, checked_add(n, align)));
    }

    Block &b = blocks_[block_index_];
    const auto p = reinterpret_cast<std::uintptr_t>(b.data + b.used);
    if (p > std::numeric_limits<std::uintptr_t>::max() - (align - 1)) {
      throw std::bad_alloc();
    }
    const std::uintptr_t aligned = (p + (align - 1)) & ~(align - 1);
    const auto pad = static_cast<std::size_t>(aligned - p);

    if (b.used <= b.size && pad <= b.size - b.used &&
        n <= b.size - b.used - pad) {
      b.used += pad;
      void *out = b.data + b.used;
      b.used += n;
      return out;
    }

    block_index_++;
  }
}

std::string_view BumpArena::append(std::string_view s) {
  if (s.empty()) {
    return std::string_view{};
  }
  void *dst = alloc(s.size(), alignof(char));
  if (!dst) {
    return std::string_view{};
  }
  std::memcpy(dst, s.data(), s.size());
  return {static_cast<const char *>(dst), s.size()};
}

void BumpArena::add_block(std::size_t want) {
  if (want > static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
    throw std::bad_alloc();
  }
  auto *pool = memory_pool_from_handle(pool_handle_);
  uint8_t *p = nullptr;
  const auto st = pool->Allocate(static_cast<int64_t>(want), &p);
  if (!st.ok()) {
    throw std::bad_alloc();
  }
  try {
    blocks_.push_back(Block{.data = p, .size = want, .used = 0});
  } catch (...) {
    pool->Free(p, static_cast<int64_t>(want));
    throw;
  }
}

} // namespace sanitize::internal

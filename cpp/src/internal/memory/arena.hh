// Declares bump-arena allocation for short-lived parser data.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string_view>
#include <vector>

#include "internal/memory/pool_resource.hh"

namespace sanitize::internal {

// Stable bump allocator for short-lived string_views.
//
// This is used for parsing CSV/JSON slices into many small string_views while
// ensuring the backing storage never relocates (unlike std::string growth).
//
// Memory comes from the internal MemoryPool so the full ingestion pipeline can
// be accounted and controlled consistently.
class BumpArena {
public:
  // Creates an arena backed by the configured memory pool.
  explicit BumpArena(void *pool_handle = nullptr,
                     std::size_t block_size = (1u << 20));
  // Destroys the BumpArena.
  ~BumpArena();

  // Disables copy construction.
  BumpArena(const BumpArena &) = delete;
  // Disables copy assignment.
  BumpArena &operator=(const BumpArena &) = delete;
  // Disables move construction.
  BumpArena(BumpArena &&) = delete;
  // Disables move assignment.
  BumpArena &operator=(BumpArena &&) = delete;

  // Reset allocations (keeps blocks for reuse).
  void reset() noexcept;

  // Allocate n bytes aligned to `align`.
  void *alloc(std::size_t n, std::size_t align = alignof(std::max_align_t));

  // Copies a string into stable arena storage.
  std::string_view append(std::string_view s);

  // Returns the operation pool used by arena payload and metadata allocations.
  [[nodiscard]] void *pool() const noexcept { return pool_handle_; }

private:
  struct Block {
    uint8_t *data = nullptr;
    std::size_t size = 0;
    std::size_t used = 0;
  };

  // Adds a backing block large enough for the requested byte count.
  void add_block(std::size_t want);

  void *pool_handle_ = nullptr;
  PoolResource metadata_resource_;
  std::size_t block_size_ = (1u << 20);
  std::pmr::vector<Block> blocks_;
  std::size_t block_index_ = 0;
};

} // namespace sanitize::internal

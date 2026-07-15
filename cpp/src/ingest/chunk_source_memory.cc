// Implements owned in-memory chunk sources.

#include "ingest/chunk_source_detail.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace sanitize {
namespace {

class OwnedChunkSource final : public ChunkSource {
public:
  explicit OwnedChunkSource(std::string bytes)
      : data_(std::make_shared<std::string>(std::move(bytes))) {}

  sanitize::Status Reset() override {
    pos_ = 0;
    return {};
  }

  sanitize::Result<Chunk> NextChunk(std::int64_t max_bytes) override {
    SAN_RETURN_NOT_OK(
        internal::validate_chunk_request(max_bytes, "OwnedChunkSource"));
    const std::string_view data(*data_);
    if (pos_ >= data.size()) {
      Chunk chunk;
      chunk.owner = data_;
      chunk.base_offset = pos_;
      return chunk;
    }
    const auto size = std::min<std::size_t>(static_cast<std::size_t>(max_bytes),
                                            data.size() - pos_);
    Chunk chunk;
    chunk.owner = data_;
    chunk.data = data.substr(pos_, size);
    chunk.base_offset = pos_;
    pos_ += size;
    return chunk;
  }

  sanitize::Result<Chunk> View() override {
    Chunk chunk;
    chunk.owner = data_;
    chunk.data = std::string_view(*data_);
    return chunk;
  }

private:
  std::shared_ptr<std::string> data_;
  std::size_t pos_ = 0;
};

} // namespace

namespace internal {

ChunkSourcePtr make_memory_chunk_source(std::string bytes) {
  return std::make_shared<OwnedChunkSource>(std::move(bytes));
}

} // namespace internal

ChunkSourcePtr chunk_source_from_bytes(std::string bytes) {
  return internal::make_memory_chunk_source(std::move(bytes));
}

} // namespace sanitize

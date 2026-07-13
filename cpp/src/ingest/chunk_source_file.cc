// Implements local-file chunk sources and bounded file checks.

#include "ingest/chunk_source_detail.hh"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace sanitize {
namespace {

enum class DetectedCompression : std::uint8_t { kNone = 0, kGzip };

DetectedCompression sniff_compression(const std::uint8_t *data,
                                      std::size_t size) {
  if (!data || size < 2) {
    return DetectedCompression::kNone;
  }
  return data[0] == 0x1f && data[1] == 0x8b ? DetectedCompression::kGzip
                                            : DetectedCompression::kNone;
}

sanitize::Result<DetectedCompression>
sniff_file_compression(const std::string &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.good()) {
    return sanitize::Status::Invalid("failed to open input file '", path, "'");
  }
  std::array<std::uint8_t, 2> magic{};
  input.read(reinterpret_cast<char *>(magic.data()),
             static_cast<std::streamsize>(magic.size()));
  return sniff_compression(magic.data(),
                           static_cast<std::size_t>(input.gcount()));
}

class FileChunkSource final : public ChunkSource {
public:
  explicit FileChunkSource(std::string path) : path_(std::move(path)) {}

  sanitize::Status Reset() override {
    input_.close();
    input_.clear();
    pos_ = 0;
    eof_ = false;
    full_view_.reset();
    return {};
  }

  sanitize::Result<Chunk> NextChunk(std::int64_t max_bytes) override {
    if (max_bytes <= 0) {
      return sanitize::Status::Invalid("NextChunk: max_bytes must be > 0");
    }
    if (eof_) {
      Chunk chunk;
      chunk.base_offset = pos_;
      return chunk;
    }
    SAN_RETURN_NOT_OK(open_if_needed());

    const auto max_stream =
        static_cast<std::int64_t>(std::numeric_limits<std::streamsize>::max());
    const auto requested = static_cast<std::streamsize>(
        std::min<std::int64_t>(max_bytes, max_stream));
    auto bytes = std::make_shared<std::string>(
        static_cast<std::size_t>(requested), '\0');
    input_.read(bytes->data(), requested);
    const auto read = input_.gcount();
    if (read <= 0) {
      eof_ = true;
      Chunk chunk;
      chunk.base_offset = pos_;
      return chunk;
    }

    bytes->resize(static_cast<std::size_t>(read));
    Chunk chunk;
    chunk.owner = bytes;
    chunk.data = std::string_view(*bytes);
    chunk.base_offset = pos_;
    pos_ += static_cast<std::size_t>(read);
    if (read < requested || input_.eof()) {
      eof_ = true;
    }
    return chunk;
  }

  sanitize::Result<Chunk> View() override {
    if (!full_view_) {
      SAN_ASSIGN_OR_RAISE(auto bytes, internal::read_file_bytes(path_));
      full_view_ = std::make_shared<std::string>(std::move(bytes));
    }
    Chunk chunk;
    chunk.owner = full_view_;
    chunk.data = std::string_view(*full_view_);
    return chunk;
  }

private:
  sanitize::Status open_if_needed() {
    if (input_.is_open()) {
      return {};
    }
    input_.open(path_, std::ios::binary);
    if (!input_.good()) {
      return sanitize::Status::Invalid("FileChunkSource: failed to open '",
                                       path_, "'");
    }
    return {};
  }

  std::string path_;
  std::ifstream input_;
  std::size_t pos_ = 0;
  bool eof_ = false;
  std::shared_ptr<std::string> full_view_;
};

} // namespace

namespace internal {

sanitize::Result<std::string> read_file_bytes(const std::string &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.good()) {
    return sanitize::Status::Invalid("read_file_bytes: failed to open '", path,
                                     "'");
  }
  input.seekg(0, std::ios::end);
  const auto end = input.tellg();
  if (end < 0) {
    return sanitize::Status::Invalid("read_file_bytes: tellg failed for '",
                                     path, "'");
  }
  const auto file_size = static_cast<std::uintmax_t>(end);
  if (file_size > std::numeric_limits<std::size_t>::max() ||
      file_size > static_cast<std::uintmax_t>(
                      std::numeric_limits<std::streamsize>::max())) {
    return sanitize::Status::OutOfMemory(
        "read_file_bytes: file too large to materialize: '", path, "'");
  }

  input.seekg(0, std::ios::beg);
  std::string bytes(static_cast<std::size_t>(file_size), '\0');
  if (file_size > 0) {
    input.read(bytes.data(), static_cast<std::streamsize>(file_size));
    if (!input) {
      return sanitize::Status::Invalid("read_file_bytes: short read for '",
                                       path, "'");
    }
  }
  return bytes;
}

sanitize::Status ensure_uncompressed_file(const std::string &path,
                                          std::string_view operation) {
  SAN_ASSIGN_OR_RAISE(const auto compression, sniff_file_compression(path));
  if (compression == DetectedCompression::kNone) {
    return {};
  }
  return sanitize::Status::NotImplemented(
      operation, ": compressed input is not available in core runtime; provide "
                 "decompressed input or use an adapter path");
}

ChunkSourcePtr make_file_chunk_source(std::string path) {
  return std::make_shared<FileChunkSource>(std::move(path));
}

} // namespace internal
} // namespace sanitize

// Implements file-backed chunk sources with canonical text transcoding.

#include "ingest/chunk_source_detail.hh"
#include "ingest/transcoding/decoder.hh"

#include <fstream>
#include <ios>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace sanitize {
namespace {

class TranscodingFileChunkSource final : public ChunkSource {
public:
  TranscodingFileChunkSource(std::string path, internal::TextEncoding encoding)
      : path_(std::move(path)), decoder_(encoding) {}

  sanitize::Status Reset() override {
    input_.close();
    input_.clear();
    utf8_pos_ = 0;
    eof_ = false;
    decoder_.Reset();
    full_view_.reset();
    return {};
  }

  sanitize::Result<Chunk> NextChunk(std::int64_t max_bytes) override {
    if (max_bytes <= 0) {
      return sanitize::Status::Invalid("NextChunk: max_bytes must be > 0");
    }
    if (eof_) {
      return empty_chunk();
    }
    SAN_RETURN_NOT_OK(open_if_needed());

    const auto requested = decoder_.raw_read_size(max_bytes);
    for (;;) {
      std::string raw(requested, '\0');
      input_.read(raw.data(), static_cast<std::streamsize>(requested));
      const auto read = input_.gcount();
      raw.resize(read > 0 ? static_cast<std::size_t>(read) : 0);

      const bool final = read <= 0;
      SAN_ASSIGN_OR_RAISE(auto transcoded, decoder_.Decode(raw, final));
      if (final) {
        eof_ = true;
        input_.close();
      }
      if (!transcoded.empty()) {
        auto bytes = std::make_shared<std::string>(std::move(transcoded));
        Chunk chunk;
        chunk.owner = bytes;
        chunk.data = std::string_view(*bytes);
        chunk.base_offset = utf8_pos_;
        utf8_pos_ += bytes->size();
        return chunk;
      }
      if (eof_) {
        return empty_chunk();
      }
    }
  }

  sanitize::Result<Chunk> View() override {
    if (!full_view_) {
      SAN_RETURN_NOT_OK(Reset());
      auto bytes = std::make_shared<std::string>();
      for (;;) {
        SAN_ASSIGN_OR_RAISE(auto chunk, NextChunk(1LL << 20));
        if (chunk.data.empty()) {
          break;
        }
        bytes->append(chunk.data);
      }
      full_view_ = std::move(bytes);
    }
    Chunk chunk;
    chunk.owner = full_view_;
    chunk.data = std::string_view(*full_view_);
    return chunk;
  }

private:
  [[nodiscard]] Chunk empty_chunk() const {
    Chunk chunk;
    chunk.base_offset = utf8_pos_;
    return chunk;
  }

  sanitize::Status open_if_needed() {
    if (input_.is_open()) {
      return {};
    }
    input_.open(path_, std::ios::binary);
    if (!input_.good()) {
      return sanitize::Status::Invalid(
          "TranscodingFileChunkSource: failed to open '", path_, "'");
    }
    return {};
  }

  std::string path_;
  internal::TranscodingDecoder decoder_;
  std::ifstream input_;
  std::size_t utf8_pos_ = 0;
  bool eof_ = false;
  std::shared_ptr<std::string> full_view_;
};

} // namespace

namespace internal {

ChunkSourcePtr make_transcoding_file_chunk_source(std::string path,
                                                  TextEncoding encoding) {
  return std::make_shared<TranscodingFileChunkSource>(std::move(path),
                                                      encoding);
}

} // namespace internal
} // namespace sanitize

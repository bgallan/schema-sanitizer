// Implements file-backed chunk sources with canonical text transcoding.

#include "ingest/chunk_source_detail.hh"
#include "internal/memory/memory_budget.hh"
#include "ingest/transcoding/decoder.hh"

#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "internal/memory/memory_pool.hh"

namespace sanitize {
namespace {

class SensitiveStringGuard {
public:
  explicit SensitiveStringGuard(std::string *value) : value_(value) {}
  ~SensitiveStringGuard() {
    if (value_ && internal::secure_memory_cleanup_enabled() &&
        !value_->empty()) {
      internal::secure_zero_memory(value_->data(), value_->size());
    }
  }

  SensitiveStringGuard(const SensitiveStringGuard &) = delete;
  SensitiveStringGuard &operator=(const SensitiveStringGuard &) = delete;

private:
  std::string *value_ = nullptr;
};

class TranscodingFileChunkSource final : public ChunkSource {
public:
  TranscodingFileChunkSource(std::string path, internal::TextEncoding encoding,
                             std::int64_t memory_limit_bytes)
      : path_(std::move(path)), decoder_(encoding),
        materialized_limit_(static_cast<std::uint64_t>(
            internal::memory_budget_from_limit(memory_limit_bytes)
                .materialized_input_bytes)) {}

  sanitize::Status Reset() override {
    input_.close();
    input_.clear();
    utf8_pos_ = 0;
    eof_ = false;
    pending_utf8_.reset();
    pending_utf8_pos_ = 0;
    decoder_.Reset();
    full_view_.reset();
    return {};
  }

  sanitize::Result<Chunk> NextChunk(std::int64_t max_bytes) override {
    SAN_RETURN_NOT_OK(internal::validate_chunk_request(
        max_bytes, "TranscodingFileChunkSource"));
    if (pending_utf8_) {
      return take_pending_chunk(max_bytes);
    }
    if (eof_) {
      return empty_chunk();
    }
    SAN_RETURN_NOT_OK(open_if_needed());

    const auto requested = decoder_.raw_read_size(max_bytes);
    for (;;) {
      std::string raw(requested, '\0');
      SensitiveStringGuard raw_guard(&raw);
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
        pending_utf8_ =
            std::make_shared<std::string>(std::move(transcoded));
        pending_utf8_pos_ = 0;
        return take_pending_chunk(max_bytes);
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
        SAN_RETURN_NOT_OK(internal::validate_materialized_input_growth(
            "TranscodingFileChunkSource: full view", path_,
            static_cast<std::uint64_t>(bytes->size()),
            static_cast<std::uint64_t>(chunk.data.size()),
            materialized_limit_));
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
  sanitize::Result<Chunk> take_pending_chunk(std::int64_t max_bytes) {
    if (!pending_utf8_ || pending_utf8_pos_ >= pending_utf8_->size()) {
      return sanitize::Status::Invalid(
          "TranscodingFileChunkSource: missing pending UTF-8 bytes");
    }
    const auto available = pending_utf8_->size() - pending_utf8_pos_;
    const auto take = std::min<std::size_t>(
        available, static_cast<std::size_t>(max_bytes));
    if (take > std::numeric_limits<std::size_t>::max() - utf8_pos_) {
      return sanitize::Status::OutOfMemory(
          "TranscodingFileChunkSource: UTF-8 offset overflow");
    }

    auto owner = pending_utf8_;
    Chunk chunk;
    chunk.owner = owner;
    chunk.data = std::string_view(*owner).substr(pending_utf8_pos_, take);
    chunk.base_offset = utf8_pos_;
    pending_utf8_pos_ += take;
    utf8_pos_ += take;
    if (pending_utf8_pos_ == owner->size()) {
      pending_utf8_.reset();
      pending_utf8_pos_ = 0;
    }
    return chunk;
  }

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
  std::uint64_t materialized_limit_ = 0;
  std::ifstream input_;
  std::size_t utf8_pos_ = 0;
  bool eof_ = false;
  std::shared_ptr<std::string> pending_utf8_;
  std::size_t pending_utf8_pos_ = 0;
  std::shared_ptr<std::string> full_view_;
};

} // namespace

namespace internal {

ChunkSourcePtr make_transcoding_file_chunk_source(
    std::string path, TextEncoding encoding, std::int64_t memory_limit_bytes) {
  return std::make_shared<TranscodingFileChunkSource>(
      std::move(path), encoding, memory_limit_bytes);
}

} // namespace internal
} // namespace sanitize

// Implements file-backed chunk sources with canonical text transcoding. The
// implementation preserves split code units and bounded buffers across
// incremental source reads.

#include "ingest/chunk_source_detail.hh"
#include "ingest/secure_read_only_file.hh"
#include "ingest/transcoding/decoder.hh"
#include "internal/memory/memory_budget.hh"

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
  /// Tracks a decoded buffer that may require secure erasure on scope exit.
  explicit SensitiveStringGuard(std::string *value) : value_(value) {}

  /// Securely erases the tracked bytes when hardened memory cleanup is enabled.
  ~SensitiveStringGuard() {
    if (value_ && internal::secure_memory_cleanup_enabled() &&
        !value_->empty()) {
      internal::secure_zero_memory(value_->data(), value_->size());
    }
  }

  /// Disables copying so a sensitive buffer has exactly one cleanup guard.
  SensitiveStringGuard(const SensitiveStringGuard &) = delete;

  /// Disables copy assignment so cleanup responsibility cannot be duplicated.
  SensitiveStringGuard &operator=(const SensitiveStringGuard &) = delete;

private:
  std::string *value_ = nullptr;
};

class TranscodingFileChunkSource final : public ChunkSource {
public:
  /// Configures a bounded file source and decoder for the requested text
  /// encoding.
  TranscodingFileChunkSource(std::string path, internal::TextEncoding encoding,
                             std::int64_t memory_limit_bytes)
      : path_(std::move(path)), decoder_(encoding),
        materialized_limit_(static_cast<std::uint64_t>(
            internal::memory_budget_from_limit(memory_limit_bytes)
                .materialized_input_bytes)) {}

  /// Releases the securely opened input through its non-throwing RAII owner.
  ~TranscodingFileChunkSource() override = default;

  /// Rewinds the text transcoding source to its initial input position and
  /// clears per-pass state.
  sanitize::Status Reset() override {
    if (!input_.Close()) {
      return sanitize::Status::IOError(
          "TranscodingFileChunkSource: failed closing input");
    }
    utf8_pos_ = 0;
    eof_ = false;
    pending_utf8_.reset();
    pending_utf8_pos_ = 0;
    decoder_.Reset();
    full_view_.reset();
    return {};
  }

  /// Returns the next decoded UTF-8 chunk and advances the raw-input cursor.
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
      SAN_ASSIGN_OR_RAISE(const auto read, input_.Read(raw.data(), requested));
      raw.resize(read);

      const bool final = read == 0U;
      SAN_ASSIGN_OR_RAISE(auto transcoded, decoder_.Decode(raw, final));
      if (final) {
        eof_ = true;
        if (!input_.Close()) {
          return sanitize::Status::IOError(
              "TranscodingFileChunkSource: failed closing input");
        }
      }
      if (!transcoded.empty()) {
        pending_utf8_ = std::make_shared<std::string>(std::move(transcoded));
        pending_utf8_pos_ = 0;
        return take_pending_chunk(max_bytes);
      }
      if (eof_) {
        return empty_chunk();
      }
    }
  }

  /// Exposes the current text transcoding source bytes without extending their
  /// documented lifetime.
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
  /// Returns a bounded slice of pending UTF-8 bytes and advances the decoded
  /// offset.
  sanitize::Result<Chunk> take_pending_chunk(std::int64_t max_bytes) {
    if (!pending_utf8_ || pending_utf8_pos_ >= pending_utf8_->size()) {
      return sanitize::Status::Invalid(
          "TranscodingFileChunkSource: missing pending UTF-8 bytes");
    }
    const auto available = pending_utf8_->size() - pending_utf8_pos_;
    const auto take =
        std::min<std::size_t>(available, static_cast<std::size_t>(max_bytes));
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

  /// Produces an empty end-of-input chunk at the current decoded offset.
  [[nodiscard]] Chunk empty_chunk() const {
    Chunk chunk;
    chunk.base_offset = utf8_pos_;
    return chunk;
  }

  /// Lazily acquires a descriptor permit and opens the encoded source in binary
  /// mode.
  sanitize::Status open_if_needed() {
    if (input_.is_open()) {
      return {};
    }
    SAN_ASSIGN_OR_RAISE(input_, internal::SecureReadOnlyFile::Open(
                                    path_, "TranscodingFileChunkSource"));
    return {};
  }

  std::string path_;
  internal::TranscodingDecoder decoder_;
  std::uint64_t materialized_limit_ = 0;
  internal::SecureReadOnlyFile input_;
  std::size_t utf8_pos_ = 0;
  bool eof_ = false;
  std::shared_ptr<std::string> pending_utf8_;
  std::size_t pending_utf8_pos_ = 0;
  std::shared_ptr<std::string> full_view_;
};

} // namespace

namespace internal {

/// Creates a file source that incrementally decodes non-UTF-8 input into
/// canonical UTF-8.
ChunkSourcePtr
make_transcoding_file_chunk_source(std::string path, TextEncoding encoding,
                                   std::int64_t memory_limit_bytes) {
  return std::make_shared<TranscodingFileChunkSource>(std::move(path), encoding,
                                                      memory_limit_bytes);
}

} // namespace internal
} // namespace sanitize

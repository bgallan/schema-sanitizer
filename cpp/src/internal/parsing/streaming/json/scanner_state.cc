// Owns lifecycle and chunk state for incremental JSON scanning.

#include "internal/parsing/streaming/json/scanner.hh"

#include <utility>

namespace sanitize::internal {

JsonStreamingScanner::JsonStreamingScanner(ChunkSourcePtr src,
                                           int64_t chunk_bytes,
                                           bool require_top_level_array,
                                           bool line_delimited)
    : src_(std::move(src)),
      chunk_bytes_(chunk_bytes > 0 ? chunk_bytes : (int64_t{1} << 20)),
      require_top_level_array_(require_top_level_array),
      line_delimited_(line_delimited) {}

sanitize::Status JsonStreamingScanner::Reset() {
  eof_ = false;
  have_chunk_ = false;
  pos_ = 0;
  eof_offset_ = 0;
  last_end_offset_ = 0;
  chunk_ = Chunk{};
  state_ = State::kInit;
  if (!src_) {
    return sanitize::Status::Invalid("JSON scanner: source is null");
  }
  return src_->Reset();
}

bool JsonStreamingScanner::done() const noexcept {
  return state_ == State::kDone;
}

std::size_t JsonStreamingScanner::eof_offset() const noexcept {
  return eof_ ? eof_offset_ : last_end_offset_;
}

sanitize::Status JsonStreamingScanner::ensure_chunk() {
  if (have_chunk_ && pos_ < chunk_.data.size()) {
    return sanitize::Status::OK();
  }
  if (have_chunk_ && pos_ >= chunk_.data.size() && eof_) {
    return sanitize::Status::OK();
  }
  return refill();
}

sanitize::Status JsonStreamingScanner::refill() {
  if (!src_) {
    return sanitize::Status::Invalid("JSON scanner: source is null");
  }
  SAN_ASSIGN_OR_RAISE(Chunk c, src_->NextChunk(chunk_bytes_));
  have_chunk_ = true;
  chunk_ = std::move(c);
  pos_ = 0;
  if (chunk_.data.empty()) {
    eof_ = true;
    eof_offset_ = chunk_.base_offset;
    last_end_offset_ = eof_offset_;
  } else {
    last_end_offset_ = chunk_.base_offset + chunk_.data.size();
  }
  return sanitize::Status::OK();
}

char JsonStreamingScanner::peek() const noexcept {
  if (!have_chunk_ || pos_ >= chunk_.data.size()) {
    return '\0';
  }
  return chunk_.data[pos_];
}

void JsonStreamingScanner::consume() noexcept {
  if (have_chunk_ && pos_ < chunk_.data.size()) {
    ++pos_;
  }
}

} // namespace sanitize::internal

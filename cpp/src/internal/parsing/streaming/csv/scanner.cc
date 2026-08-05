// Implements lifecycle and chunk flow for the streaming CSV scanner.

#include "internal/parsing/streaming/csv/scanner.hh"

#include <algorithm>
#include <string_view>
#include <utility>

namespace sanitize::internal {

CsvStreamingScanner::CsvStreamingScanner(ChunkSourcePtr source,
                                         int64_t chunk_bytes,
                                         std::size_t max_record_bytes,
                                         std::size_t max_record_segments,
                                         void *pool_handle)
    : segment_resource_(pool_handle), segments_(&segment_resource_),
      source_(std::move(source)),
      chunk_bytes_(chunk_bytes > 0 ? chunk_bytes : kDefaultCsvChunkBytes),
      max_record_bytes_(std::max<std::size_t>(1, max_record_bytes)),
      max_record_segments_(std::max<std::size_t>(1, max_record_segments)) {
  segments_.reserve(4);
}

void CsvStreamingScanner::clear_segments() noexcept {
  segments_.clear();
  constexpr std::size_t kMaxRetainedSegments = 1024;
  if (segments_.capacity() > kMaxRetainedSegments) {
    std::pmr::vector<Segment> empty(&segment_resource_);
    segments_.swap(empty);
  }
}

sanitize::Status CsvStreamingScanner::Reset() {
  eof_ = false;
  have_chunk_ = false;
  pos_ = 0;
  pending_consume_lf_ = false;
  prefer_vector_scan_ = false;
  eof_offset_ = 0;
  chunk_ = Chunk{};
  clear_segments();
  if (!source_) {
    return sanitize::Status::Invalid("CSV scanner: source is null");
  }
  return source_->Reset();
}

sanitize::Result<TextSlice> CsvStreamingScanner::next_record(BumpArena *arena) {
  if (!arena) {
    return sanitize::Status::Invalid("CSV scanner: arena is null");
  }
  clear_segments();
  SAN_ASSIGN_OR_RAISE(bool has_record, prepare_record_start());
  if (!has_record) {
    return make_text_slice(std::string_view{}, eof_offset_);
  }
  return scan_csv_record_span(*this, arena);
}

bool CsvStreamingScanner::done() const noexcept {
  if (!eof_) {
    return false;
  }
  return !have_chunk_ || pos_ >= chunk_.data.size();
}

sanitize::Status CsvStreamingScanner::ensure_chunk() {
  if (have_chunk_ && pos_ < chunk_.data.size()) {
    return sanitize::Status::OK();
  }
  if (have_chunk_ && pos_ >= chunk_.data.size() && eof_) {
    return sanitize::Status::OK();
  }
  return refill();
}

sanitize::Status CsvStreamingScanner::refill() {
  if (!source_) {
    return sanitize::Status::Invalid("CSV scanner: source is null");
  }
  SAN_ASSIGN_OR_RAISE(Chunk chunk, source_->NextChunk(chunk_bytes_));
  have_chunk_ = true;
  chunk_ = std::move(chunk);
  pos_ = 0;
  if (chunk_.data.empty()) {
    eof_ = true;
    eof_offset_ = chunk_.base_offset;
  }
  return sanitize::Status::OK();
}

void CsvStreamingScanner::consume_pending_lf() noexcept {
  if (!pending_consume_lf_) {
    return;
  }
  pending_consume_lf_ = false;
  if (pos_ < chunk_.data.size() && chunk_.data[pos_] == '\n') {
    ++pos_;
  }
}

sanitize::Result<bool> CsvStreamingScanner::prepare_record_start() {
  for (;;) {
    SAN_RETURN_NOT_OK(ensure_chunk());
    if (chunk_.data.empty()) {
      return false;
    }
    consume_pending_lf();
    if (pos_ < chunk_.data.size()) {
      return true;
    }
    if (eof_) {
      return false;
    }
    SAN_RETURN_NOT_OK(refill());
  }
}

} // namespace sanitize::internal

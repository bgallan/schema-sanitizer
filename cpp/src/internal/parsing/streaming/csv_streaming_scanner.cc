// Scans chunked CSV input into complete record spans.

#include "internal/parsing/streaming/csv_streaming_scanner.hh"

#include <string_view>
#include <utility>

namespace sanitize::internal {

CsvStreamingScanner::CsvStreamingScanner(ChunkSourcePtr src,
                                         int64_t chunk_bytes)
    : src_(std::move(src)),
      chunk_bytes_(chunk_bytes > 0 ? chunk_bytes : kDefaultCsvChunkBytes) {
  segs_.reserve(4);
}

sanitize::Status CsvStreamingScanner::Reset() {
  eof_ = false;
  have_chunk_ = false;
  pos_ = 0;
  pending_consume_lf_ = false;
  eof_offset_ = 0;
  chunk_ = Chunk{};
  if (!src_)
    return sanitize::Status::Invalid("CSV scanner: source is null");
  return src_->Reset();
}

sanitize::Result<TextSlice> CsvStreamingScanner::next_record(BumpArena *arena) {
  if (!arena)
    return sanitize::Status::Invalid("CSV scanner: arena is null");

  segs_.clear();
  SAN_ASSIGN_OR_RAISE(bool has_record, prepare_record_start());
  if (!has_record) {
    return make_text_slice(std::string_view{}, eof_offset_);
  }
  return scan_csv_record_span(*this, arena);
}

bool CsvStreamingScanner::done() const noexcept {
  if (!eof_)
    return false;
  if (!have_chunk_)
    return true;
  return pos_ >= chunk_.data.size();
}

sanitize::Status CsvStreamingScanner::ensure_chunk() {
  if (have_chunk_ && pos_ < chunk_.data.size())
    return sanitize::Status::OK();
  if (have_chunk_ && pos_ >= chunk_.data.size() && eof_)
    return sanitize::Status::OK();
  return refill();
}

sanitize::Status CsvStreamingScanner::refill() {
  if (!src_)
    return sanitize::Status::Invalid("CSV scanner: source is null");
  SAN_ASSIGN_OR_RAISE(Chunk c, src_->NextChunk(chunk_bytes_));
  have_chunk_ = true;
  chunk_ = std::move(c);
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

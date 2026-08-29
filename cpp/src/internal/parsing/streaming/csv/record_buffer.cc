// Implements buffering and arena materialization for multi-chunk CSV records.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/streaming/csv/record_span_internal.hh"

#include <cstring>

namespace sanitize::internal {

sanitize::Status CsvRecordSpanScanner::push_segment(std::size_t end_pos) {
  if (end_pos <= segment_start_pos_) {
    return sanitize::Status::OK();
  }
  std::string_view part = scanner_.chunk_.data.substr(
      segment_start_pos_, end_pos - segment_start_pos_);
  if (part.size() > scanner_.max_record_bytes_ ||
      total_bytes_ > scanner_.max_record_bytes_ - part.size()) {
    return sanitize::Status::OutOfMemory(
        "CSV raw record size exceeds effective operation limit: ",
        scanner_.max_record_bytes_);
  }
  if (scanner_.segments_.size() >= scanner_.max_record_segments_) {
    return sanitize::Status::Invalid("CSV record spans too many input chunks");
  }
  total_bytes_ += part.size();
  scanner_.segments_.push_back(CsvStreamingScanner::Segment{
      .owner = segment_owner_,
      .view = part,
  });
  return sanitize::Status::OK();
}

sanitize::Status CsvRecordSpanScanner::span_to_next_chunk() {
  SAN_RETURN_NOT_OK(push_segment(scanner_.chunk_.data.size()));
  multi_ = true;
  SAN_RETURN_NOT_OK(scanner_.refill());
  segment_start_pos_ = 0;
  segment_owner_ = scanner_.chunk_.owner;
  return sanitize::Status::OK();
}

void CsvRecordSpanScanner::trim_trailing_cr() {
  if (!scanner_.segments_.empty() && !scanner_.segments_.back().view.empty() &&
      scanner_.segments_.back().view.back() == '\r') {
    auto view = scanner_.segments_.back().view;
    view.remove_suffix(1);
    --total_bytes_;
    scanner_.segments_.back().view = view;
  }
}

sanitize::Result<TextSlice> CsvRecordSpanScanner::materialize_segments() {
  char *destination =
      static_cast<char *>(arena_->alloc(total_bytes_, alignof(char)));
  if (!destination && total_bytes_) {
    return sanitize::Status::Invalid("CSV scanner: arena alloc failed");
  }
  std::size_t written = 0;
  for (const auto &segment : scanner_.segments_) {
    std::memcpy(destination + written, segment.view.data(),
                segment.view.size());
    written += segment.view.size();
  }
  // The arena copy owns the completed record. Drop chunk owners now rather
  // than retaining every contributing input chunk until the next record.
  scanner_.clear_segments();
  return make_text_slice(std::string_view(destination, total_bytes_),
                         record_start_abs_, {}, record_source_file_owner_,
                         record_source_file_, record_source_index_,
                         record_has_source_index_);
}

} // namespace sanitize::internal

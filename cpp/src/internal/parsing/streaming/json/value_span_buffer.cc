// Buffers and materializes JSON values that span multiple input chunks.

#include "internal/parsing/streaming/json/value_span_scanner.hh"

#include <cstring>

namespace sanitize::internal {

sanitize::Status JsonValueSpanScanner::push_segment(std::size_t end_pos) {
  if (end_pos <= seg_start_pos_) {
    return sanitize::Status::OK();
  }
  std::string_view part =
      scanner_.chunk_.data.substr(seg_start_pos_, end_pos - seg_start_pos_);
  if (part.size() > kMaxValueBytes ||
      total_bytes_ > kMaxValueBytes - part.size()) {
    return sanitize::Status::Invalid("JSON value exceeds max buffered size");
  }
  total_bytes_ += part.size();
  segments_.push_back(Segment{.owner = seg_owner_, .view = part});
  return sanitize::Status::OK();
}

sanitize::Status JsonValueSpanScanner::need_more() {
  SAN_RETURN_NOT_OK(push_segment(scanner_.chunk_.data.size()));
  multi_ = true;
  SAN_RETURN_NOT_OK(scanner_.refill());
  seg_start_pos_ = 0;
  seg_owner_ = scanner_.chunk_.owner;
  return sanitize::Status::OK();
}

sanitize::Result<TextSlice> JsonValueSpanScanner::finish() {
  if (!multi_) {
    const std::string_view view =
        scanner_.chunk_.data.substr(start_pos_, scanner_.pos_ - start_pos_);
    return make_text_slice(view, start_abs_, start_owner_,
                           start_source_file_owner_, start_source_file_,
                           start_source_index_, start_has_source_index_);
  }

  SAN_RETURN_NOT_OK(push_segment(scanner_.pos_));

  char *dst = static_cast<char *>(arena_->alloc(total_bytes_, alignof(char)));
  if (!dst && total_bytes_) {
    return sanitize::Status::Invalid("JSON scanner: arena alloc failed");
  }

  std::size_t written = 0;
  for (const auto &segment : segments_) {
    std::memcpy(dst + written, segment.view.data(), segment.view.size());
    written += segment.view.size();
  }

  return make_text_slice(std::string_view(dst, total_bytes_), start_abs_, {},
                         start_source_file_owner_, start_source_file_,
                         start_source_index_, start_has_source_index_);
}

} // namespace sanitize::internal

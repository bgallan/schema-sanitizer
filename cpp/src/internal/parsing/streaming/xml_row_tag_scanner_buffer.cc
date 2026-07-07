// Implements XML row scanner buffering and refill helpers.

#include "internal/parsing/streaming/xml_row_tag_scanner.hh"

#include <algorithm>
#include <cstdint>

namespace sanitize::internal {

sanitize::Status
XmlRowTagScanner::enforce_buffer_limit(std::size_t incoming) const {
  if (memory_limit_bytes_ <= 0) {
    return sanitize::Status::OK();
  }
  const auto limit = static_cast<std::uint64_t>(memory_limit_bytes_);
  const auto current = static_cast<std::uint64_t>(buffer_.size());
  const auto add = static_cast<std::uint64_t>(incoming);
  if (add > limit || current > limit - add) {
    return sanitize::Status::OutOfMemory(
        "memory_limit_bytes limit exceeded during xml parsing: ", current + add,
        " bytes > ", memory_limit_bytes_, " bytes");
  }
  return sanitize::Status::OK();
}

bool XmlRowTagScanner::should_compact_before_refill() const noexcept {
  const std::size_t keep_from =
      (row_start_pos_ == npos) ? scan_pos_ : row_start_pos_;
  if (keep_from == 0) {
    return false;
  }
  if (row_start_pos_ == npos) {
    return true;
  }
  if (memory_limit_bytes_ > 0 &&
      buffer_.size() >= static_cast<std::size_t>(memory_limit_bytes_)) {
    return true;
  }
  const auto chunk_threshold = static_cast<std::size_t>(
      chunk_bytes_ > 0 ? chunk_bytes_ : (int64_t{1} << 20));
  return keep_from >= chunk_threshold && keep_from * 2 >= buffer_.size();
}

void XmlRowTagScanner::compact_buffer() {
  const std::size_t keep_from =
      (row_start_pos_ == npos) ? scan_pos_ : row_start_pos_;
  if (keep_from == 0) {
    return;
  }
  if (keep_from >= buffer_.size()) {
    buffer_start_offset_ += keep_from;
    buffer_.clear();
    scan_pos_ = 0;
    if (row_start_pos_ != npos) {
      row_start_pos_ = 0;
    }
    return;
  }
  buffer_.erase(0, keep_from);
  buffer_start_offset_ += keep_from;
  scan_pos_ -= std::min(scan_pos_, keep_from);
  if (row_start_pos_ != npos) {
    row_start_pos_ -= keep_from;
  }
}

sanitize::Result<bool> XmlRowTagScanner::ensure_data() {
  if (scan_pos_ < buffer_.size()) {
    return true;
  }
  if (eof_) {
    return false;
  }
  SAN_RETURN_NOT_OK(refill());
  return scan_pos_ < buffer_.size();
}

sanitize::Status XmlRowTagScanner::refill() {
  if (should_compact_before_refill()) {
    compact_buffer();
  }
  int64_t request_bytes = chunk_bytes_;
  if (memory_limit_bytes_ > 0) {
    if (buffer_.size() >= static_cast<std::size_t>(memory_limit_bytes_)) {
      SAN_RETURN_NOT_OK(enforce_buffer_limit(1));
    }
    const auto remaining = static_cast<int64_t>(
        static_cast<std::size_t>(memory_limit_bytes_) - buffer_.size());
    request_bytes = std::min<int64_t>(request_bytes, remaining);
  }
  SAN_ASSIGN_OR_RAISE(auto chunk, src_->NextChunk(request_bytes));
  if (chunk.data.empty()) {
    eof_ = true;
    return sanitize::Status::OK();
  }
  SAN_RETURN_NOT_OK(enforce_buffer_limit(chunk.data.size()));
  if (buffer_.empty()) {
    buffer_start_offset_ = chunk.base_offset;
  }
  buffer_.append(chunk.data);
  return sanitize::Status::OK();
}

sanitize::Status
XmlRowTagScanner::read_more_or_fail(std::string_view eof_message) {
  if (eof_) {
    return invalid(eof_message);
  }
  return refill();
}

} // namespace sanitize::internal

// Implements XML row scanner lifecycle and row iteration.

#include "internal/parsing/streaming/xml/row_scanner.hh"

#include <utility>

#include "internal/parsing/xml/token_match.hh"

namespace sanitize::internal {

namespace xml_scan = xml_tokens;

XmlRowTagScanner::XmlRowTagScanner(ChunkSourcePtr src, std::string row_tag,
                                   int64_t chunk_bytes,
                                   int64_t memory_limit_bytes)
    : src_(std::move(src)), row_tag_(std::move(row_tag)),
      chunk_bytes_((chunk_bytes > 0) ? chunk_bytes : (int64_t{1} << 20)),
      memory_limit_bytes_(memory_limit_bytes) {
  if (memory_limit_bytes_ > 0 && chunk_bytes_ > memory_limit_bytes_) {
    chunk_bytes_ = memory_limit_bytes_;
  }
}

sanitize::Status XmlRowTagScanner::Reset() {
  if (!src_) {
    return sanitize::Status::Invalid("XML scanner: source is null");
  }
  buffer_.clear();
  buffer_start_offset_ = 0;
  scan_pos_ = 0;
  eof_ = false;
  done_ = false;
  root_open_ = false;
  root_closed_ = false;
  depth_ = 0;
  stack_.clear();
  row_start_pos_ = npos;
  row_parent_depth_ = 0;
  return src_->Reset();
}

sanitize::Result<XmlRowTagScanner::RowSlice> XmlRowTagScanner::next_row() {
  if (done_) {
    return RowSlice{};
  }
  if (row_tag_.empty()) {
    return sanitize::Status::Invalid("XML scanner: row tag is empty");
  }

  for (;;) {
    SAN_ASSIGN_OR_RAISE(bool has_data, ensure_data());
    if (!has_data) {
      if (row_start_pos_ != npos) {
        return invalid("unterminated row element");
      }
      if (depth_ != 0) {
        return invalid("unterminated document");
      }
      if (!root_open_) {
        return invalid("empty document");
      }
      done_ = true;
      return RowSlice{};
    }

    const std::size_t lt = buffer_.find('<', scan_pos_);
    if (lt == std::string::npos) {
      SAN_RETURN_NOT_OK(handle_text(buffer_.substr(scan_pos_)));
      scan_pos_ = buffer_.size();
      continue;
    }

    SAN_RETURN_NOT_OK(handle_text(buffer_.substr(scan_pos_, lt - scan_pos_)));
    SAN_ASSIGN_OR_RAISE(std::size_t gt, find_markup_end(lt));
    if (gt == npos) {
      SAN_RETURN_NOT_OK(read_more_or_fail("unterminated markup"));
      continue;
    }

    SAN_ASSIGN_OR_RAISE(auto row, handle_markup(lt, gt));
    if (!row.text.empty()) {
      return row;
    }
  }
}

sanitize::Status XmlRowTagScanner::invalid(std::string_view message) const {
  return sanitize::Status::Invalid("XML parse error at byte ",
                                   buffer_start_offset_ + scan_pos_, ": ",
                                   message);
}

sanitize::Status XmlRowTagScanner::handle_text(std::string_view text) {
  if (text.empty()) {
    return sanitize::Status::OK();
  }
  if ((!root_open_ || root_closed_) && !xml_scan::xml_is_ws(text)) {
    return invalid("non-whitespace content outside root element");
  }
  return sanitize::Status::OK();
}

sanitize::Result<XmlRowTagScanner::RowSlice>
XmlRowTagScanner::make_row(std::size_t end_pos) {
  RowSlice row{
      .text = buffer_.substr(row_start_pos_, end_pos - row_start_pos_),
      .base_offset = buffer_start_offset_ + row_start_pos_,
  };
  row_start_pos_ = npos;
  row_parent_depth_ = 0;
  scan_pos_ = end_pos;
  return row;
}

} // namespace sanitize::internal

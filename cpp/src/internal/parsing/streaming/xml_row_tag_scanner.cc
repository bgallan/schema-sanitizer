// Implements streaming XML row-tag slicing.

#include "internal/parsing/streaming/xml_row_tag_scanner.hh"

#include <cctype>
#include <utility>

#include "internal/parsing/streaming/xml_row_tag_scanner_utils.hh"

namespace sanitize::internal {

namespace {

namespace xml_scan = xml_row_tag_scanner_utils;

} // namespace

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

sanitize::Result<std::size_t>
XmlRowTagScanner::find_markup_end(std::size_t lt) {
  if (xml_scan::starts_with_at(buffer_, lt, "<!--")) {
    const std::size_t found = buffer_.find("-->", lt + 4);
    return found == npos ? npos : found + 2;
  }
  if (xml_scan::starts_with_at(buffer_, lt, "<?")) {
    const std::size_t found = buffer_.find("?>", lt + 2);
    return found == npos ? npos : found + 1;
  }
  if (xml_scan::starts_with_at(buffer_, lt, "<![CDATA[")) {
    const std::size_t found = buffer_.find("]]>", lt + 9);
    return found == npos ? npos : found + 2;
  }

  bool in_quote = false;
  char quote = '\0';
  for (std::size_t pos = lt + 1; pos < buffer_.size(); ++pos) {
    const char ch = buffer_[pos];
    if (in_quote) {
      if (ch == quote) {
        in_quote = false;
      }
      continue;
    }
    if (ch == '\'' || ch == '"') {
      in_quote = true;
      quote = ch;
      continue;
    }
    if (ch == '>') {
      return pos;
    }
  }
  return npos;
}

sanitize::Result<std::string>
XmlRowTagScanner::parse_markup_name(std::size_t pos, std::size_t gt) const {
  const std::size_t start = pos;
  while (pos < gt) {
    const unsigned char c = static_cast<unsigned char>(buffer_[pos]);
    if (std::isspace(c) != 0 || buffer_[pos] == '/' || buffer_[pos] == '>' ||
        buffer_[pos] == '=') {
      break;
    }
    pos++;
  }
  if (start == pos) {
    return invalid("expected element name");
  }
  return std::string(buffer_.substr(start, pos - start));
}

bool XmlRowTagScanner::tag_is_self_closing(std::size_t lt,
                                           std::size_t gt) const {
  std::size_t pos = gt;
  while (pos > lt + 1 &&
         std::isspace(static_cast<unsigned char>(buffer_[pos - 1])) != 0) {
    pos--;
  }
  return pos > lt + 1 && buffer_[pos - 1] == '/';
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

sanitize::Result<XmlRowTagScanner::RowSlice>
XmlRowTagScanner::handle_markup(std::size_t lt, std::size_t gt) {
  if (xml_scan::starts_with_at(buffer_, lt, "<!--") ||
      xml_scan::starts_with_at(buffer_, lt, "<?")) {
    scan_pos_ = gt + 1;
    return RowSlice{};
  }
  if (xml_scan::starts_with_at(buffer_, lt, "<![CDATA[")) {
    if (depth_ == 0) {
      return invalid("CDATA is only valid inside an element");
    }
    scan_pos_ = gt + 1;
    return RowSlice{};
  }
  if (xml_scan::starts_with_at(buffer_, lt, "<!")) {
    if (xml_scan::starts_with_ascii_ci_at(buffer_, lt, "<!DOCTYPE") ||
        xml_scan::starts_with_ascii_ci_at(buffer_, lt, "<!ENTITY")) {
      return invalid("DTD and entity declarations are not supported");
    }
    return invalid("unsupported declaration");
  }
  if (xml_scan::starts_with_at(buffer_, lt, "</")) {
    SAN_ASSIGN_OR_RAISE(auto close_name, parse_markup_name(lt + 2, gt));
    if (depth_ == 0 || stack_.empty()) {
      return invalid("unexpected closing tag");
    }
    if (close_name != stack_.back()) {
      return sanitize::Status::Invalid(
          "XML parse error at byte ", buffer_start_offset_ + lt,
          ": closing tag </", close_name, "> does not match <", stack_.back(),
          ">");
    }
    stack_.pop_back();
    depth_--;
    if (depth_ == 0) {
      root_closed_ = true;
    }
    if (row_start_pos_ != npos && depth_ == row_parent_depth_) {
      return make_row(gt + 1);
    }
    scan_pos_ = gt + 1;
    return RowSlice{};
  }

  if (root_closed_) {
    return invalid("trailing element after root element");
  }
  SAN_ASSIGN_OR_RAISE(auto name, parse_markup_name(lt + 1, gt));
  const bool self_closing = tag_is_self_closing(lt, gt);
  const int parent_depth = depth_;

  if (!root_open_) {
    root_open_ = true;
    if (name == row_tag_) {
      row_start_pos_ = lt;
      row_parent_depth_ = parent_depth;
    }
  } else if (parent_depth == 1 && name == row_tag_) {
    row_start_pos_ = lt;
    row_parent_depth_ = parent_depth;
  }

  if (!self_closing) {
    depth_++;
    stack_.push_back(name);
  } else if (parent_depth == 0) {
    root_closed_ = true;
  }

  if (self_closing && row_start_pos_ != npos &&
      row_parent_depth_ == parent_depth) {
    return make_row(gt + 1);
  }

  scan_pos_ = gt + 1;
  return RowSlice{};
}

} // namespace sanitize::internal

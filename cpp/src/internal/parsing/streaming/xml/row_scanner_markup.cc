// Implements XML markup parsing and row-boundary transitions.

#include "internal/parsing/streaming/xml/row_scanner.hh"

#include "internal/parsing/xml/token_match.hh"

namespace sanitize::internal {

namespace xml_scan = xml_tokens;

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
    if (xml_scan::is_xml_whitespace(static_cast<char>(c)) ||
        buffer_[pos] == '/' || buffer_[pos] == '>' || buffer_[pos] == '=') {
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
  while (pos > lt + 1 && xml_scan::is_xml_whitespace(buffer_[pos - 1])) {
    pos--;
  }
  return pos > lt + 1 && buffer_[pos - 1] == '/';
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

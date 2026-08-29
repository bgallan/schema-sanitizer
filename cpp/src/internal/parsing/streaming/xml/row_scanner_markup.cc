// Implements hardened XML markup parsing and row-boundary transitions.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/streaming/xml/row_scanner.hh"

#include <limits>
#include <memory_resource>
#include <string_view>

#include "internal/parsing/xml/token_match.hh"

namespace sanitize::internal {
namespace {

namespace xml_scan = xml_tokens;

/// Reports whether buffered XML markup is a prefix of the requested delimiter
/// token.
[[nodiscard]] bool is_token_prefix(std::string_view available,
                                   std::string_view token) noexcept {
  return available.size() < token.size() && token.starts_with(available);
}

} // namespace

sanitize::Result<std::size_t>
XmlRowTagScanner::find_markup_end(std::size_t lt) {
  if (pending_markup_lt_ != lt) {
    reset_pending_markup();
    pending_markup_lt_ = lt;
  }

  if (pending_markup_kind_ == PendingMarkupKind::kNone) {
    const std::string_view available = std::string_view(buffer_).substr(lt);
    if (is_token_prefix(available, "<!--") ||
        is_token_prefix(available, "<?") ||
        is_token_prefix(available, "<![CDATA[")) {
      return npos;
    }
    if (xml_scan::starts_with_at(buffer_, lt, "<!--")) {
      pending_markup_kind_ = PendingMarkupKind::kComment;
      pending_markup_resume_ = lt + 4U;
    } else if (xml_scan::starts_with_at(buffer_, lt, "<?")) {
      pending_markup_kind_ = PendingMarkupKind::kProcessingInstruction;
      pending_markup_resume_ = lt + 2U;
    } else if (xml_scan::starts_with_at(buffer_, lt, "<![CDATA[")) {
      pending_markup_kind_ = PendingMarkupKind::kCdata;
      pending_markup_resume_ = lt + 9U;
    } else {
      pending_markup_kind_ = PendingMarkupKind::kTag;
      pending_markup_resume_ = lt + 1U;
    }
  }

  std::string_view terminator;
  switch (pending_markup_kind_) {
  case PendingMarkupKind::kComment:
    terminator = "-->";
    break;
  case PendingMarkupKind::kProcessingInstruction:
    terminator = "?>";
    break;
  case PendingMarkupKind::kCdata:
    terminator = "]]>";
    break;
  case PendingMarkupKind::kTag:
  case PendingMarkupKind::kNone:
    break;
  }

  if (!terminator.empty()) {
    const std::size_t found = buffer_.find(terminator, pending_markup_resume_);
    if (found == npos) {
      const std::size_t overlap = terminator.size() - 1U;
      pending_markup_resume_ = buffer_.size() > overlap
                                   ? buffer_.size() - overlap
                                   : pending_markup_resume_;
      return npos;
    }
    return found + terminator.size() - 1U;
  }

  for (std::size_t pos = pending_markup_resume_; pos < buffer_.size(); ++pos) {
    const char byte = buffer_[pos];
    if (pending_markup_in_quote_) {
      if (byte == pending_markup_quote_) {
        pending_markup_in_quote_ = false;
      }
      continue;
    }
    if (byte == '\'' || byte == '"') {
      pending_markup_in_quote_ = true;
      pending_markup_quote_ = byte;
      continue;
    }
    if (byte == '>') {
      return pos;
    }
  }
  pending_markup_resume_ = buffer_.size();
  return npos;
}

sanitize::Result<std::pmr::string>
XmlRowTagScanner::parse_markup_name(std::size_t pos, std::size_t gt) const {
  const std::size_t start = pos;
  while (pos < gt) {
    const unsigned char byte = static_cast<unsigned char>(buffer_[pos]);
    if (xml_scan::is_xml_whitespace(static_cast<char>(byte)) ||
        buffer_[pos] == '/' || buffer_[pos] == '>' || buffer_[pos] == '=' ||
        buffer_[pos] == '?') {
      break;
    }
    ++pos;
  }
  if (start == pos) {
    return invalid_at(start, "expected element name");
  }
  const std::string_view name(buffer_.data() + start, pos - start);
  if (!xml_scan::is_valid_xml_name(name)) {
    return invalid_at(start, "invalid XML name");
  }
  return std::pmr::string(name, resource_);
}

sanitize::Status
XmlRowTagScanner::validate_start_markup(std::size_t lt, std::size_t gt,
                                        std::string_view expected_name,
                                        bool *self_closing) const {
  if (!self_closing) {
    return sanitize::Status::Invalid("XML scanner self-closing output is null");
  }
  *self_closing = false;
  std::size_t pos = lt + 1U;
  const std::size_t name_start = pos;
  while (pos < gt && !xml_scan::is_xml_whitespace(buffer_[pos]) &&
         buffer_[pos] != '/' && buffer_[pos] != '>') {
    ++pos;
  }
  const std::string_view name(buffer_.data() + name_start, pos - name_start);
  if (name != expected_name || !xml_scan::is_valid_xml_name(name)) {
    return invalid_at(name_start, "invalid XML element name");
  }

  std::pmr::vector<std::string_view> attributes(resource_);
  attributes.reserve(8U);
  for (;;) {
    while (pos < gt && xml_scan::is_xml_whitespace(buffer_[pos])) {
      ++pos;
    }
    if (pos == gt) {
      return sanitize::Status::OK();
    }
    if (buffer_[pos] == '/') {
      ++pos;
      while (pos < gt && xml_scan::is_xml_whitespace(buffer_[pos])) {
        ++pos;
      }
      if (pos != gt) {
        return invalid_at(pos, "unexpected bytes after '/' in start tag");
      }
      *self_closing = true;
      return sanitize::Status::OK();
    }
    if (attributes.size() >= xml_scan::kMaxXmlAttributesPerElement) {
      return invalid_at(
          pos, "XML attributes per element exceed internal safety limit");
    }

    const std::size_t attribute_start = pos;
    while (pos < gt && !xml_scan::is_xml_whitespace(buffer_[pos]) &&
           buffer_[pos] != '=' && buffer_[pos] != '/' && buffer_[pos] != '>') {
      ++pos;
    }
    const std::string_view attribute(buffer_.data() + attribute_start,
                                     pos - attribute_start);
    if (!xml_scan::is_valid_xml_name(attribute)) {
      return invalid_at(attribute_start, "invalid XML attribute name");
    }
    for (const auto existing : attributes) {
      if (existing == attribute) {
        return invalid_at(attribute_start,
                          "duplicate attribute on the same element");
      }
    }
    attributes.push_back(attribute);

    while (pos < gt && xml_scan::is_xml_whitespace(buffer_[pos])) {
      ++pos;
    }
    if (pos >= gt || buffer_[pos] != '=') {
      return invalid_at(pos, "expected '=' after attribute name");
    }
    ++pos;
    while (pos < gt && xml_scan::is_xml_whitespace(buffer_[pos])) {
      ++pos;
    }
    if (pos >= gt || (buffer_[pos] != '\'' && buffer_[pos] != '"')) {
      return invalid_at(pos, "expected quoted attribute value");
    }
    const char quote = buffer_[pos++];
    const std::size_t value_start = pos;
    while (pos < gt && buffer_[pos] != quote) {
      if (buffer_[pos] == '<') {
        return invalid_at(pos, "raw '<' is not allowed in an attribute value");
      }
      ++pos;
    }
    if (pos >= gt) {
      return invalid_at(value_start, "unterminated attribute value");
    }
    SAN_RETURN_NOT_OK(validate_xml_entities(
        std::string_view(buffer_).substr(value_start, pos - value_start),
        buffer_start_offset_ + value_start));
    ++pos;
    if (pos < gt && !xml_scan::is_xml_whitespace(buffer_[pos]) &&
        buffer_[pos] != '/') {
      return invalid_at(pos, "unexpected byte after closing attribute quote");
    }
  }
}

sanitize::Status XmlRowTagScanner::validate_closing_markup(
    std::size_t lt, std::size_t gt, std::string_view expected_name) const {
  std::size_t pos = lt + 2U;
  const std::size_t name_start = pos;
  while (pos < gt && !xml_scan::is_xml_whitespace(buffer_[pos]) &&
         buffer_[pos] != '/' && buffer_[pos] != '>') {
    ++pos;
  }
  const std::string_view name(buffer_.data() + name_start, pos - name_start);
  if (!xml_scan::is_valid_xml_name(name)) {
    return invalid_at(name_start, "invalid XML closing tag name");
  }
  if (name != expected_name) {
    return sanitize::Status::Invalid(
        "XML parse error at byte ", buffer_start_offset_ + lt,
        ": closing tag does not match the open element");
  }
  while (pos < gt && xml_scan::is_xml_whitespace(buffer_[pos])) {
    ++pos;
  }
  if (pos != gt) {
    return invalid_at(pos, "unexpected bytes in closing tag");
  }
  return sanitize::Status::OK();
}

sanitize::Status
XmlRowTagScanner::validate_comment_markup(std::size_t lt,
                                          std::size_t gt) const {
  if (gt < lt + 6U) {
    return invalid_at(lt, "malformed XML comment");
  }
  const std::size_t content_start = lt + 4U;
  const std::size_t close_start = gt - 2U;
  const std::size_t embedded = buffer_.find("--", content_start);
  if (embedded != npos && embedded != close_start) {
    return invalid_at(embedded, "'--' is not allowed inside an XML comment");
  }
  return sanitize::Status::OK();
}

sanitize::Status
XmlRowTagScanner::validate_processing_instruction(std::size_t lt,
                                                  std::size_t gt) const {
  if (gt < lt + 3U) {
    return invalid_at(lt, "malformed processing instruction");
  }
  std::size_t pos = lt + 2U;
  const std::size_t target_start = pos;
  while (pos + 1U < gt && !xml_scan::is_xml_whitespace(buffer_[pos]) &&
         buffer_[pos] != '?') {
    ++pos;
  }
  const std::string_view target(buffer_.data() + target_start,
                                pos - target_start);
  if (!xml_scan::is_valid_xml_name(target)) {
    return invalid_at(target_start, "invalid processing instruction target");
  }
  if (target.size() == 3U &&
      xml_scan::ascii_lower(static_cast<unsigned char>(target[0])) == 'x' &&
      xml_scan::ascii_lower(static_cast<unsigned char>(target[1])) == 'm' &&
      xml_scan::ascii_lower(static_cast<unsigned char>(target[2])) == 'l' &&
      buffer_start_offset_ + lt != 0U) {
    return invalid_at(lt, "XML declaration is only allowed at document start");
  }
  return sanitize::Status::OK();
}

sanitize::Result<XmlRowTagScanner::RowSlice>
XmlRowTagScanner::handle_markup(std::size_t lt, std::size_t gt) {
  if (xml_scan::starts_with_at(buffer_, lt, "<!--")) {
    SAN_RETURN_NOT_OK(validate_comment_markup(lt, gt));
    scan_pos_ = gt + 1U;
    return RowSlice{};
  }
  if (xml_scan::starts_with_at(buffer_, lt, "<?")) {
    SAN_RETURN_NOT_OK(validate_processing_instruction(lt, gt));
    scan_pos_ = gt + 1U;
    return RowSlice{};
  }
  if (xml_scan::starts_with_at(buffer_, lt, "<![CDATA[")) {
    if (depth_ == 0U) {
      return invalid_at(lt, "CDATA is only valid inside an element");
    }
    scan_pos_ = gt + 1U;
    return RowSlice{};
  }
  if (xml_scan::starts_with_at(buffer_, lt, "<!")) {
    if (xml_scan::starts_with_ascii_ci_at(buffer_, lt, "<!DOCTYPE") ||
        xml_scan::starts_with_ascii_ci_at(buffer_, lt, "<!ENTITY")) {
      return invalid_at(lt, "DTD and entity declarations are not supported");
    }
    return invalid_at(lt, "unsupported declaration");
  }
  if (xml_scan::starts_with_at(buffer_, lt, "</")) {
    if (depth_ == 0U || stack_.empty()) {
      return invalid_at(lt, "unexpected closing tag");
    }
    SAN_RETURN_NOT_OK(validate_closing_markup(lt, gt, stack_.back()));
    stack_.pop_back();
    --depth_;
    if (depth_ == 0U) {
      root_closed_ = true;
    }
    if (row_start_pos_ != npos && depth_ == row_parent_depth_) {
      return make_row(gt + 1U);
    }
    scan_pos_ = gt + 1U;
    return RowSlice{};
  }

  if (root_closed_) {
    return invalid_at(lt, "trailing element after root element");
  }
  SAN_ASSIGN_OR_RAISE(auto name, parse_markup_name(lt + 1U, gt));
  bool self_closing = false;
  SAN_RETURN_NOT_OK(validate_start_markup(lt, gt, name, &self_closing));
  const std::uint32_t parent_depth = depth_;
  if (parent_depth >= xml_scan::kMaxXmlNestingDepth) {
    return sanitize::Status::Invalid(
        "XML parse error at byte ", buffer_start_offset_ + lt,
        ": XML nesting depth ", static_cast<std::uint64_t>(parent_depth) + 1U,
        " exceeds internal safety limit ", xml_scan::kMaxXmlNestingDepth);
  }

  if (!root_open_) {
    root_open_ = true;
    if (name == row_tag_) {
      row_start_pos_ = lt;
      row_parent_depth_ = parent_depth;
    }
  } else if (parent_depth == 1U && name == row_tag_) {
    row_start_pos_ = lt;
    row_parent_depth_ = parent_depth;
  }

  if (!self_closing) {
    if (depth_ == std::numeric_limits<std::uint32_t>::max()) {
      return invalid_at(lt, "XML nesting depth counter overflow");
    }
    ++depth_;
    stack_.push_back(std::move(name));
  } else if (parent_depth == 0U) {
    root_closed_ = true;
  }

  if (self_closing && row_start_pos_ != npos &&
      row_parent_depth_ == parent_depth) {
    return make_row(gt + 1U);
  }

  scan_pos_ = gt + 1U;
  return RowSlice{};
}

} // namespace sanitize::internal

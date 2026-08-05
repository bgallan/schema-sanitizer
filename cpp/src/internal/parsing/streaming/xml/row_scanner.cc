// Implements hardened XML row scanner lifecycle and row iteration.

#include "internal/parsing/streaming/xml/row_scanner.hh"

#include <algorithm>
#include <new>
#include <utility>

#include "internal/parsing/xml/token_match.hh"

namespace sanitize::internal {

namespace xml_scan = xml_tokens;

XmlRowTagScanner::XmlRowTagScanner(ChunkSourcePtr src, std::string row_tag,
                                   int64_t chunk_bytes,
                                   int64_t memory_limit_bytes,
                                   std::pmr::memory_resource *resource)
    : src_(std::move(src)),
      resource_(resource ? resource : std::pmr::get_default_resource()),
      row_tag_(std::move(row_tag), resource_),
      chunk_bytes_((chunk_bytes > 0) ? chunk_bytes : (int64_t{1} << 20)),
      memory_limit_bytes_(memory_limit_bytes), buffer_(resource_),
      stack_(resource_), text_entity_(resource_) {
  if (memory_limit_bytes_ > 0 && chunk_bytes_ > memory_limit_bytes_) {
    chunk_bytes_ = memory_limit_bytes_;
  }
}

sanitize::Status XmlRowTagScanner::Reset() {
  if (!src_) {
    return sanitize::Status::Invalid("XML scanner: source is null");
  }
  try {
    discard_buffer();
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
    reset_pending_markup();
    utf8_validator_.Reset();
    text_entity_open_ = false;
    text_entity_offset_ = 0;
    text_entity_.clear();
    text_closing_brackets_ = 0;
    return src_->Reset();
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory("XML scanner reset allocation failed");
  }
}

sanitize::Result<XmlRowTagScanner::RowSlice> XmlRowTagScanner::next_row() {
  if (done_) {
    return RowSlice{};
  }
  if (row_tag_.empty()) {
    return sanitize::Status::Invalid("XML scanner: row tag is empty");
  }

  try {
    for (;;) {
      SAN_ASSIGN_OR_RAISE(bool has_data, ensure_data());
      if (!has_data) {
        SAN_RETURN_NOT_OK(finish_text_token());
        SAN_RETURN_NOT_OK(
            utf8_validator_.Finish(buffer_start_offset_ + buffer_.size()));
        if (row_start_pos_ != npos) {
          return invalid("unterminated row element");
        }
        if (depth_ != 0U || (root_open_ && !root_closed_)) {
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
        SAN_RETURN_NOT_OK(handle_text(buffer_.substr(scan_pos_), scan_pos_));
        scan_pos_ = buffer_.size();
        continue;
      }

      SAN_RETURN_NOT_OK(
          handle_text(buffer_.substr(scan_pos_, lt - scan_pos_), scan_pos_));
      SAN_RETURN_NOT_OK(finish_text_token());
      text_closing_brackets_ = 0;
      scan_pos_ = lt;
      SAN_ASSIGN_OR_RAISE(std::size_t gt, find_markup_end(lt));
      if (gt == npos) {
        SAN_RETURN_NOT_OK(read_more_or_fail("unterminated markup"));
        continue;
      }

      SAN_ASSIGN_OR_RAISE(auto row, handle_markup(lt, gt));
      reset_pending_markup();
      if (!row.text.empty()) {
        return row;
      }
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "XML scanner allocation failed at byte ",
        buffer_start_offset_ + scan_pos_);
  }
}

sanitize::Status XmlRowTagScanner::invalid(std::string_view message) const {
  return invalid_at(scan_pos_, message);
}

sanitize::Status XmlRowTagScanner::invalid_at(std::size_t buffer_position,
                                              std::string_view message) const {
  return sanitize::Status::Invalid("XML parse error at byte ",
                                   buffer_start_offset_ + buffer_position, ": ",
                                   message);
}

sanitize::Status XmlRowTagScanner::handle_text(std::string_view text,
                                               std::size_t buffer_position) {
  if (text.empty()) {
    return sanitize::Status::OK();
  }
  if ((!root_open_ || root_closed_) && !xml_scan::xml_is_ws(text)) {
    return invalid_at(buffer_position,
                      "non-whitespace content outside root element");
  }

  for (std::size_t index = 0; index < text.size(); ++index) {
    const char byte = text[index];
    const std::size_t absolute = buffer_start_offset_ + buffer_position + index;

    if (byte == ']') {
      text_closing_brackets_ = static_cast<std::uint8_t>(std::min<unsigned>(
          2U, static_cast<unsigned>(text_closing_brackets_) + 1U));
    } else {
      if (byte == '>' && text_closing_brackets_ >= 2U) {
        return sanitize::Status::Invalid(
            "XML parse error at byte ", absolute - 2U,
            ": ']]>' is only allowed to terminate CDATA");
      }
      text_closing_brackets_ = 0;
    }

    if (!text_entity_open_) {
      if (byte == '&') {
        text_entity_open_ = true;
        text_entity_offset_ = absolute;
        text_entity_.clear();
        text_entity_.push_back('&');
      }
      continue;
    }

    if (byte == '&' || byte == '<') {
      return sanitize::Status::Invalid(
          "XML parse error at byte ", text_entity_offset_,
          ": unterminated or malformed entity reference");
    }
    if (text_entity_.size() >= 64U) {
      return sanitize::Status::Invalid(
          "XML parse error at byte ", text_entity_offset_,
          ": entity reference exceeds internal safety limit");
    }
    text_entity_.push_back(byte);
    if (byte == ';') {
      SAN_RETURN_NOT_OK(
          validate_xml_entities(text_entity_, text_entity_offset_));
      text_entity_open_ = false;
      text_entity_.clear();
    }
  }
  return sanitize::Status::OK();
}

sanitize::Status XmlRowTagScanner::finish_text_token() {
  if (text_entity_open_) {
    return sanitize::Status::Invalid(
        "XML parse error at byte ", text_entity_offset_,
        ": unterminated or malformed entity reference");
  }
  return sanitize::Status::OK();
}

sanitize::Result<XmlRowTagScanner::RowSlice>
XmlRowTagScanner::make_row(std::size_t end_pos) {
  RowSlice row{
      .text = std::string_view(buffer_).substr(row_start_pos_,
                                               end_pos - row_start_pos_),
      .base_offset = buffer_start_offset_ + row_start_pos_,
  };
  row_start_pos_ = npos;
  row_parent_depth_ = 0;
  scan_pos_ = end_pos;
  return row;
}

void XmlRowTagScanner::reset_pending_markup() noexcept {
  pending_markup_kind_ = PendingMarkupKind::kNone;
  pending_markup_lt_ = npos;
  pending_markup_resume_ = 0;
  pending_markup_in_quote_ = false;
  pending_markup_quote_ = '\0';
}

} // namespace sanitize::internal

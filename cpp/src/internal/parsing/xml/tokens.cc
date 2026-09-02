// Implements XML names, attributes, and ignorable markup parsing.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/xml/document.hh"

#include "internal/parsing/xml/token_match.hh"
#include "internal/parsing/xml_entities.hh"

#include <algorithm>
#include <utility>

namespace sanitize::internal {

bool XmlParser::starts_with_ascii_ci(std::string_view token) const {
  return xml_tokens::starts_with_ascii_ci_at(input_, pos_, token);
}

sanitize::Status XmlParser::skip_until(std::string_view token) {
  const std::size_t found = input_.find(token, pos_);
  if (found == std::string_view::npos) {
    return invalid("unterminated markup");
  }
  pos_ = found + token.size();
  return sanitize::Status::OK();
}

sanitize::Status XmlParser::skip_comment() {
  const std::size_t content_start = pos_ + 4U;
  const std::size_t close = input_.find("-->", content_start);
  if (close == std::string_view::npos) {
    return invalid("unterminated comment");
  }
  const std::size_t double_hyphen = input_.find("--", content_start);
  if (double_hyphen != close) {
    return invalid_at(double_hyphen,
                      "'--' is not allowed inside an XML comment");
  }
  pos_ = close + 3U;
  return sanitize::Status::OK();
}

sanitize::Status XmlParser::skip_processing_instruction() {
  const std::size_t begin = pos_;
  pos_ += 2U;
  SAN_ASSIGN_OR_RAISE(auto target, parse_name());
  if (target.size() == 3U &&
      xml_tokens::ascii_lower(static_cast<unsigned char>(target[0])) == 'x' &&
      xml_tokens::ascii_lower(static_cast<unsigned char>(target[1])) == 'm' &&
      xml_tokens::ascii_lower(static_cast<unsigned char>(target[2])) == 'l' &&
      base_offset_ + begin != 0U) {
    return invalid_at(begin,
                      "XML declaration is only allowed at document start");
  }
  const std::size_t close = input_.find("?>", pos_);
  if (close == std::string_view::npos) {
    return invalid_at(begin, "unterminated processing instruction");
  }
  pos_ = close + 2U;
  return sanitize::Status::OK();
}

sanitize::Status XmlParser::skip_bang_markup() {
  if (starts_with("<!--")) {
    return skip_comment();
  }
  if (starts_with("<![CDATA[")) {
    return invalid("CDATA is only valid inside an element");
  }
  if (starts_with_ascii_ci("<!DOCTYPE") || starts_with_ascii_ci("<!ENTITY")) {
    return invalid("DTD and entity declarations are not supported");
  }
  return invalid("unsupported declaration");
}

sanitize::Status XmlParser::skip_misc() {
  for (;;) {
    skip_ws();
    if (starts_with("<?")) {
      SAN_RETURN_NOT_OK(skip_processing_instruction());
      continue;
    }
    if (starts_with("<!")) {
      SAN_RETURN_NOT_OK(skip_bang_markup());
      continue;
    }
    return sanitize::Status::OK();
  }
}

sanitize::Result<std::pmr::string> XmlParser::parse_name() {
  const std::size_t start = pos_;
  while (pos_ < input_.size()) {
    const unsigned char byte = static_cast<unsigned char>(input_[pos_]);
    if (xml_tokens::is_xml_whitespace(static_cast<char>(byte)) ||
        input_[pos_] == '/' || input_[pos_] == '>' || input_[pos_] == '=' ||
        input_[pos_] == '?') {
      break;
    }
    ++pos_;
  }
  if (start == pos_) {
    return invalid("expected name");
  }
  const std::string_view name = input_.substr(start, pos_ - start);
  if (!xml_tokens::is_valid_xml_name(name)) {
    return invalid_at(start, "invalid XML name");
  }
  return std::pmr::string(name, resource_);
}

sanitize::Result<std::pmr::string> XmlParser::parse_quoted_value() {
  if (pos_ >= input_.size() || (input_[pos_] != '"' && input_[pos_] != '\'')) {
    return invalid("expected quoted attribute value");
  }
  const char quote = input_[pos_++];
  const std::size_t start = pos_;
  while (pos_ < input_.size() && input_[pos_] != quote) {
    if (input_[pos_] == '<') {
      return invalid("raw '<' is not allowed in an attribute value");
    }
    ++pos_;
  }
  if (pos_ >= input_.size()) {
    return invalid_at(start, "unterminated attribute value");
  }
  SAN_ASSIGN_OR_RAISE(auto value,
                      decode_xml_entities(input_.substr(start, pos_ - start),
                                          resource_, base_offset_ + start));
  if (value.size() >
      xml_tokens::kMaxXmlDecodedBytes -
          std::min(decoded_bytes_, xml_tokens::kMaxXmlDecodedBytes)) {
    return invalid_at(start, "decoded XML text exceeds internal safety limit");
  }
  decoded_bytes_ += value.size();
  ++pos_;
  return value;
}

sanitize::Result<XmlAttribute> XmlParser::parse_attribute() {
  SAN_ASSIGN_OR_RAISE(auto name, parse_name());
  skip_ws();
  if (pos_ >= input_.size() || input_[pos_] != '=') {
    return invalid("expected '=' after attribute name");
  }
  ++pos_;
  skip_ws();
  SAN_ASSIGN_OR_RAISE(auto value, parse_quoted_value());
  XmlAttribute attribute(resource_);
  attribute.name = std::move(name);
  attribute.value = std::move(value);
  return attribute;
}

} // namespace sanitize::internal

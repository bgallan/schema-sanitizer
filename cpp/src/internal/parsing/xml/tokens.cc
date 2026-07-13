// Implements XML names, attributes, and ignorable markup parsing.

#include "internal/parsing/xml/document.hh"

#include "internal/parsing/xml/token_match.hh"
#include "internal/parsing/xml_entities.hh"

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

sanitize::Status XmlParser::skip_bang_markup() {
  if (starts_with("<!--")) {
    pos_ += 4;
    return skip_until("-->");
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
      pos_ += 2;
      SAN_RETURN_NOT_OK(skip_until("?>"));
      continue;
    }
    if (starts_with("<!")) {
      SAN_RETURN_NOT_OK(skip_bang_markup());
      continue;
    }
    return sanitize::Status::OK();
  }
}

sanitize::Result<std::string> XmlParser::parse_name() {
  const std::size_t start = pos_;
  while (pos_ < input_.size()) {
    const unsigned char c = static_cast<unsigned char>(input_[pos_]);
    if (xml_tokens::is_xml_whitespace(static_cast<char>(c)) ||
        input_[pos_] == '/' || input_[pos_] == '>' || input_[pos_] == '=') {
      break;
    }
    pos_++;
  }
  if (start == pos_) {
    return invalid("expected name");
  }
  return std::string(input_.substr(start, pos_ - start));
}

sanitize::Result<std::string> XmlParser::parse_quoted_value() {
  if (pos_ >= input_.size() || (input_[pos_] != '"' && input_[pos_] != '\'')) {
    return invalid("expected quoted attribute value");
  }
  const char quote = input_[pos_++];
  const std::size_t start = pos_;
  while (pos_ < input_.size() && input_[pos_] != quote) {
    pos_++;
  }
  if (pos_ >= input_.size()) {
    return invalid("unterminated attribute value");
  }
  std::string value = decode_xml_entities(input_.substr(start, pos_ - start));
  pos_++;
  return value;
}

sanitize::Result<XmlAttribute> XmlParser::parse_attribute() {
  SAN_ASSIGN_OR_RAISE(auto name, parse_name());
  skip_ws();
  if (pos_ >= input_.size() || input_[pos_] != '=') {
    return invalid("expected '=' after attribute name");
  }
  pos_++;
  skip_ws();
  SAN_ASSIGN_OR_RAISE(auto value, parse_quoted_value());
  return XmlAttribute{.name = std::move(name), .value = std::move(value)};
}

} // namespace sanitize::internal

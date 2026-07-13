// Implements XML document parser lifecycle and basic cursor helpers.

#include "internal/parsing/xml/document.hh"

#include "internal/parsing/xml/token_match.hh"

#include <cctype>

namespace sanitize::internal {

XmlParser::XmlParser(std::string_view input) : input_(input) {}

sanitize::Result<std::unique_ptr<XmlNode>> XmlParser::parse_document() {
  SAN_RETURN_NOT_OK(skip_misc());
  if (pos_ >= input_.size()) {
    return sanitize::Status::Invalid("XML parse error: empty document");
  }
  SAN_ASSIGN_OR_RAISE(auto root, parse_element());
  SAN_RETURN_NOT_OK(skip_misc());
  skip_ws();
  if (pos_ != input_.size()) {
    return invalid("trailing content after root element");
  }
  return root;
}

bool XmlParser::starts_with(std::string_view token) const {
  return input_.substr(pos_, token.size()) == token;
}

sanitize::Status XmlParser::invalid(std::string_view message) const {
  return sanitize::Status::Invalid("XML parse error at byte ", pos_, ": ",
                                   message);
}

void XmlParser::skip_ws() {
  while (pos_ < input_.size() && xml_tokens::is_xml_whitespace(input_[pos_])) {
    pos_++;
  }
}

} // namespace sanitize::internal

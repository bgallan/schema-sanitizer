// Implements XML document parsing and value-view projection.

#include "internal/parsing/xml_document.hh"

#include <cctype>
#include <charconv>
#include <system_error>
#include <utility>

namespace sanitize::internal {

namespace {

/// Append one Unicode code point encoded as UTF-8.
void append_utf8(std::string *out, std::uint32_t cp) {
  if (cp <= 0x7f) {
    out->push_back(static_cast<char>(cp));
  } else if (cp <= 0x7ff) {
    out->push_back(static_cast<char>(0xc0 | (cp >> 6)));
    out->push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  } else if (cp <= 0xffff) {
    out->push_back(static_cast<char>(0xe0 | (cp >> 12)));
    out->push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  } else {
    out->push_back(static_cast<char>(0xf0 | (cp >> 18)));
    out->push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  }
}

/// Decode XML predefined and numeric character entities in text.
std::string decode_xml_entities(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (std::size_t i = 0; i < text.size();) {
    if (text[i] != '&') {
      out.push_back(text[i++]);
      continue;
    }

    const std::size_t semi = text.find(';', i + 1);
    if (semi == std::string_view::npos) {
      out.push_back(text[i++]);
      continue;
    }

    const std::string_view ent = text.substr(i + 1, semi - i - 1);
    if (ent == "amp") {
      out.push_back('&');
    } else if (ent == "lt") {
      out.push_back('<');
    } else if (ent == "gt") {
      out.push_back('>');
    } else if (ent == "quot") {
      out.push_back('"');
    } else if (ent == "apos") {
      out.push_back('\'');
    } else if (ent.starts_with("#x") || ent.starts_with("#X")) {
      std::uint32_t cp = 0;
      auto first = ent.data() + 2;
      auto last = ent.data() + ent.size();
      auto res = std::from_chars(first, last, cp, 16);
      if (res.ec == std::errc{} && res.ptr == last) {
        append_utf8(&out, cp);
      } else {
        out.append(text.substr(i, semi - i + 1));
      }
    } else if (ent.starts_with("#")) {
      std::uint32_t cp = 0;
      auto first = ent.data() + 1;
      auto last = ent.data() + ent.size();
      auto res = std::from_chars(first, last, cp, 10);
      if (res.ec == std::errc{} && res.ptr == last) {
        append_utf8(&out, cp);
      } else {
        out.append(text.substr(i, semi - i + 1));
      }
    } else {
      out.append(text.substr(i, semi - i + 1));
    }
    i = semi + 1;
  }
  return out;
}

} // namespace

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

bool XmlParser::starts_with_ascii_ci(std::string_view token) const {
  if (input_.size() - pos_ < token.size()) {
    return false;
  }
  for (std::size_t i = 0; i < token.size(); ++i) {
    const auto a = static_cast<unsigned char>(input_[pos_ + i]);
    const auto b = static_cast<unsigned char>(token[i]);
    if (std::tolower(a) != std::tolower(b)) {
      return false;
    }
  }
  return true;
}

sanitize::Status XmlParser::invalid(std::string_view message) const {
  return sanitize::Status::Invalid("XML parse error at byte ", pos_, ": ",
                                   message);
}

void XmlParser::skip_ws() {
  while (pos_ < input_.size() &&
         std::isspace(static_cast<unsigned char>(input_[pos_])) != 0) {
    pos_++;
  }
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
    if (std::isspace(c) != 0 || input_[pos_] == '/' || input_[pos_] == '>' ||
        input_[pos_] == '=') {
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

sanitize::Result<std::unique_ptr<XmlNode>> XmlParser::parse_element() {
  if (pos_ >= input_.size() || input_[pos_] != '<') {
    return invalid("expected element");
  }
  if (starts_with("</")) {
    return invalid("unexpected closing tag");
  }

  const std::size_t start_offset = pos_;
  pos_++;
  SAN_ASSIGN_OR_RAISE(auto name, parse_name());

  auto node = std::make_unique<XmlNode>();
  node->name = std::move(name);
  node->start_offset = start_offset;

  for (;;) {
    skip_ws();
    if (pos_ >= input_.size()) {
      return invalid("unterminated start tag");
    }
    if (starts_with("/>")) {
      pos_ += 2;
      node->end_offset = pos_;
      return node;
    }
    if (input_[pos_] == '>') {
      pos_++;
      break;
    }
    SAN_ASSIGN_OR_RAISE(auto attr, parse_attribute());
    node->attrs.push_back(std::move(attr));
  }

  for (;;) {
    if (pos_ >= input_.size()) {
      return invalid("unterminated element");
    }
    if (starts_with("</")) {
      pos_ += 2;
      SAN_ASSIGN_OR_RAISE(auto close_name, parse_name());
      skip_ws();
      if (pos_ >= input_.size() || input_[pos_] != '>') {
        return invalid("expected '>' after closing tag");
      }
      pos_++;
      if (close_name != node->name) {
        return sanitize::Status::Invalid("XML parse error at byte ", pos_,
                                         ": closing tag </", close_name,
                                         "> does not match <", node->name, ">");
      }
      node->end_offset = pos_;
      return node;
    }
    if (starts_with("<!--")) {
      pos_ += 4;
      SAN_RETURN_NOT_OK(skip_until("-->"));
      continue;
    }
    if (starts_with("<?")) {
      pos_ += 2;
      SAN_RETURN_NOT_OK(skip_until("?>"));
      continue;
    }
    if (starts_with("<![CDATA[")) {
      pos_ += 9;
      const std::size_t start = pos_;
      const std::size_t found = input_.find("]]>", pos_);
      if (found == std::string_view::npos) {
        return invalid("unterminated CDATA");
      }
      node->text.append(input_.substr(start, found - start));
      pos_ = found + 3;
      continue;
    }
    if (starts_with("<!")) {
      SAN_RETURN_NOT_OK(skip_bang_markup());
      continue;
    }
    if (input_[pos_] == '<') {
      SAN_ASSIGN_OR_RAISE(auto child, parse_element());
      node->children.push_back(std::move(child));
      continue;
    }

    const std::size_t start = pos_;
    while (pos_ < input_.size() && input_[pos_] != '<') {
      pos_++;
    }
    node->text.append(decode_xml_entities(input_.substr(start, pos_ - start)));
  }
}

} // namespace sanitize::internal

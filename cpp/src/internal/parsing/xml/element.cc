// Implements recursive XML element and content parsing.

#include "internal/parsing/xml/document.hh"

#include "internal/parsing/xml_entities.hh"

#include <utility>

namespace sanitize::internal {

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

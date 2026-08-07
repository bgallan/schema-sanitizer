// Implements iterative XML element and content parsing.

#include "internal/parsing/xml/document.hh"

#include "internal/parsing/xml/token_match.hh"

#include <algorithm>
#include <memory_resource>
#include <utility>

namespace sanitize::internal {

sanitize::Result<XmlNodePtr> XmlParser::parse_element() {
  using StartTag = std::pair<XmlNodePtr, bool>;
  auto parse_start_tag = [this]() -> sanitize::Result<StartTag> {
    if (pos_ >= input_.size() || input_[pos_] != '<') {
      return invalid("expected element");
    }
    if (starts_with("</")) {
      return invalid("unexpected closing tag");
    }
    if (starts_with("<?") || starts_with("<!")) {
      return invalid("expected element start tag");
    }

    const std::size_t start_offset = pos_;
    ++pos_;
    SAN_ASSIGN_OR_RAISE(auto name, parse_name());
    SAN_RETURN_NOT_OK(register_node(start_offset));

    auto node = make_xml_node(resource_);
    node->name = std::move(name);
    node->start_offset = start_offset;

    for (;;) {
      skip_ws();
      if (pos_ >= input_.size()) {
        return invalid_at(start_offset, "unterminated start tag");
      }
      if (starts_with("/>")) {
        pos_ += 2U;
        node->end_offset = pos_;
        return StartTag{std::move(node), true};
      }
      if (input_[pos_] == '>') {
        ++pos_;
        return StartTag{std::move(node), false};
      }

      const std::size_t attribute_offset = pos_;
      SAN_RETURN_NOT_OK(
          register_attribute(attribute_offset, node->attrs.size()));
      SAN_ASSIGN_OR_RAISE(auto attribute, parse_attribute());
      for (const auto &existing : node->attrs) {
        if (existing.name == attribute.name) {
          return invalid_at(attribute_offset,
                            "duplicate attribute on the same element");
        }
      }
      node->attrs.push_back(std::move(attribute));
    }
  };

  SAN_ASSIGN_OR_RAISE(auto root_tag, parse_start_tag());
  auto root = std::move(root_tag.first);
  if (root_tag.second) {
    return root;
  }

  std::pmr::vector<XmlNode *> stack(resource_);
  stack.reserve(32U);
  stack.push_back(root.get());
  max_depth_ = std::max<std::size_t>(max_depth_, 1U);

  while (!stack.empty()) {
    XmlNode *node = stack.back();
    if (pos_ >= input_.size()) {
      return invalid_at(node->start_offset, "unterminated element");
    }

    if (starts_with("</")) {
      const std::size_t close_offset = pos_;
      pos_ += 2U;
      SAN_ASSIGN_OR_RAISE(auto close_name, parse_name());
      skip_ws();
      if (pos_ >= input_.size() || input_[pos_] != '>') {
        return invalid("expected '>' after closing tag");
      }
      ++pos_;
      if (close_name != node->name) {
        return sanitize::Status::Invalid(
            "XML parse error at byte ", base_offset_ + close_offset,
            ": closing tag does not match the open element");
      }
      node->end_offset = pos_;
      stack.pop_back();
      continue;
    }

    if (starts_with("<!--")) {
      SAN_RETURN_NOT_OK(skip_comment());
      continue;
    }
    if (starts_with("<?")) {
      SAN_RETURN_NOT_OK(skip_processing_instruction());
      continue;
    }
    if (starts_with("<![CDATA[")) {
      const std::size_t cdata_offset = pos_;
      pos_ += 9U;
      const std::size_t content_start = pos_;
      const std::size_t close = input_.find("]]>", pos_);
      if (close == std::string_view::npos) {
        return invalid_at(cdata_offset, "unterminated CDATA section");
      }
      const std::size_t content_size = close - content_start;
      if (content_size >
          xml_tokens::kMaxXmlDecodedBytes -
              std::min(decoded_bytes_, xml_tokens::kMaxXmlDecodedBytes)) {
        return invalid_at(content_start,
                          "decoded XML text exceeds internal safety limit");
      }
      decoded_bytes_ += content_size;
      node->text.append(input_.substr(content_start, content_size));
      pos_ = close + 3U;
      continue;
    }
    if (starts_with("<!")) {
      SAN_RETURN_NOT_OK(skip_bang_markup());
      continue;
    }

    if (input_[pos_] == '<') {
      if (stack.size() >= xml_tokens::kMaxXmlNestingDepth) {
        return sanitize::Status::Invalid(
            "XML parse error at byte ", base_offset_ + pos_,
            ": XML nesting depth ", stack.size() + 1U,
            " exceeds internal safety limit ", xml_tokens::kMaxXmlNestingDepth);
      }
      SAN_ASSIGN_OR_RAISE(auto child_tag, parse_start_tag());
      max_depth_ = std::max(max_depth_, stack.size() + 1U);
      XmlNode *child = child_tag.first.get();
      node->children.push_back(std::move(child_tag.first));
      if (!child_tag.second) {
        stack.push_back(child);
      }
      continue;
    }

    const std::size_t text_start = pos_;
    while (pos_ < input_.size() && input_[pos_] != '<') {
      ++pos_;
    }
    const std::string_view encoded =
        input_.substr(text_start, pos_ - text_start);
    const std::size_t forbidden = encoded.find("]]>");
    if (forbidden != std::string_view::npos) {
      return invalid_at(text_start + forbidden,
                        "']]>' is only allowed to terminate CDATA");
    }
    SAN_RETURN_NOT_OK(append_decoded(&node->text, encoded, text_start));
  }
  return root;
}

} // namespace sanitize::internal

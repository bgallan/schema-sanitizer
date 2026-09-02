// Implements XML document parser lifecycle, allocation, and cursor helpers.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/xml/document.hh"

#include "internal/parsing/xml/token_match.hh"
#include "internal/parsing/xml_entities.hh"

#include <algorithm>
#include <memory_resource>
#include <new>

namespace sanitize::internal {

/// Destroys and deallocates an XML node through the same polymorphic resource
/// that created it.
void XmlNodeDeleter::operator()(XmlNode *node) const noexcept {
  if (!node) {
    return;
  }
  auto *actual_resource =
      resource ? resource : std::pmr::get_default_resource();
  std::pmr::polymorphic_allocator<XmlNode> allocator(actual_resource);
  std::allocator_traits<decltype(allocator)>::destroy(allocator, node);
  allocator.deallocate(node, 1U);
}

XmlNodePtr make_xml_node(std::pmr::memory_resource *resource) {
  auto *actual_resource =
      resource ? resource : std::pmr::get_default_resource();
  std::pmr::polymorphic_allocator<XmlNode> allocator(actual_resource);
  XmlNode *node = allocator.allocate(1U);
  try {
    std::allocator_traits<decltype(allocator)>::construct(allocator, node,
                                                          actual_resource);
  } catch (...) {
    allocator.deallocate(node, 1U);
    throw;
  }
  return XmlNodePtr(node, XmlNodeDeleter{actual_resource});
}

XmlParser::XmlParser(std::string_view input,
                     std::pmr::memory_resource *resource,
                     std::size_t base_offset)
    : input_(input),
      resource_(resource ? resource : std::pmr::get_default_resource()),
      base_offset_(base_offset) {}

sanitize::Result<XmlNodePtr> XmlParser::parse_document() {
  try {
    SAN_RETURN_NOT_OK(validate_xml_utf8(input_, base_offset_));
    SAN_RETURN_NOT_OK(skip_misc());
    if (pos_ >= input_.size()) {
      return sanitize::Status::Invalid("XML parse error at byte ", base_offset_,
                                       ": empty document");
    }
    SAN_ASSIGN_OR_RAISE(auto root, parse_element());
    SAN_RETURN_NOT_OK(skip_misc());
    skip_ws();
    if (pos_ != input_.size()) {
      return invalid("trailing content after root element");
    }
    return root;
  } catch (const std::bad_alloc &) {
    return out_of_memory("document parsing");
  }
}

bool XmlParser::starts_with(std::string_view token) const {
  return input_.substr(pos_, token.size()) == token;
}

sanitize::Status XmlParser::invalid(std::string_view message) const {
  return invalid_at(pos_, message);
}

sanitize::Status XmlParser::invalid_at(std::size_t position,
                                       std::string_view message) const {
  return sanitize::Status::Invalid("XML parse error at byte ",
                                   base_offset_ + position, ": ", message);
}

sanitize::Status XmlParser::out_of_memory(std::string_view stage) const {
  return sanitize::Status::OutOfMemory("XML allocation failed during ", stage,
                                       " at byte ", base_offset_ + pos_);
}

void XmlParser::skip_ws() {
  while (pos_ < input_.size() && xml_tokens::is_xml_whitespace(input_[pos_])) {
    ++pos_;
  }
}

sanitize::Status XmlParser::register_node(std::size_t offset) {
  if (node_count_ >= xml_tokens::kMaxXmlNodes) {
    return invalid_at(offset, "XML node count exceeds internal safety limit");
  }
  ++node_count_;
  return sanitize::Status::OK();
}

sanitize::Status XmlParser::register_attribute(std::size_t offset,
                                               std::size_t element_attributes) {
  if (element_attributes >= xml_tokens::kMaxXmlAttributesPerElement) {
    return invalid_at(
        offset, "XML attributes per element exceed internal safety limit");
  }
  if (attribute_count_ >= xml_tokens::kMaxXmlTotalAttributes) {
    return invalid_at(
        offset, "XML total attribute count exceeds internal safety limit");
  }
  ++attribute_count_;
  return sanitize::Status::OK();
}

sanitize::Status XmlParser::append_decoded(std::pmr::string *target,
                                           std::string_view encoded,
                                           std::size_t encoded_offset) {
  SAN_ASSIGN_OR_RAISE(
      auto decoded,
      decode_xml_entities(encoded, resource_, base_offset_ + encoded_offset));
  if (decoded.size() >
      xml_tokens::kMaxXmlDecodedBytes -
          std::min(decoded_bytes_, xml_tokens::kMaxXmlDecodedBytes)) {
    return invalid_at(encoded_offset,
                      "decoded XML text exceeds internal safety limit");
  }
  decoded_bytes_ += decoded.size();
  target->append(decoded);
  return sanitize::Status::OK();
}

} // namespace sanitize::internal

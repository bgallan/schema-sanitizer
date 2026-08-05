// Declares the hardened XML document model and parser.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string_view>
#include <vector>

#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"

namespace sanitize::internal {

struct XmlNode;

struct XmlNodeDeleter {
  std::pmr::memory_resource *resource = std::pmr::get_default_resource();
  void operator()(XmlNode *node) const noexcept;
};

using XmlNodePtr = std::unique_ptr<XmlNode, XmlNodeDeleter>;

struct XmlArrayGroup {
  explicit XmlArrayGroup(std::pmr::memory_resource *resource)
      : elements(resource) {}

  std::pmr::vector<const XmlNode *> elements;
};

struct XmlField {
  enum class Kind : std::uint8_t { kScalar, kNode, kGroup };

  explicit XmlField(std::pmr::memory_resource *resource)
      : key(resource), scalar(resource) {}

  std::pmr::string key;
  std::uint64_t key_hash = 0;
  Kind kind = Kind::kScalar;
  std::pmr::string scalar;
  const XmlNode *node = nullptr;
  const XmlArrayGroup *group = nullptr;
};

struct XmlAttribute {
  explicit XmlAttribute(std::pmr::memory_resource *resource)
      : name(resource), value(resource) {}

  std::pmr::string name;
  std::pmr::string value;
};

struct XmlNode {
  explicit XmlNode(std::pmr::memory_resource *memory_resource)
      : resource(memory_resource ? memory_resource
                                 : std::pmr::get_default_resource()),
        name(this->resource), text(this->resource), attrs(this->resource),
        children(this->resource), scalar(this->resource),
        groups(this->resource), fields(this->resource) {}

  std::pmr::memory_resource *resource;
  std::pmr::string name;
  std::pmr::string text;
  std::pmr::vector<XmlAttribute> attrs;
  std::pmr::vector<XmlNodePtr> children;
  std::size_t start_offset = 0;
  std::size_t end_offset = 0;

  bool scalar_only = false;
  bool scalar_is_null = true;
  std::pmr::string scalar;
  std::pmr::vector<XmlArrayGroup> groups;
  std::pmr::vector<XmlField> fields;
};

/// Allocate one XML node from the supplied operation memory resource.
XmlNodePtr make_xml_node(std::pmr::memory_resource *resource);

/// Parses a bounded XML document into the lightweight XML node model.
class XmlParser {
public:
  /// Store the XML document bytes, allocation resource, and source base offset.
  explicit XmlParser(
      std::string_view input,
      std::pmr::memory_resource *resource = std::pmr::get_default_resource(),
      std::size_t base_offset = 0);

  /// Parse the whole XML document and return its root node.
  sanitize::Result<XmlNodePtr> parse_document();

  [[nodiscard]] std::size_t node_count() const noexcept { return node_count_; }
  [[nodiscard]] std::size_t decoded_bytes() const noexcept {
    return decoded_bytes_;
  }
  [[nodiscard]] std::size_t max_depth() const noexcept { return max_depth_; }

private:
  [[nodiscard]] bool starts_with(std::string_view token) const;
  [[nodiscard]] bool starts_with_ascii_ci(std::string_view token) const;
  sanitize::Status invalid(std::string_view message) const;
  sanitize::Status invalid_at(std::size_t position,
                              std::string_view message) const;
  sanitize::Status out_of_memory(std::string_view stage) const;
  void skip_ws();
  sanitize::Status skip_until(std::string_view token);
  sanitize::Status skip_comment();
  sanitize::Status skip_processing_instruction();
  sanitize::Status skip_bang_markup();
  sanitize::Status skip_misc();
  sanitize::Result<std::pmr::string> parse_name();
  sanitize::Result<std::pmr::string> parse_quoted_value();
  sanitize::Result<XmlAttribute> parse_attribute();
  sanitize::Result<XmlNodePtr> parse_element();
  sanitize::Status append_decoded(std::pmr::string *target,
                                  std::string_view encoded,
                                  std::size_t encoded_offset);
  sanitize::Status register_node(std::size_t offset);
  sanitize::Status register_attribute(std::size_t offset,
                                      std::size_t element_attributes);

  std::string_view input_;
  std::pmr::memory_resource *resource_ = std::pmr::get_default_resource();
  std::size_t base_offset_ = 0;
  std::size_t pos_ = 0;
  std::size_t node_count_ = 0;
  std::size_t attribute_count_ = 0;
  std::size_t decoded_bytes_ = 0;
  std::size_t max_depth_ = 0;
};

/// Convert one XML field to a generic value view.
ValueView xml_field_to_value(const XmlField &field);

/// Convert an XML node to a scalar, object, or null value view.
ValueView xml_node_to_value(const XmlNode *node);

/// Build scalar/object/array field projections using iterative post-order.
sanitize::Status build_xml_node_model(XmlNode *node);

} // namespace sanitize::internal

// Declares the lightweight XML document model and parser.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "sanitize/core/status.hh"
#include "sanitize/core/value_view.hh"

namespace sanitize::internal {

struct XmlNode;

struct XmlArrayGroup {
  std::vector<const XmlNode *> elements;
};

struct XmlField {
  enum class Kind : std::uint8_t { kScalar, kNode, kGroup };

  std::string key;
  std::uint64_t key_hash = 0;
  Kind kind;
  std::string scalar;
  const XmlNode *node = nullptr;
  const XmlArrayGroup *group = nullptr;
};

struct XmlAttribute {
  std::string name;
  std::string value;
};

struct XmlNode {
  std::string name;
  std::string text;
  std::vector<XmlAttribute> attrs;
  std::vector<std::unique_ptr<XmlNode>> children;
  std::size_t start_offset = 0;
  std::size_t end_offset = 0;

  bool scalar_only = false;
  bool scalar_is_null = true;
  std::string scalar;
  std::vector<std::unique_ptr<XmlArrayGroup>> groups;
  std::vector<XmlField> fields;
};

/// Parses a bounded XML document into the lightweight XML node model.
class XmlParser {
public:
  /// Store the XML document bytes to parse.
  explicit XmlParser(std::string_view input);

  /// Parse the whole XML document and return its root node.
  sanitize::Result<std::unique_ptr<XmlNode>> parse_document();

private:
  [[nodiscard]] bool starts_with(std::string_view token) const;
  [[nodiscard]] bool starts_with_ascii_ci(std::string_view token) const;
  sanitize::Status invalid(std::string_view message) const;
  void skip_ws();
  sanitize::Status skip_until(std::string_view token);
  sanitize::Status skip_bang_markup();
  sanitize::Status skip_misc();
  sanitize::Result<std::string> parse_name();
  sanitize::Result<std::string> parse_quoted_value();
  sanitize::Result<XmlAttribute> parse_attribute();
  sanitize::Result<std::unique_ptr<XmlNode>> parse_element();

  std::string_view input_;
  std::size_t pos_ = 0;
};

/// Convert one XML field to a generic value view.
ValueView xml_field_to_value(const XmlField &field);

/// Convert an XML node to a scalar, object, or null value view.
ValueView xml_node_to_value(const XmlNode *node);

/// Build scalar/object/array field projections for a parsed XML node tree.
void build_xml_node_model(XmlNode *node);

} // namespace sanitize::internal

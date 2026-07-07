// Builds XML node projections exposed through generic ValueView objects.

#include "internal/parsing/xml_document.hh"

#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "sanitize/detail/hash.hh"

namespace sanitize::internal {
namespace {

/// Return a copy of text with XML-significant surrounding whitespace removed.
std::string trim_copy(std::string_view text) {
  auto is_ws = [](unsigned char c) { return std::isspace(c) != 0; };
  while (!text.empty() && is_ws(static_cast<unsigned char>(text.front()))) {
    text.remove_prefix(1);
  }
  while (!text.empty() && is_ws(static_cast<unsigned char>(text.back()))) {
    text.remove_suffix(1);
  }
  return std::string(text);
}

/// Visit XML array-group elements through the generic value-view ABI.
sanitize::Status xml_array_for_each(const void *self, void *ctx,
                                    ValueView::ArrayEachFn fn) {
  auto *group = static_cast<const XmlArrayGroup *>(self);
  for (const XmlNode *node : group->elements) {
    SAN_RETURN_NOT_OK(fn(ctx, xml_node_to_value(node)));
  }
  return sanitize::Status::OK();
}

/// Visit XML object fields through the generic value-view ABI.
sanitize::Status xml_object_for_each(const void *self, void *ctx,
                                     ValueView::ObjectEachFn fn) {
  auto *node = static_cast<const XmlNode *>(self);
  for (const XmlField &field : node->fields) {
    const std::string_view key(field.key);
    SAN_RETURN_NOT_OK(fn(ctx, key, sanitize::detail::hash_key64(key),
                         xml_field_to_value(field)));
  }
  return sanitize::Status::OK();
}

const ValueView::ArrayVTable kXmlArrayVTable{.for_each = &xml_array_for_each};
const ValueView::ObjectVTable kXmlObjectVTable{.for_each =
                                                   &xml_object_for_each};

} // namespace

ValueView xml_field_to_value(const XmlField &field) {
  switch (field.kind) {
  case XmlField::Kind::kScalar:
    return ValueView::String(field.scalar);
  case XmlField::Kind::kNode:
    return xml_node_to_value(field.node);
  case XmlField::Kind::kGroup:
    return ValueView::ArrayView(field.group, &kXmlArrayVTable);
  }
  return ValueView::Null();
}

ValueView xml_node_to_value(const XmlNode *node) {
  if (!node) {
    return ValueView::Null();
  }
  if (node->scalar_only) {
    return node->scalar_is_null ? ValueView::Null()
                                : ValueView::String(node->scalar);
  }
  return ValueView::ObjectView(node, &kXmlObjectVTable);
}

void build_xml_node_model(XmlNode *node) {
  for (auto &child : node->children) {
    build_xml_node_model(child.get());
  }

  const std::string text = trim_copy(node->text);
  if (node->attrs.empty() && node->children.empty()) {
    node->scalar_only = true;
    node->scalar_is_null = text.empty();
    node->scalar = text;
    return;
  }

  node->scalar_only = false;
  node->fields.reserve(node->attrs.size() + node->children.size() +
                       (text.empty() ? 0u : 1u));
  for (const auto &attr : node->attrs) {
    node->fields.push_back(XmlField{
        .key = "@" + attr.name,
        .kind = XmlField::Kind::kScalar,
        .scalar = attr.value,
    });
  }
  if (!text.empty()) {
    node->fields.push_back(XmlField{
        .key = "#text",
        .kind = XmlField::Kind::kScalar,
        .scalar = text,
    });
  }

  if (node->children.size() == 1) {
    const XmlNode *child = node->children.front().get();
    node->fields.push_back(XmlField{
        .key = child->name,
        .kind = XmlField::Kind::kNode,
        .scalar = {},
        .node = child,
    });
    return;
  }

  std::vector<std::string_view> order;
  order.reserve(node->children.size());
  std::unordered_map<std::string_view, std::vector<const XmlNode *>> by_name;
  by_name.reserve(node->children.size());
  for (const auto &child : node->children) {
    const std::string_view name(child->name);
    auto [it, inserted] = by_name.try_emplace(name);
    if (inserted) {
      order.push_back(name);
    }
    it->second.push_back(child.get());
  }

  for (std::string_view name : order) {
    const auto &items = by_name[name];
    if (items.size() == 1) {
      node->fields.push_back(XmlField{
          .key = std::string(name),
          .kind = XmlField::Kind::kNode,
          .scalar = {},
          .node = items.front(),
      });
      continue;
    }
    auto group = std::make_unique<XmlArrayGroup>();
    group->elements = items;
    const XmlArrayGroup *group_ptr = group.get();
    node->groups.push_back(std::move(group));
    node->fields.push_back(XmlField{
        .key = std::string(name),
        .kind = XmlField::Kind::kGroup,
        .scalar = {},
        .node = nullptr,
        .group = group_ptr,
    });
  }
}

} // namespace sanitize::internal

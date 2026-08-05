// Builds XML node projections exposed through generic ValueView objects.

#include "internal/parsing/xml/document.hh"

#include "internal/parsing/xml/token_match.hh"

#include <cstddef>
#include <memory_resource>
#include <new>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "sanitize/detail/hash.hh"

namespace sanitize::internal {
namespace {

std::string_view trim_xml_whitespace(std::string_view text) noexcept {
  while (!text.empty() && xml_tokens::is_xml_whitespace(text.front())) {
    text.remove_prefix(1U);
  }
  while (!text.empty() && xml_tokens::is_xml_whitespace(text.back())) {
    text.remove_suffix(1U);
  }
  return text;
}

void append_scalar_field(XmlNode *node, std::string_view key,
                         std::string_view value) {
  XmlField field(node->resource);
  field.key.assign(key);
  field.key_hash = sanitize::detail::hash_key64(field.key);
  field.kind = XmlField::Kind::kScalar;
  field.scalar.assign(value);
  node->fields.push_back(std::move(field));
}

void append_node_field(XmlNode *node, std::string_view key,
                       const XmlNode *child) {
  XmlField field(node->resource);
  field.key.assign(key);
  field.key_hash = sanitize::detail::hash_key64(field.key);
  field.kind = XmlField::Kind::kNode;
  field.node = child;
  node->fields.push_back(std::move(field));
}

void append_group_field(XmlNode *node, std::string_view key,
                        const XmlArrayGroup *group) {
  XmlField field(node->resource);
  field.key.assign(key);
  field.key_hash = sanitize::detail::hash_key64(field.key);
  field.kind = XmlField::Kind::kGroup;
  field.group = group;
  node->fields.push_back(std::move(field));
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
    SAN_RETURN_NOT_OK(
        fn(ctx, field.key, field.key_hash, xml_field_to_value(field)));
  }
  return sanitize::Status::OK();
}

const ValueView::ArrayVTable kXmlArrayVTable{.for_each = &xml_array_for_each};
const ValueView::ObjectVTable kXmlObjectVTable{.for_each =
                                                   &xml_object_for_each};

struct GroupAccumulator {
  GroupAccumulator(std::string_view group_name,
                   std::pmr::memory_resource *resource)
      : name(group_name), elements(resource) {}

  std::string_view name;
  std::pmr::vector<const XmlNode *> elements;
};

sanitize::Status build_one_xml_node(XmlNode *node) {
  const std::string_view text = trim_xml_whitespace(node->text);
  node->groups.clear();
  node->fields.clear();
  node->scalar.clear();

  if (node->attrs.empty() && node->children.empty()) {
    node->scalar_only = true;
    node->scalar_is_null = text.empty();
    node->scalar.assign(text);
    return sanitize::Status::OK();
  }

  node->scalar_only = false;
  node->scalar_is_null = false;
  node->fields.reserve(node->attrs.size() + node->children.size() +
                       (text.empty() ? 0U : 1U));
  for (const auto &attribute : node->attrs) {
    std::pmr::string key(node->resource);
    key.reserve(attribute.name.size() + 1U);
    key.push_back('@');
    key.append(attribute.name);
    append_scalar_field(node, key, attribute.value);
  }
  if (!text.empty()) {
    append_scalar_field(node, "#text", text);
  }

  if (node->children.size() == 1U) {
    const XmlNode *child = node->children.front().get();
    append_node_field(node, child->name, child);
    return sanitize::Status::OK();
  }

  std::pmr::vector<GroupAccumulator> groups(node->resource);
  groups.reserve(node->children.size());
  std::pmr::unordered_map<std::string_view, std::size_t> group_index(
      node->resource);
  group_index.reserve(node->children.size());
  for (const auto &child : node->children) {
    const std::string_view name(child->name);
    auto [iterator, inserted] = group_index.try_emplace(name, groups.size());
    if (inserted) {
      groups.emplace_back(name, node->resource);
    }
    groups[iterator->second].elements.push_back(child.get());
  }

  std::size_t repeated_groups = 0;
  for (const auto &group : groups) {
    if (group.elements.size() > 1U) {
      ++repeated_groups;
    }
  }
  node->groups.reserve(repeated_groups);
  for (const auto &group : groups) {
    if (group.elements.size() == 1U) {
      append_node_field(node, group.name, group.elements.front());
      continue;
    }
    node->groups.emplace_back(node->resource);
    node->groups.back().elements.assign(group.elements.begin(),
                                        group.elements.end());
    append_group_field(node, group.name, &node->groups.back());
  }
  return sanitize::Status::OK();
}

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

sanitize::Status build_xml_node_model(XmlNode *node) {
  if (!node) {
    return sanitize::Status::Invalid("XML model root is null");
  }
  try {
    using Visit = std::pair<XmlNode *, bool>;
    std::pmr::vector<Visit> stack(node->resource);
    stack.reserve(64U);
    stack.emplace_back(node, false);
    while (!stack.empty()) {
      const Visit visit = stack.back();
      stack.pop_back();
      if (visit.second) {
        SAN_RETURN_NOT_OK(build_one_xml_node(visit.first));
        continue;
      }
      stack.emplace_back(visit.first, true);
      for (auto iterator = visit.first->children.rbegin();
           iterator != visit.first->children.rend(); ++iterator) {
        stack.emplace_back(iterator->get(), false);
      }
    }
    return sanitize::Status::OK();
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "XML model construction allocation failed");
  }
}

} // namespace sanitize::internal

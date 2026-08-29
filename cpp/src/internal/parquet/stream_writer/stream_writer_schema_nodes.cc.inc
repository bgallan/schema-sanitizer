// Maps recursive Arrow schema fields to validated Parquet nodes and physical
// leaf columns. This fragment is included by the consolidated writer to isolate
// format-specific details.

/// Converts a supported primitive Arrow field into a Parquet leaf schema node.
sanitize::Result<ParquetNode>
primitive_node_from_field(const jsonl::JsonlField &field, std::string name) {
  ParquetNode node;
  node.node_kind = NodeKind::kPrimitive;
  node.arrow_kind = field.kind;
  node.name = std::move(name);
  node.required = !field.nullable;
  auto set_int_logical = [&](std::int8_t bit_width, bool is_signed,
                             ConvertedType converted_type) {
    node.has_converted_type = true;
    node.converted_type = converted_type;
    node.has_int_logical_type = true;
    node.int_bit_width = bit_width;
    node.int_is_signed = is_signed;
  };
  switch (field.kind) {
  case jsonl::JsonlKind::kNull:
    node.physical_type = PhysicalType::kInt32;
    node.has_null_logical_type = true;
    return node;
  case jsonl::JsonlKind::kBool:
    node.physical_type = PhysicalType::kBoolean;
    return node;
  case jsonl::JsonlKind::kInt8:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(8, true, ConvertedType::kInt8);
    return node;
  case jsonl::JsonlKind::kUInt8:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(8, false, ConvertedType::kUInt8);
    return node;
  case jsonl::JsonlKind::kInt16:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(16, true, ConvertedType::kInt16);
    return node;
  case jsonl::JsonlKind::kUInt16:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(16, false, ConvertedType::kUInt16);
    return node;
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kDate32:
    node.physical_type = PhysicalType::kInt32;
    if (field.kind == jsonl::JsonlKind::kDate32) {
      node.has_converted_type = true;
      node.converted_type = ConvertedType::kDate;
    }
    return node;
  case jsonl::JsonlKind::kTime32s:
    node.physical_type = PhysicalType::kInt32;
    node.has_converted_type = true;
    node.converted_type = ConvertedType::kTimeMillis;
    node.has_time_millis_logical_type = true;
    return node;
  case jsonl::JsonlKind::kUInt32:
    node.physical_type = PhysicalType::kInt32;
    set_int_logical(32, false, ConvertedType::kUInt32);
    return node;
  case jsonl::JsonlKind::kInt64:
    node.physical_type = PhysicalType::kInt64;
    return node;
  case jsonl::JsonlKind::kUInt64:
    node.physical_type = PhysicalType::kInt64;
    set_int_logical(64, false, ConvertedType::kUInt64);
    return node;
  case jsonl::JsonlKind::kFloat32:
    node.physical_type = PhysicalType::kFloat;
    return node;
  case jsonl::JsonlKind::kFloat64:
    node.physical_type = PhysicalType::kDouble;
    return node;
  case jsonl::JsonlKind::kString:
  case jsonl::JsonlKind::kLargeString:
  case jsonl::JsonlKind::kBinary:
  case jsonl::JsonlKind::kLargeBinary:
    node.physical_type = PhysicalType::kByteArray;
    if (field.kind == jsonl::JsonlKind::kString ||
        field.kind == jsonl::JsonlKind::kLargeString) {
      node.has_converted_type = true;
      node.converted_type = ConvertedType::kUtf8;
    }
    return node;
  case jsonl::JsonlKind::kFixedSizeBinary:
    if (field.fixed_size_binary_size <= 0) {
      return sanitize::Status::NotImplemented(
          kUnsupportedPrefix, " Arrow fixed-size binary field '", field.name,
          "' has unsupported format '", field.format, "'");
    }
    node.physical_type = PhysicalType::kFixedLenByteArray;
    node.fixed_binary_byte_width = field.fixed_size_binary_size;
    return node;
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
    node.physical_type = PhysicalType::kInt64;
    node.has_timestamp_logical_type = true;
    return node;
  case jsonl::JsonlKind::kDecimal:
    if (field.decimal_precision <= 0 ||
        (field.decimal_byte_width != 16 && field.decimal_byte_width != 32)) {
      return sanitize::Status::NotImplemented(
          kUnsupportedPrefix, " Arrow decimal field '", field.name,
          "' has unsupported format '", field.format, "'");
    }
    node.physical_type = PhysicalType::kFixedLenByteArray;
    node.has_converted_type = true;
    node.converted_type = ConvertedType::kDecimal;
    node.has_decimal_metadata = true;
    node.decimal_precision = field.decimal_precision;
    node.decimal_scale = field.decimal_scale;
    node.decimal_byte_width = field.decimal_byte_width;
    node.fixed_binary_byte_width = field.decimal_byte_width;
    return node;
  default:
    return sanitize::Status::NotImplemented(
        kUnsupportedPrefix, " Arrow field '", field.name,
        "' has unsupported format '", field.format, "'");
  }
}

// Dictionary and nested Arrow-to-Parquet schema node conversion.

/// Converts one supported Arrow schema field into a recursive Parquet node.
sanitize::Result<ParquetNode> node_from_field(const jsonl::JsonlField &field,
                                              std::string name);

/// Converts a supported Arrow dictionary field into a plain Parquet leaf.
sanitize::Result<ParquetNode>
dictionary_node_from_field(const jsonl::JsonlField &field, std::string name) {
  if (field.children.size() != 1) {
    return sanitize::Status::NotImplemented(kUnsupportedPrefix,
                                            " dictionary field '", field.name,
                                            "' must have one value child");
  }
  SAN_ASSIGN_OR_RAISE(auto value_node,
                      node_from_field(field.children.front(), std::move(name)));
  if (value_node.node_kind != NodeKind::kPrimitive ||
      value_node.arrow_kind == jsonl::JsonlKind::kDictionary) {
    return sanitize::Status::NotImplemented(
        kUnsupportedPrefix, " dictionary field '", field.name,
        "' has unsupported value format '", field.children.front().format, "'");
  }
  value_node.dictionary_value_kind = value_node.arrow_kind;
  value_node.arrow_kind = jsonl::JsonlKind::kDictionary;
  value_node.dictionary_index_kind = field.dictionary_index_kind;
  return value_node;
}

/// Converts a supported Arrow struct field into a Parquet group node.
sanitize::Result<ParquetNode>
struct_node_from_field(const jsonl::JsonlField &field, std::string name) {
  ParquetNode node;
  node.node_kind = name == "schema" ? NodeKind::kRoot : NodeKind::kStruct;
  node.arrow_kind = field.kind;
  node.name = std::move(name);
  node.required = node.node_kind == NodeKind::kRoot || !field.nullable;
  node.children.reserve(field.children.size());
  for (const auto &child : field.children) {
    SAN_ASSIGN_OR_RAISE(auto child_node, node_from_field(child, child.name));
    node.children.push_back(std::move(child_node));
  }
  return node;
}

/// Converts a supported Arrow list field into a Parquet LIST node.
sanitize::Result<ParquetNode>
list_node_from_field(const jsonl::JsonlField &field, std::string name) {
  if (field.children.size() != 1) {
    return sanitize::Status::NotImplemented(kUnsupportedPrefix, " list field '",
                                            field.name,
                                            "' must have one child");
  }
  const auto &value = field.children.front();
  ParquetNode node;
  node.node_kind = NodeKind::kList;
  node.arrow_kind = field.kind;
  node.name = std::move(name);
  node.required = !field.nullable;
  node.fixed_size_list_size = field.fixed_size_list_size;
  SAN_ASSIGN_OR_RAISE(auto element, node_from_field(value, value.name));
  node.element = std::make_unique<ParquetNode>(std::move(element));
  return node;
}

/// Converts a supported Arrow map field into a Parquet MAP node.
sanitize::Result<ParquetNode>
map_node_from_field(const jsonl::JsonlField &field, std::string name) {
  if (field.children.size() != 1 ||
      field.children.front().kind != jsonl::JsonlKind::kStruct ||
      field.children.front().children.size() != 2) {
    return sanitize::Status::NotImplemented(
        kUnsupportedPrefix, " map field '", field.name,
        "' does not have key/value entry children");
  }
  const auto &entry = field.children.front();
  const auto &key = entry.children[0];
  const auto &value = entry.children[1];
  if (key.kind == jsonl::JsonlKind::kList ||
      key.kind == jsonl::JsonlKind::kLargeList ||
      key.kind == jsonl::JsonlKind::kFixedSizeList ||
      key.kind == jsonl::JsonlKind::kMap ||
      key.kind == jsonl::JsonlKind::kStruct) {
    return sanitize::Status::NotImplemented(
        kUnsupportedPrefix, " map field '", field.name,
        "' has unsupported key format '", key.format, "'");
  }

  ParquetNode node;
  node.node_kind = NodeKind::kMap;
  node.arrow_kind = field.kind;
  node.name = std::move(name);
  node.required = !field.nullable;
  auto entry_node = std::make_unique<ParquetNode>();
  entry_node->node_kind = NodeKind::kStruct;
  entry_node->arrow_kind = entry.kind;
  entry_node->name = "key_value";
  entry_node->required = true;
  entry_node->children.reserve(2);
  SAN_ASSIGN_OR_RAISE(auto key_node, node_from_field(key, "key"));
  key_node.required = true;
  SAN_ASSIGN_OR_RAISE(auto value_node, node_from_field(value, "value"));
  entry_node->children.push_back(std::move(key_node));
  entry_node->children.push_back(std::move(value_node));
  node.element = std::move(entry_node);
  return node;
}

sanitize::Result<ParquetNode> node_from_field(const jsonl::JsonlField &field,
                                              std::string name) {
  if (field.kind == jsonl::JsonlKind::kDictionary) {
    return dictionary_node_from_field(field, std::move(name));
  }
  if (field.kind == jsonl::JsonlKind::kStruct) {
    return struct_node_from_field(field, std::move(name));
  }
  if (field.kind == jsonl::JsonlKind::kList ||
      field.kind == jsonl::JsonlKind::kLargeList ||
      field.kind == jsonl::JsonlKind::kFixedSizeList) {
    return list_node_from_field(field, std::move(name));
  }
  if (field.kind == jsonl::JsonlKind::kMap) {
    return map_node_from_field(field, std::move(name));
  }
  return primitive_node_from_field(field, std::move(name));
}

// Analyzes schema order, levels, leaf ranges, and footer element counts in one
// pass.
struct SchemaAnalysisFrame {
  ParquetNode *node = nullptr;
  std::vector<std::string> path{};
  std::int16_t definition_level = 0;
  std::int16_t repetition_level = 0;
  std::size_t first_leaf_index = 0;
  bool exiting = false;
};

/// Validates the Arrow schema and builds the corresponding Parquet node and
/// leaf-column model.
std::vector<LeafColumn> analyze_schema(ParquetNode *root) {
  std::vector<LeafColumn> columns;
  std::vector<SchemaAnalysisFrame> pending;
  pending.reserve(root->children.size() + 1U);
  pending.push_back(SchemaAnalysisFrame{.node = root});

  while (!pending.empty()) {
    auto frame = std::move(pending.back());
    pending.pop_back();
    auto &node = *frame.node;

    if (frame.exiting) {
      node.first_leaf_index = frame.first_leaf_index;
      node.leaf_count = columns.size() - frame.first_leaf_index;
      std::size_t schema_elements = 1;
      if (node.node_kind == NodeKind::kList) {
        schema_elements += 1 + node.element->subtree_schema_element_count;
      } else if (node.node_kind == NodeKind::kMap) {
        schema_elements += node.element->subtree_schema_element_count;
      } else {
        for (const auto &child : node.children) {
          schema_elements += child.subtree_schema_element_count;
        }
      }
      node.subtree_schema_element_count = schema_elements;
      continue;
    }

    if (node.node_kind == NodeKind::kPrimitive) {
      node.leaf_index = columns.size();
      node.first_leaf_index = node.leaf_index;
      node.leaf_count = 1;
      node.subtree_schema_element_count = 1;
      columns.push_back(LeafColumn{
          .node = &node,
          .path = std::move(frame.path),
          .max_definition_level = static_cast<std::int16_t>(
              frame.definition_level + (node.required ? 0 : 1)),
          .max_repetition_level = frame.repetition_level,
      });
      continue;
    }

    pending.push_back(SchemaAnalysisFrame{
        .node = &node,
        .first_leaf_index = columns.size(),
        .exiting = true,
    });

    if (node.node_kind == NodeKind::kRoot ||
        node.node_kind == NodeKind::kStruct) {
      const auto child_definition = static_cast<std::int16_t>(
          frame.definition_level +
          (node.node_kind == NodeKind::kStruct && !node.required ? 1 : 0));
      for (auto &child : node.children | std::views::reverse) {
        auto child_path = frame.path;
        child_path.push_back(child.name);
        pending.push_back(SchemaAnalysisFrame{
            .node = &child,
            .path = std::move(child_path),
            .definition_level = child_definition,
            .repetition_level = frame.repetition_level,
        });
      }
      continue;
    }

    node.repeated_repetition_level =
        static_cast<std::int16_t>(frame.repetition_level + 1);
    auto element_path = std::move(frame.path);
    const auto element_definition = static_cast<std::int16_t>(
        frame.definition_level + (node.required ? 0 : 1) + 1);
    if (node.node_kind == NodeKind::kList) {
      element_path.push_back("list");
      element_path.push_back(node.element->name);
    } else {
      element_path.push_back("key_value");
    }
    pending.push_back(SchemaAnalysisFrame{
        .node = node.element.get(),
        .path = std::move(element_path),
        .definition_level = element_definition,
        .repetition_level = node.repeated_repetition_level,
    });
  }

  return columns;
}

/// Parses and validates the root schema supported by this writer.
sanitize::Result<ParquetNode>
parse_supported_root_schema(const ArrowSchema &schema) {
  SAN_ASSIGN_OR_RAISE(auto root_field, jsonl::parse_schema_field(schema));
  if (root_field.kind != jsonl::JsonlKind::kStruct) {
    return sanitize::Status::NotImplemented(kUnsupportedPrefix,
                                            " root schema is not a struct");
  }
  SAN_ASSIGN_OR_RAISE(auto root, struct_node_from_field(root_field, "schema"));
  return root;
}

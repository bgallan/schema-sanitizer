sanitize::Status add_native_output_field(
    std::vector<NativeParquetOutputField> *fields,
    sanitize::internal::StringLookupMap<std::size_t> *field_index_by_name,
    const std::vector<std::string> &path,
    const std::vector<std::int16_t> &path_definition_levels,
    std::size_t column_index, std::int16_t max_repetition_level,
    bool top_level_required) {
  if (!fields || !field_index_by_name) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output layout is null");
  }
  if (path.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: column path is empty");
  }
  auto recursive_tree = build_native_recursive_materialization_tree(
      path, max_repetition_level, column_index, &path_definition_levels);
  sanitize::Status materializability_status = sanitize::Status::NotImplemented(
      "native Parquet reader: path does not match the recursive "
      "materialization grammar");
  if (recursive_tree.valid) {
    materializability_status =
        validate_native_recursive_materialization_tree_supported(
            recursive_tree);
  }
  if (!materializability_status.ok()) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: nested path is not materializable yet: " +
        materializability_status.message());
  }
  const auto &top_level_name = path[0];
  const auto existing = field_index_by_name->find(top_level_name);
  if (existing == field_index_by_name->end()) {
    NativeParquetOutputField field;
    field.top_level_required = top_level_required;
    field.name = top_level_name;
    field.column_indices.push_back(column_index);
    field.recursive_trees.push_back(std::move(recursive_tree));
    SAN_RETURN_NOT_OK(merge_native_recursive_materialization_tree(
        field.recursive_trees.back(), &field.recursive_tree));
    const std::size_t field_index = fields->size();
    fields->push_back(std::move(field));
    field_index_by_name->try_emplace(fields->back().name, field_index);
    return {};
  }
  NativeParquetOutputField &field = (*fields)[existing->second];
  if (!field.recursive_tree.valid ||
      field.recursive_tree.root >= field.recursive_tree.nodes.size() ||
      recursive_tree.root >= recursive_tree.nodes.size() ||
      field.recursive_tree.nodes[field.recursive_tree.root].kind !=
          recursive_tree.nodes[recursive_tree.root].kind) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: mixed scalar, struct, and list output path");
  }
  if (field.top_level_required != top_level_required) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: inconsistent struct output nullability");
  }
  SAN_RETURN_NOT_OK(merge_native_recursive_materialization_tree(
      recursive_tree, &field.recursive_tree));
  field.column_indices.push_back(column_index);
  field.recursive_trees.push_back(std::move(recursive_tree));
  return {};
}

sanitize::Status validate_native_recursive_output_layout(
    const std::vector<NativeParquetOutputField> &fields,
    std::size_t expected_leaf_count) {
  enum class LeafState : std::uint8_t { Unseen, Expected, Matched, Complete };
  std::vector<LeafState> leaf_states(expected_leaf_count, LeafState::Unseen);
  std::size_t observed_leaf_count = 0;
  for (const auto &field : fields) {
    if (!field.recursive_tree.valid ||
        field.recursive_tree.root >= field.recursive_tree.nodes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive output field tree is invalid");
    }
    if (field.recursive_trees.size() != field.column_indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive output field leaf tree count "
          "mismatch");
    }
    if (field.recursive_counts.leaf_count != field.column_indices.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive output field leaf count mismatch");
    }
    for (const std::size_t column_index : field.column_indices) {
      if (column_index >= leaf_states.size() ||
          leaf_states[column_index] != LeafState::Unseen) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive output field column index "
            "is duplicated or out of range");
      }
      leaf_states[column_index] = LeafState::Expected;
    }
    SAN_ASSIGN_OR_RAISE(
        auto recursive_leaf_columns,
        native_recursive_materialization_leaf_columns(field.recursive_tree));
    for (const std::size_t column_index : recursive_leaf_columns) {
      if (column_index >= leaf_states.size() ||
          leaf_states[column_index] != LeafState::Expected) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive output field leaf columns "
            "mismatch");
      }
      leaf_states[column_index] = LeafState::Matched;
    }
    for (const std::size_t column_index : field.column_indices) {
      if (leaf_states[column_index] != LeafState::Matched) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive output field leaf columns "
            "mismatch");
      }
      leaf_states[column_index] = LeafState::Complete;
    }
    observed_leaf_count += field.recursive_counts.leaf_count;
  }
  if (observed_leaf_count != expected_leaf_count ||
      std::find(leaf_states.cbegin(), leaf_states.cend(), LeafState::Unseen) !=
          leaf_states.cend()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive output layout leaf count mismatch");
  }
  return {};
}

struct NativeRecursiveMetadataValidationState {
  std::size_t node_index = 0;
  std::size_t expected_repeated_layout_index = 0;
  NativeRecursiveStructValidityDomain expected_struct_validity_domain =
      NativeRecursiveStructValidityDomain::None;
  std::optional<std::size_t> expected_struct_validity_repeated_layout_index;
};

sanitize::Status validate_native_recursive_materialization_tree_metadata(
    const NativeRecursiveMaterializationTree &tree) {
  if (!tree.valid || tree.root >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive metadata validation tree is invalid");
  }

  std::vector<NativeRecursiveMetadataValidationState> pending;
  pending.reserve(tree.nodes.size());
  pending.push_back({tree.root, 0,
                     NativeRecursiveStructValidityDomain::RowGroup,
                     std::nullopt});
  std::vector<bool> visited(tree.nodes.size(), false);

  while (!pending.empty()) {
    const auto state = pending.back();
    pending.pop_back();
    if (state.node_index >= tree.nodes.size() || visited[state.node_index]) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive metadata validation contains an "
          "invalid or repeated node");
    }
    visited[state.node_index] = true;
    const auto &node = tree.nodes[state.node_index];
    switch (node.kind) {
    case NativeRecursiveMaterializationNodeKind::Leaf:
      if (node.repeated_layout_index.has_value() ||
          node.struct_validity_domain !=
              NativeRecursiveStructValidityDomain::None ||
          node.struct_validity_repeated_layout_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive leaf has container metadata");
      }
      break;
    case NativeRecursiveMaterializationNodeKind::Struct:
      if (node.repeated_layout_index.has_value() ||
          node.struct_validity_domain ==
              NativeRecursiveStructValidityDomain::None) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive struct metadata is invalid");
      }
      if (node.children.empty()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive struct has no children");
      }
      if (node.struct_validity_domain !=
              state.expected_struct_validity_domain ||
          node.struct_validity_repeated_layout_index !=
              state.expected_struct_validity_repeated_layout_index) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive struct metadata does not match "
            "parent");
      }
      if (node.struct_validity_domain ==
              NativeRecursiveStructValidityDomain::RowGroup &&
          node.struct_validity_repeated_layout_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: row-group struct has repeated metadata");
      }
      if ((node.struct_validity_domain ==
               NativeRecursiveStructValidityDomain::ListElement ||
           node.struct_validity_domain ==
               NativeRecursiveStructValidityDomain::MapEntry) &&
          !node.struct_validity_repeated_layout_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: repeated struct is missing parent layout");
      }
      break;
    case NativeRecursiveMaterializationNodeKind::List:
      if (node.children.size() != 1) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive list child count is invalid");
      }
      if (!node.repeated_layout_index.has_value() ||
          node.repeated_layout_index.value() !=
              state.expected_repeated_layout_index ||
          node.repeated_layout_index.value() >=
              static_cast<std::size_t>(tree.repetition_depth) ||
          node.struct_validity_domain !=
              NativeRecursiveStructValidityDomain::None ||
          node.struct_validity_repeated_layout_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive repeated node metadata is "
            "invalid");
      }
      break;
    case NativeRecursiveMaterializationNodeKind::Map:
      if (node.children.empty()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive map has no children");
      }
      if (!node.repeated_layout_index.has_value() ||
          node.repeated_layout_index.value() !=
              state.expected_repeated_layout_index ||
          node.repeated_layout_index.value() >=
              static_cast<std::size_t>(tree.repetition_depth) ||
          node.struct_validity_domain !=
              NativeRecursiveStructValidityDomain::None ||
          node.struct_validity_repeated_layout_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive repeated node metadata is "
            "invalid");
      }
      break;
    }

    auto child_state = state;
    if (node.kind == NativeRecursiveMaterializationNodeKind::List ||
        node.kind == NativeRecursiveMaterializationNodeKind::Map) {
      child_state.expected_struct_validity_domain =
          node.kind == NativeRecursiveMaterializationNodeKind::List
              ? NativeRecursiveStructValidityDomain::ListElement
              : NativeRecursiveStructValidityDomain::MapEntry;
      child_state.expected_struct_validity_repeated_layout_index =
          node.repeated_layout_index;
      child_state.expected_repeated_layout_index =
          node.repeated_layout_index.value() + 1;
    }
    for (const auto child_index : node.children | std::views::reverse) {
      child_state.node_index = child_index;
      pending.push_back(child_state);
    }
  }
  return {};
}

sanitize::Result<std::size_t> select_native_recursive_layout_column_index(
    const std::vector<ColumnChunkInfo> &columns,
    const NativeRecursiveMaterializationTree &tree, std::size_t node_index) {
  if (!tree.valid || !tree.traversal_plan_valid ||
      node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive layout column plan node is invalid");
  }
  const auto &node = tree.nodes[node_index];
  if (node.kind == NativeRecursiveMaterializationNodeKind::Leaf) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive leaf has no container layout column");
  }
  SAN_ASSIGN_OR_RAISE(const auto leaf_columns,
                      native_recursive_materialization_leaf_columns_from_node(
                          tree, node_index));

  std::size_t target_repeated_level_count = 0;
  std::size_t minimum_repeated_level_count = 0;
  if (node.kind == NativeRecursiveMaterializationNodeKind::List ||
      node.kind == NativeRecursiveMaterializationNodeKind::Map) {
    if (!node.repeated_layout_index.has_value()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive repeated layout index is missing");
    }
    target_repeated_level_count = node.repeated_layout_index.value() + 1;
    minimum_repeated_level_count = target_repeated_level_count;
  } else {
    switch (node.struct_validity_domain) {
    case NativeRecursiveStructValidityDomain::RowGroup:
      break;
    case NativeRecursiveStructValidityDomain::ListElement:
    case NativeRecursiveStructValidityDomain::MapEntry:
      if (!node.struct_validity_repeated_layout_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive struct layout index is missing");
      }
      target_repeated_level_count =
          node.struct_validity_repeated_layout_index.value() + 1;
      break;
    case NativeRecursiveStructValidityDomain::None:
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive struct layout context is invalid");
    }
  }

  std::optional<std::size_t> fallback;
  std::optional<std::size_t> shallowest_repeated_fallback;
  std::size_t shallowest_repeated_level_count =
      std::numeric_limits<std::size_t>::max();
  for (const std::size_t column_index : leaf_columns) {
    if (column_index >= columns.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive layout column is invalid");
    }
    const auto &column = columns[column_index];
    if (column.max_repetition_level < 0) {
      return sanitize::Status::Invalid("native Parquet reader: recursive "
                                       "layout repetition level is invalid");
    }
    const auto repeated_level_count =
        static_cast<std::size_t>(column.max_repetition_level);
    if (repeated_level_count < minimum_repeated_level_count) {
      continue;
    }
    if (!fallback.has_value()) {
      fallback = column_index;
    }
    if (repeated_level_count == target_repeated_level_count) {
      return column_index;
    }
    if (repeated_level_count > target_repeated_level_count &&
        repeated_level_count < shallowest_repeated_level_count) {
      shallowest_repeated_fallback = column_index;
      shallowest_repeated_level_count = repeated_level_count;
    }
  }
  if (!fallback.has_value()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive container layout has no leaves");
  }
  return shallowest_repeated_fallback.value_or(fallback.value());
}

sanitize::Status plan_native_recursive_layout_columns(
    const std::vector<ColumnChunkInfo> &columns,
    NativeRecursiveMaterializationTree *tree) {
  if (!tree || !tree->valid || !tree->traversal_plan_valid) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive layout column plan is invalid");
  }
  for (std::size_t node_index = 0; node_index < tree->nodes.size();
       ++node_index) {
    auto &node = tree->nodes[node_index];
    if (node.kind == NativeRecursiveMaterializationNodeKind::Leaf) {
      continue;
    }
    if (node.kind == NativeRecursiveMaterializationNodeKind::Struct &&
        !node.definition_level.has_value()) {
      return sanitize::Status::Invalid("native Parquet reader: recursive "
                                       "struct definition level is missing");
    }
    SAN_ASSIGN_OR_RAISE(node.layout_column_index,
                        select_native_recursive_layout_column_index(
                            columns, *tree, node_index));
  }
  return {};
}

sanitize::Status
finalize_native_output_layout(std::vector<NativeParquetOutputField> *fields,
                              std::size_t expected_leaf_count) {
  if (!fields) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output layout is null");
  }
  for (auto &field : *fields) {
    SAN_ASSIGN_OR_RAISE(
        field.recursive_subtree_counts,
        plan_native_recursive_materialization_tree(&field.recursive_tree));
    if (field.recursive_tree.root >= field.recursive_subtree_counts.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive output field root is invalid");
    }
    field.recursive_counts =
        field.recursive_subtree_counts[field.recursive_tree.root];
    SAN_RETURN_NOT_OK(validate_native_recursive_materialization_tree_metadata(
        field.recursive_tree));
  }
  SAN_RETURN_NOT_OK(
      validate_native_recursive_output_layout(*fields, expected_leaf_count));
  return {};
}

sanitize::Status
build_native_output_layout(const std::vector<ColumnChunkInfo> &columns,
                           std::vector<NativeParquetOutputField> *fields) {
  if (!fields) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output layout is null");
  }
  fields->clear();
  fields->reserve(columns.size());
  sanitize::internal::StringLookupMap<std::size_t> field_index_by_name;
  field_index_by_name.reserve(columns.size());
  for (std::size_t i = 0; i < columns.size(); ++i) {
    const auto &column = columns[i];
    SAN_RETURN_NOT_OK(add_native_output_field(
        fields, &field_index_by_name, column.path_in_schema,
        column.path_definition_levels, i, column.max_repetition_level,
        column.top_level_required));
  }
  SAN_RETURN_NOT_OK(finalize_native_output_layout(fields, columns.size()));
  for (auto &field : *fields) {
    SAN_RETURN_NOT_OK(
        plan_native_recursive_layout_columns(columns, &field.recursive_tree));
  }
  return {};
}

sanitize::Status
build_native_output_layout(const std::vector<LeafLevelInfo> &leaves,
                           std::vector<NativeParquetOutputField> *fields) {
  if (!fields) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output layout is null");
  }
  fields->clear();
  fields->reserve(leaves.size());
  sanitize::internal::StringLookupMap<std::size_t> field_index_by_name;
  field_index_by_name.reserve(leaves.size());
  for (std::size_t i = 0; i < leaves.size(); ++i) {
    const auto &leaf = leaves[i];
    SAN_RETURN_NOT_OK(add_native_output_field(
        fields, &field_index_by_name, leaf.path, leaf.path_definition_levels, i,
        leaf.max_repetition_level, leaf.top_level_required));
  }
  return finalize_native_output_layout(fields, leaves.size());
}

const char *native_recursive_materialization_node_kind_label(
    NativeRecursiveMaterializationNodeKind kind) {
  switch (kind) {
  case NativeRecursiveMaterializationNodeKind::Leaf:
    return "leaf";
  case NativeRecursiveMaterializationNodeKind::Struct:
    return "struct";
  case NativeRecursiveMaterializationNodeKind::List:
    return "list";
  case NativeRecursiveMaterializationNodeKind::Map:
    return "map";
  }
  return "unknown";
}

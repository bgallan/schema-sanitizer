// Iterative recursive-path construction, annotation, cloning, and merging.

struct NativeRecursiveTraversalState {
  std::size_t node_index = 0;
  std::size_t repeated_layout_index = 0;
  NativeRecursiveStructValidityDomain struct_validity_domain =
      NativeRecursiveStructValidityDomain::None;
  std::optional<std::size_t> struct_validity_repeated_layout_index;
};

bool assign_native_recursive_repeated_layout_indexes(
    NativeRecursiveMaterializationTree *tree) {
  if (!tree || !tree->valid || tree->root >= tree->nodes.size()) {
    return false;
  }

  std::vector<NativeRecursiveTraversalState> pending;
  pending.reserve(tree->nodes.size());
  pending.push_back({tree->root, 0,
                     NativeRecursiveStructValidityDomain::RowGroup,
                     std::nullopt});
  std::vector<bool> visited(tree->nodes.size(), false);

  while (!pending.empty()) {
    const auto state = pending.back();
    pending.pop_back();
    if (state.node_index >= tree->nodes.size() || visited[state.node_index] ||
        state.repeated_layout_index >
            static_cast<std::size_t>(
                std::numeric_limits<std::int16_t>::max())) {
      return false;
    }
    visited[state.node_index] = true;
    auto &node = tree->nodes[state.node_index];

    NativeRecursiveTraversalState child_state = state;
    switch (node.kind) {
    case NativeRecursiveMaterializationNodeKind::Leaf:
      node.repeated_layout_index.reset();
      node.struct_validity_domain = NativeRecursiveStructValidityDomain::None;
      node.struct_validity_repeated_layout_index.reset();
      continue;
    case NativeRecursiveMaterializationNodeKind::Struct:
      node.repeated_layout_index.reset();
      node.struct_validity_domain = state.struct_validity_domain;
      node.struct_validity_repeated_layout_index =
          state.struct_validity_repeated_layout_index;
      break;
    case NativeRecursiveMaterializationNodeKind::List:
    case NativeRecursiveMaterializationNodeKind::Map:
      if (state.repeated_layout_index >=
          static_cast<std::size_t>(tree->repetition_depth)) {
        return false;
      }
      node.repeated_layout_index = state.repeated_layout_index;
      node.struct_validity_domain = NativeRecursiveStructValidityDomain::None;
      node.struct_validity_repeated_layout_index.reset();
      child_state.repeated_layout_index =
          state.repeated_layout_index + std::size_t{1};
      child_state.struct_validity_domain =
          node.kind == NativeRecursiveMaterializationNodeKind::List
              ? NativeRecursiveStructValidityDomain::ListElement
              : NativeRecursiveStructValidityDomain::MapEntry;
      child_state.struct_validity_repeated_layout_index =
          state.repeated_layout_index;
      break;
    }

    for (const std::size_t child_index : node.children | std::views::reverse) {
      child_state.node_index = child_index;
      pending.push_back(child_state);
    }
  }
  return true;
}

NativeRecursiveMaterializationTree build_native_recursive_materialization_tree(
    const std::vector<std::string> &path, std::int16_t max_repetition_level,
    std::optional<std::size_t> leaf_column_index = std::nullopt,
    const std::vector<std::int16_t> *path_definition_levels = nullptr) {
  NativeRecursiveMaterializationTree tree;
  if (path.empty() || max_repetition_level < 0 ||
      (path_definition_levels &&
       path_definition_levels->size() != path.size())) {
    return tree;
  }

  struct ParsedNode {
    NativeRecursiveMaterializationNodeKind kind =
        NativeRecursiveMaterializationNodeKind::Leaf;
    std::size_t path_index = 0;
  };

  std::vector<ParsedNode> parsed_nodes;
  parsed_nodes.reserve(path.size());
  std::size_t path_index = 0;
  std::int16_t repetition_depth = 0;
  while (path_index < path.size()) {
    if (path_index + 1 == path.size()) {
      parsed_nodes.push_back(
          {NativeRecursiveMaterializationNodeKind::Leaf, path_index});
      break;
    }

    const std::string_view marker = path[path_index + 1];
    if (marker == "list" || marker == "key_value") {
      if (path_index + 2 >= path.size() ||
          (marker == "key_value" && path[path_index + 2] != "key" &&
           path[path_index + 2] != "value") ||
          repetition_depth == std::numeric_limits<std::int16_t>::max()) {
        return {};
      }
      parsed_nodes.push_back({marker == "list"
                                  ? NativeRecursiveMaterializationNodeKind::List
                                  : NativeRecursiveMaterializationNodeKind::Map,
                              path_index});
      ++repetition_depth;
      path_index += 2;
      continue;
    }

    parsed_nodes.push_back(
        {NativeRecursiveMaterializationNodeKind::Struct, path_index});
    ++path_index;
  }

  if (parsed_nodes.empty() || repetition_depth != max_repetition_level) {
    return {};
  }

  tree.nodes.reserve(parsed_nodes.size());
  std::optional<std::size_t> child_index;
  for (const ParsedNode &parsed : parsed_nodes | std::views::reverse) {
    NativeRecursiveMaterializationNode node;
    node.kind = parsed.kind;
    node.name = path[parsed.path_index];
    if (path_definition_levels) {
      node.definition_level = (*path_definition_levels)[parsed.path_index];
      const std::int16_t parent_definition_level =
          parsed.path_index == 0
              ? std::int16_t{0}
              : (*path_definition_levels)[parsed.path_index - 1];
      node.nullable = (*path_definition_levels)[parsed.path_index] >
                      parent_definition_level;
    }
    if (node.kind == NativeRecursiveMaterializationNodeKind::Leaf) {
      node.leaf_column_index = leaf_column_index;
    } else if (!child_index.has_value()) {
      return {};
    }
    if (child_index.has_value()) {
      node.children.push_back(child_index.value());
    }
    child_index = tree.nodes.size();
    tree.nodes.push_back(std::move(node));
  }

  if (!child_index.has_value()) {
    return {};
  }
  tree.valid = true;
  tree.root = child_index.value();
  tree.repetition_depth = repetition_depth;
  if (!assign_native_recursive_repeated_layout_indexes(&tree)) {
    return {};
  }
  return tree;
}

NativeRecursiveMaterializationNode
clone_native_recursive_node(const NativeRecursiveMaterializationNode &source) {
  NativeRecursiveMaterializationNode clone = source;
  clone.parent_index.reset();
  clone.layout_column_index.reset();
  clone.subtree_leaf_offset = 0;
  clone.subtree_leaf_count = 0;
  clone.children.clear();
  clone.children.reserve(source.children.size());
  return clone;
}

sanitize::Result<std::size_t> clone_native_recursive_materialization_subtree(
    const NativeRecursiveMaterializationTree &source, std::size_t source_index,
    NativeRecursiveMaterializationTree *destination) {
  if (!destination || source_index >= source.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive materialization subtree is invalid");
  }

  constexpr auto missing = std::numeric_limits<std::size_t>::max();
  std::vector<std::size_t> cloned_indexes(source.nodes.size(), missing);
  std::vector<std::pair<std::size_t, bool>> pending;
  pending.reserve(source.nodes.size());
  pending.emplace_back(source_index, false);
  std::vector<bool> entered(source.nodes.size(), false);

  while (!pending.empty()) {
    const auto [node_index, expanded] = pending.back();
    pending.pop_back();
    if (node_index >= source.nodes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive materialization source node is "
          "invalid");
    }
    const auto &source_node = source.nodes[node_index];
    if (!expanded) {
      if (entered[node_index]) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive materialization tree contains "
            "a repeated node");
      }
      entered[node_index] = true;
      pending.emplace_back(node_index, true);
      for (const std::size_t child_index :
           source_node.children | std::views::reverse) {
        pending.emplace_back(child_index, false);
      }
      continue;
    }

    auto clone = clone_native_recursive_node(source_node);
    for (const std::size_t child_index : source_node.children) {
      if (child_index >= cloned_indexes.size() ||
          cloned_indexes[child_index] == missing) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive materialization source child "
            "is invalid");
      }
      clone.children.push_back(cloned_indexes[child_index]);
    }
    cloned_indexes[node_index] = destination->nodes.size();
    destination->nodes.push_back(std::move(clone));
  }
  return cloned_indexes[source_index];
}

sanitize::Status merge_native_recursive_materialization_node_properties(
    const NativeRecursiveMaterializationNode &source,
    NativeRecursiveMaterializationNode *destination) {
  if (!destination || source.kind != destination->kind ||
      source.name != destination->name ||
      source.nullable != destination->nullable) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: recursive materialization tree branches are "
        "incompatible");
  }
  if (source.definition_level.has_value() &&
      destination->definition_level.has_value() &&
      source.definition_level != destination->definition_level) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: recursive materialization definition level "
        "mismatch");
  }
  if (!destination->definition_level.has_value()) {
    destination->definition_level = source.definition_level;
  }
  destination->layout_column_index.reset();
  if (source.repeated_layout_index.has_value() &&
      destination->repeated_layout_index.has_value() &&
      source.repeated_layout_index != destination->repeated_layout_index) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: recursive materialization repeated layout "
        "index mismatch");
  }
  if (!destination->repeated_layout_index.has_value()) {
    destination->repeated_layout_index = source.repeated_layout_index;
  }
  if (source.struct_validity_domain !=
          NativeRecursiveStructValidityDomain::None &&
      destination->struct_validity_domain !=
          NativeRecursiveStructValidityDomain::None &&
      source.struct_validity_domain != destination->struct_validity_domain) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: recursive materialization struct validity "
        "domain mismatch");
  }
  if (source.struct_validity_repeated_layout_index.has_value() &&
      destination->struct_validity_repeated_layout_index.has_value() &&
      source.struct_validity_repeated_layout_index !=
          destination->struct_validity_repeated_layout_index) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: recursive materialization struct validity "
        "layout index mismatch");
  }
  if (destination->struct_validity_domain ==
      NativeRecursiveStructValidityDomain::None) {
    destination->struct_validity_domain = source.struct_validity_domain;
  }
  if (!destination->struct_validity_repeated_layout_index.has_value()) {
    destination->struct_validity_repeated_layout_index =
        source.struct_validity_repeated_layout_index;
  }
  if (source.kind == NativeRecursiveMaterializationNodeKind::Leaf) {
    if (destination->leaf_column_index.has_value() &&
        source.leaf_column_index.has_value() &&
        destination->leaf_column_index != source.leaf_column_index) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: duplicate recursive leaf path has different "
          "columns");
    }
    if (!destination->leaf_column_index.has_value()) {
      destination->leaf_column_index = source.leaf_column_index;
    }
  }
  return {};
}

sanitize::Status merge_native_recursive_materialization_node(
    const NativeRecursiveMaterializationTree &source, std::size_t source_index,
    NativeRecursiveMaterializationTree *destination,
    std::size_t destination_index) {
  if (!destination || source_index >= source.nodes.size() ||
      destination_index >= destination->nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive materialization merge node is "
        "invalid");
  }

  std::vector<std::pair<std::size_t, std::size_t>> pending;
  pending.reserve(source.nodes.size());
  pending.emplace_back(source_index, destination_index);
  std::vector<bool> visited(source.nodes.size(), false);

  while (!pending.empty()) {
    const auto [current_source_index, current_destination_index] =
        pending.back();
    pending.pop_back();
    if (current_source_index >= source.nodes.size() ||
        current_destination_index >= destination->nodes.size() ||
        visited[current_source_index]) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive materialization merge traversal "
          "is invalid");
    }
    visited[current_source_index] = true;
    const auto &source_node = source.nodes[current_source_index];
    SAN_RETURN_NOT_OK(merge_native_recursive_materialization_node_properties(
        source_node, &destination->nodes[current_destination_index]));

    for (const std::size_t source_child_index :
         source_node.children | std::views::reverse) {
      if (source_child_index >= source.nodes.size()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive materialization source child is "
            "invalid");
      }
      const auto &source_child = source.nodes[source_child_index];
      std::optional<std::size_t> destination_child_index;
      for (const std::size_t candidate_index :
           destination->nodes[current_destination_index].children) {
        if (candidate_index >= destination->nodes.size()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: recursive materialization destination "
              "child is invalid");
        }
        const auto &candidate = destination->nodes[candidate_index];
        if (candidate.kind == source_child.kind &&
            candidate.name == source_child.name) {
          destination_child_index = candidate_index;
          break;
        }
      }
      if (destination_child_index.has_value()) {
        pending.emplace_back(source_child_index,
                             destination_child_index.value());
        continue;
      }
      SAN_ASSIGN_OR_RAISE(const auto cloned_child_index,
                          clone_native_recursive_materialization_subtree(
                              source, source_child_index, destination));
      destination->nodes[current_destination_index].children.push_back(
          cloned_child_index);
    }
  }
  return {};
}

sanitize::Status merge_native_recursive_materialization_tree(
    const NativeRecursiveMaterializationTree &source,
    NativeRecursiveMaterializationTree *destination) {
  if (!source.valid || source.root >= source.nodes.size() || !destination) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive materialization tree is invalid");
  }
  destination->traversal_plan_valid = false;
  destination->leaf_column_indices.clear();
  destination->repeated_node_indices.clear();
  destination->nodes.reserve(destination->nodes.size() + source.nodes.size());
  if (!destination->valid) {
    destination->nodes.clear();
    SAN_ASSIGN_OR_RAISE(const auto cloned_root,
                        clone_native_recursive_materialization_subtree(
                            source, source.root, destination));
    destination->valid = true;
    destination->root = cloned_root;
    destination->repetition_depth = source.repetition_depth;
    return {};
  }
  if (destination->root >= destination->nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive materialization destination root is "
        "invalid");
  }
  SAN_RETURN_NOT_OK(merge_native_recursive_materialization_node(
      source, source.root, destination, destination->root));
  destination->repetition_depth =
      std::max(destination->repetition_depth, source.repetition_depth);
  return {};
}

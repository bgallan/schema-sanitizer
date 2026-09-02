// Defines the recursive materialization model and non-recursive tree traversal
// helpers. This fragment is included by footer_reader.cc inside its
// implementation namespace.

enum class NativeRecursiveMaterializationNodeKind {
  Leaf,
  Struct,
  List,
  Map,
};

enum class NativeRecursiveStructValidityDomain {
  None,
  RowGroup,
  ListElement,
  MapEntry,
};

struct NativeRecursiveMaterializationNode {
  NativeRecursiveMaterializationNodeKind kind =
      NativeRecursiveMaterializationNodeKind::Leaf;
  std::string name;
  bool nullable = false;
  std::optional<std::size_t> leaf_column_index;
  std::optional<std::size_t> layout_column_index;
  std::optional<std::int16_t> definition_level;
  std::optional<std::size_t> repeated_layout_index;
  NativeRecursiveStructValidityDomain struct_validity_domain =
      NativeRecursiveStructValidityDomain::None;
  std::optional<std::size_t> struct_validity_repeated_layout_index;
  std::optional<std::size_t> parent_index;
  std::size_t subtree_leaf_offset = 0;
  std::size_t subtree_leaf_count = 0;
  std::vector<std::size_t> children;
};

struct NativeRecursiveMaterializationTree {
  bool valid = false;
  bool traversal_plan_valid = false;
  std::size_t root = 0;
  std::int16_t repetition_depth = 0;
  std::vector<NativeRecursiveMaterializationNode> nodes;
  std::vector<std::size_t> leaf_column_indices;
  std::vector<std::size_t> repeated_node_indices;
};

struct NativeRecursiveMaterializationResourceCounts {
  std::size_t struct_count = 0;
  std::size_t list_count = 0;
  std::size_t leaf_count = 0;
};

struct NativeParquetOutputField {
  bool top_level_required = true;
  std::string name;
  std::vector<std::size_t> column_indices;
  std::vector<NativeRecursiveMaterializationTree> recursive_trees;
  NativeRecursiveMaterializationTree recursive_tree;
  std::vector<NativeRecursiveMaterializationResourceCounts>
      recursive_subtree_counts;
  NativeRecursiveMaterializationResourceCounts recursive_counts;
};

struct NativeRecursivePathPlan {
  bool materializable = false;
  bool recursive_tree_valid = false;
  std::int16_t recursive_repetition_depth = 0;
  sanitize::Status materializability_status = sanitize::Status::NotImplemented(
      "native Parquet reader: recursive path was not planned");
};

/// Returns the contiguous leaf-column span covered by one planned recursive
/// node.
sanitize::Result<std::span<const std::size_t>>
native_recursive_materialization_leaf_columns_from_node(
    const NativeRecursiveMaterializationTree &tree, std::size_t node_index) {
  if (!tree.valid || !tree.traversal_plan_valid ||
      node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive materialization leaf plan is "
        "invalid");
  }
  const auto &node = tree.nodes[node_index];
  if (node.subtree_leaf_offset > tree.leaf_column_indices.size() ||
      node.subtree_leaf_count >
          tree.leaf_column_indices.size() - node.subtree_leaf_offset) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive materialization leaf range is "
        "invalid");
  }
  return std::span<const std::size_t>{tree.leaf_column_indices}.subspan(
      node.subtree_leaf_offset, node.subtree_leaf_count);
}

/// Returns the leaf-column span covered by the recursive tree root.
sanitize::Result<std::span<const std::size_t>>
native_recursive_materialization_leaf_columns(
    const NativeRecursiveMaterializationTree &tree) {
  return native_recursive_materialization_leaf_columns_from_node(tree,
                                                                 tree.root);
}

/// Plans native recursive materialization tree iteratively while enforcing
/// supported nesting and bounded resources.
sanitize::Result<std::vector<NativeRecursiveMaterializationResourceCounts>>
plan_native_recursive_materialization_tree(
    NativeRecursiveMaterializationTree *tree) {
  if (!tree || !tree->valid || tree->root >= tree->nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive materialization plan tree is "
        "invalid");
  }

  tree->traversal_plan_valid = false;
  tree->leaf_column_indices.clear();
  tree->leaf_column_indices.reserve(tree->nodes.size());
  tree->repeated_node_indices.clear();
  tree->repeated_node_indices.reserve(
      static_cast<std::size_t>(tree->repetition_depth));
  for (auto &node : tree->nodes) {
    node.parent_index.reset();
    node.layout_column_index.reset();
    node.subtree_leaf_offset = 0;
    node.subtree_leaf_count = 0;
  }

  std::vector<NativeRecursiveMaterializationResourceCounts> counts(
      tree->nodes.size());
  std::vector<std::pair<std::size_t, bool>> pending;
  pending.reserve(tree->nodes.size());
  pending.emplace_back(tree->root, false);
  std::vector<bool> entered(tree->nodes.size(), false);

  while (!pending.empty()) {
    const auto [node_index, expanded] = pending.back();
    pending.pop_back();
    if (node_index >= tree->nodes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive materialization plan node is "
          "invalid");
    }
    auto &node = tree->nodes[node_index];
    if (!expanded) {
      if (entered[node_index]) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive materialization tree contains a "
            "repeated node");
      }
      entered[node_index] = true;
      pending.emplace_back(node_index, true);
      for (const std::size_t child_index :
           node.children | std::views::reverse) {
        if (child_index >= tree->nodes.size() || child_index == tree->root ||
            tree->nodes[child_index].parent_index.has_value()) {
          return sanitize::Status::Invalid(
              "native Parquet reader: recursive materialization plan child is "
              "invalid or repeated");
        }
        tree->nodes[child_index].parent_index = node_index;
        pending.emplace_back(child_index, false);
      }
      continue;
    }

    auto &node_counts = counts[node_index];
    node.subtree_leaf_offset = tree->leaf_column_indices.size();
    switch (node.kind) {
    case NativeRecursiveMaterializationNodeKind::Leaf:
      if (!node.leaf_column_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive materialization leaf has no "
            "column index");
      }
      tree->leaf_column_indices.push_back(node.leaf_column_index.value());
      node.subtree_leaf_count = 1;
      ++node_counts.leaf_count;
      break;
    case NativeRecursiveMaterializationNodeKind::Struct:
      ++node_counts.struct_count;
      break;
    case NativeRecursiveMaterializationNodeKind::List:
      ++node_counts.list_count;
      tree->repeated_node_indices.push_back(node_index);
      break;
    case NativeRecursiveMaterializationNodeKind::Map:
      ++node_counts.list_count;
      ++node_counts.struct_count;
      tree->repeated_node_indices.push_back(node_index);
      break;
    }

    bool first_child = true;
    for (const std::size_t child_index : node.children) {
      if (child_index >= counts.size()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive materialization plan child is "
            "invalid");
      }
      const auto &child = tree->nodes[child_index];
      if (first_child) {
        node.subtree_leaf_offset = child.subtree_leaf_offset;
        first_child = false;
      } else if (child.subtree_leaf_offset !=
                 node.subtree_leaf_offset + node.subtree_leaf_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive materialization leaf ranges are "
            "not contiguous");
      }
      node.subtree_leaf_count += child.subtree_leaf_count;
      const auto &child_counts = counts[child_index];
      node_counts.struct_count += child_counts.struct_count;
      node_counts.list_count += child_counts.list_count;
      node_counts.leaf_count += child_counts.leaf_count;
    }
  }

  if (tree->nodes[tree->root].subtree_leaf_count !=
      tree->leaf_column_indices.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive materialization leaf plan count "
        "mismatch");
  }
  tree->traversal_plan_valid = true;
  return counts;
}

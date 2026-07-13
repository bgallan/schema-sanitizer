// Recursive path support validation and repeated-level helpers.
// Included directly by footer_reader.cc.

struct NativeRecursiveSupportValidationFrame {
  std::size_t node_index = 0;
  std::size_t expected_repeated_layout_index = 0;
  NativeRecursiveStructValidityDomain expected_struct_validity_domain =
      NativeRecursiveStructValidityDomain::None;
  std::optional<std::size_t> expected_struct_validity_repeated_layout_index;
};

sanitize::Status validate_native_recursive_materialization_tree_supported(
    const NativeRecursiveMaterializationTree &tree) {
  if (!tree.valid || tree.root >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive support validation tree is invalid");
  }

  std::vector<NativeRecursiveSupportValidationFrame> pending;
  pending.reserve(tree.nodes.size());
  pending.push_back({tree.root, 0,
                     NativeRecursiveStructValidityDomain::RowGroup,
                     std::nullopt});
  std::vector<bool> visited(tree.nodes.size(), false);

  while (!pending.empty()) {
    const auto frame = std::move(pending.back());
    pending.pop_back();
    if (frame.node_index >= tree.nodes.size() || visited[frame.node_index]) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive support validation contains an "
          "invalid or repeated node");
    }
    visited[frame.node_index] = true;
    const auto &node = tree.nodes[frame.node_index];

    switch (node.kind) {
    case NativeRecursiveMaterializationNodeKind::Leaf:
      if (node.repeated_layout_index.has_value() ||
          node.struct_validity_domain !=
              NativeRecursiveStructValidityDomain::None ||
          node.struct_validity_repeated_layout_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive support leaf metadata is "
            "invalid");
      }
      break;
    case NativeRecursiveMaterializationNodeKind::Struct:
      if (node.children.empty()) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: recursive struct has no materializable "
            "children");
      }
      if (node.repeated_layout_index.has_value() ||
          node.struct_validity_domain !=
              frame.expected_struct_validity_domain ||
          node.struct_validity_repeated_layout_index !=
              frame.expected_struct_validity_repeated_layout_index) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive struct support metadata is "
            "invalid");
      }
      for (const auto child_index : node.children | std::views::reverse) {
        pending.push_back(
            {child_index, frame.expected_repeated_layout_index,
             frame.expected_struct_validity_domain,
             frame.expected_struct_validity_repeated_layout_index});
      }
      break;
    case NativeRecursiveMaterializationNodeKind::List:
      if (node.children.size() != 1) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: recursive list does not have exactly one "
            "child");
      }
      if (!node.repeated_layout_index.has_value() ||
          node.repeated_layout_index.value() !=
              frame.expected_repeated_layout_index ||
          node.repeated_layout_index.value() >=
              static_cast<std::size_t>(tree.repetition_depth) ||
          node.struct_validity_domain !=
              NativeRecursiveStructValidityDomain::None ||
          node.struct_validity_repeated_layout_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive list support metadata is "
            "invalid");
      }
      pending.push_back({node.children.front(),
                         node.repeated_layout_index.value() + 1,
                         NativeRecursiveStructValidityDomain::ListElement,
                         node.repeated_layout_index.value()});
      break;
    case NativeRecursiveMaterializationNodeKind::Map:
      if (node.children.empty()) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: recursive map has no materializable "
            "children");
      }
      if (!node.repeated_layout_index.has_value() ||
          node.repeated_layout_index.value() !=
              frame.expected_repeated_layout_index ||
          node.repeated_layout_index.value() >=
              static_cast<std::size_t>(tree.repetition_depth) ||
          node.struct_validity_domain !=
              NativeRecursiveStructValidityDomain::None ||
          node.struct_validity_repeated_layout_index.has_value()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive map support metadata is "
            "invalid");
      }
      for (const auto child_index : node.children | std::views::reverse) {
        pending.push_back({child_index, node.repeated_layout_index.value() + 1,
                           NativeRecursiveStructValidityDomain::MapEntry,
                           node.repeated_layout_index.value()});
      }
      break;
    }
  }
  return {};
}

NativeRecursivePathPlan
plan_native_recursive_path(const std::vector<std::string> &path,
                           std::int16_t max_repetition_level,
                           bool top_level_required) {
  NativeRecursivePathPlan plan;
  if (path.empty()) {
    return plan;
  }
  const auto tree =
      build_native_recursive_materialization_tree(path, max_repetition_level);
  if (!tree.valid) {
    plan.materializability_status = sanitize::Status::NotImplemented(
        "native Parquet reader: path does not match the recursive "
        "materialization grammar");
    return plan;
  }
  const auto tree_materializable =
      validate_native_recursive_materialization_tree_supported(tree);
  plan.recursive_tree_valid = true;
  plan.recursive_repetition_depth = tree.repetition_depth;
  (void)top_level_required;
  plan.materializability_status = tree_materializable;
  plan.materializable = tree_materializable.ok();
  return plan;
}

sanitize::Status
native_plain_path_materializability_status(const std::vector<std::string> &path,
                                           std::int16_t max_repetition_level,
                                           bool top_level_required) {
  return plan_native_recursive_path(path, max_repetition_level,
                                    top_level_required)
      .materializability_status;
}

bool native_plain_path_is_materializable(const std::vector<std::string> &path,
                                         std::int16_t max_repetition_level,
                                         bool top_level_required) {
  return native_plain_path_materializability_status(path, max_repetition_level,
                                                    top_level_required)
      .ok();
}

bool is_native_repeated_path_leaf(const ColumnChunkInfo &column) {
  return column.max_repetition_level > 0 &&
         native_plain_path_is_materializable(column.path_in_schema,
                                             column.max_repetition_level,
                                             column.top_level_required);
}

std::vector<std::int16_t>
generic_list_defined_levels_from_path(const ColumnChunkInfo &column) {
  std::vector<std::int16_t> levels;
  if (column.max_repetition_level <= 0 || column.path_in_schema.empty() ||
      column.path_definition_levels.size() != column.path_in_schema.size()) {
    return levels;
  }
  levels.reserve(static_cast<std::size_t>(column.max_repetition_level));
  for (std::size_t i = 1; i + 1 < column.path_in_schema.size(); ++i) {
    if (column.path_in_schema[i] == "key_value") {
      levels.push_back(column.path_definition_levels[i - 1]);
      continue;
    }
    if (column.path_in_schema[i] == "list") {
      levels.push_back(column.path_definition_levels[i - 1]);
      ++i;
    }
  }
  if (levels.size() != static_cast<std::size_t>(column.max_repetition_level)) {
    levels.clear();
  }
  return levels;
}

std::int64_t list_leaf_value_count(const ColumnChunkInfo &column) {
  if (!column.repeated_level_layouts.empty()) {
    const auto &deepest_layout = column.repeated_level_layouts.back();
    if (deepest_layout.decoded) {
      return deepest_layout.element_count;
    }
  }
  return -1;
}

bool list_leaf_value_count_is_materializable(const ColumnChunkInfo &column) {
  const auto element_count = list_leaf_value_count(column);
  return element_count >= 0 &&
         element_count <= static_cast<std::int64_t>(
                              std::numeric_limits<std::int32_t>::max());
}

std::int16_t
list_leaf_value_parent_defined_level(const ColumnChunkInfo &column) {
  const auto generic_levels = generic_list_defined_levels_from_path(column);
  if (!generic_levels.empty()) {
    return generic_levels.back();
  }
  return column.top_level_required ? std::int16_t{0} : std::int16_t{1};
}

sanitize::Status
ensure_repeated_layout_validity_capacity(RepeatedLevelLayoutInfo *layout) {
  if (!layout || layout->row_count <= 0) {
    return {};
  }
  const auto validity_bytes = (layout->row_count + 7) / 8;
  if (static_cast<std::uint64_t>(validity_bytes) > kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "Parquet generic repeated levels: validity bitmap exceeds memory "
        "limit");
  }
  if (layout->validity_bitmap.empty()) {
    layout->validity_bitmap.assign(static_cast<std::size_t>(validity_bytes), 0);
  } else if (layout->row_count >
             static_cast<std::int64_t>(layout->validity_bitmap.size() * 8ULL)) {
    layout->validity_bitmap.push_back(0);
  }
  return {};
}

sanitize::Status increment_i32_counter(std::int32_t *counter,
                                       std::string_view what) {
  if (!counter || *counter == std::numeric_limits<std::int32_t>::max()) {
    return sanitize::Status::Invalid("Parquet generic repeated levels: ", what,
                                     " exceeds int32");
  }
  ++(*counter);
  return {};
}

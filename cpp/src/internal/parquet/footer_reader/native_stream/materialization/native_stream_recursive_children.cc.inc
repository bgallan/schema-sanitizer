sanitize::Result<ArrowArray *> materialize_native_recursive_child_node(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t child_node_index, const NativeFlatRowWindow &window,
    NativeRecursiveArrayCursor *cursor) {
  if (!state || !cursor || !tree.valid ||
      child_node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid recursive child materialization");
  }
  const auto &child_node = tree.nodes[child_node_index];
  if (child_node.kind == NativeRecursiveMaterializationNodeKind::Leaf) {
    if (!child_node.leaf_column_index.has_value() ||
        child_node.leaf_column_index.value() >= state->columns.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive child leaf column is invalid");
    }
    return &state->columns[child_node.leaf_column_index.value()].array;
  }
  if (child_node.kind == NativeRecursiveMaterializationNodeKind::List) {
    return materialize_native_recursive_list_node(
        state, row_group, tree, child_node_index, window, cursor);
  }
  if (child_node.kind == NativeRecursiveMaterializationNodeKind::Map) {
    return materialize_native_recursive_map_node(
        state, row_group, tree, child_node_index, window, cursor);
  }
  if (child_node.kind == NativeRecursiveMaterializationNodeKind::Struct) {
    return materialize_native_recursive_struct_node(
        state, row_group, tree, child_node_index, window, cursor);
  }
  return sanitize::Status::Invalid(
      "native Parquet reader: unknown recursive child node kind");
}

sanitize::Result<ArrowArray *> materialize_native_recursive_struct_node(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t struct_node_index, const NativeFlatRowWindow &window,
    NativeRecursiveArrayCursor *cursor) {
  if (!state || !cursor || !tree.valid ||
      struct_node_index >= tree.nodes.size() ||
      tree.nodes[struct_node_index].kind !=
          NativeRecursiveMaterializationNodeKind::Struct) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid recursive struct node "
        "materialization");
  }
  if (cursor->struct_index >= state->structs.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive struct node allocation mismatch");
  }
  SAN_ASSIGN_OR_RAISE(const auto *layout_column,
                      select_recursive_struct_layout_column_for_node(
                          row_group, tree, struct_node_index));
  auto &struct_array = state->structs[cursor->struct_index++];
  const auto &struct_node = tree.nodes[struct_node_index];
  struct_array.children.reserve(struct_node.children.size());
  SAN_RETURN_NOT_OK(append_native_recursive_node_children(
      state, row_group, tree, struct_node_index, window,
      &cursor->struct_index, &cursor->list_index, &struct_array.children));
  if (!struct_node.definition_level.has_value()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive struct definition level is missing");
  }
  const auto defined_level = struct_node.definition_level.value();
  SAN_ASSIGN_OR_RAISE(const auto validity,
                      materialize_native_recursive_struct_validity_for_node(
                          row_group, *layout_column, tree, struct_node_index,
                          window, defined_level, &struct_array.validity));
  SAN_RETURN_NOT_OK(configure_native_struct_array(
      &struct_array, validity.length, validity.null_count));
  return &struct_array.array;
}

sanitize::Status append_native_recursive_node_children(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t parent_node_index, const NativeFlatRowWindow &window,
    std::size_t *struct_index, std::size_t *list_index,
    std::vector<ArrowArray *> *children) {
  if (!state || !struct_index || !list_index || !children || !tree.valid ||
      parent_node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid recursive child assembly");
  }
  const auto &parent_node = tree.nodes[parent_node_index];
  for (const auto child_node_index : parent_node.children) {
    if (child_node_index >= tree.nodes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive child is invalid");
    }
    NativeRecursiveArrayCursor cursor{*struct_index, *list_index};
    SAN_ASSIGN_OR_RAISE(auto *child_array,
                        materialize_native_recursive_child_node(
                            state, row_group, tree, child_node_index, window,
                            &cursor));
    *struct_index = cursor.struct_index;
    *list_index = cursor.list_index;
    children->push_back(child_array);
  }
  return {};
}

sanitize::Result<ArrowArray *> materialize_native_recursive_map_node(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree, std::size_t map_node_index,
    const NativeFlatRowWindow &window, NativeRecursiveArrayCursor *cursor) {
  if (!state || !cursor || !tree.valid || map_node_index >= tree.nodes.size() ||
      tree.nodes[map_node_index].kind !=
          NativeRecursiveMaterializationNodeKind::Map) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid recursive map node materialization");
  }
  SAN_ASSIGN_OR_RAISE(
      const auto map_layout_index,
      native_recursive_repeated_layout_index_for_node(tree, map_node_index));
  SAN_ASSIGN_OR_RAISE(const auto *layout_column,
                      select_recursive_repeated_layout_column_for_node(
                          row_group, tree, map_node_index));
  SAN_ASSIGN_OR_RAISE(
      const auto map_array_layout,
      native_recursive_map_array_layout(*layout_column, map_layout_index,
                                        window));
  if (cursor->list_index >= state->lists.size() ||
      cursor->struct_index >= state->structs.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive map node allocation mismatch");
  }
  auto &map_array = state->lists[cursor->list_index++];
  auto &entries_array = state->structs[cursor->struct_index++];
  entries_array.children.reserve(tree.nodes[map_node_index].children.size());
  SAN_RETURN_NOT_OK(append_native_recursive_node_children(
      state, row_group, tree, map_node_index, window,
      &cursor->struct_index, &cursor->list_index, &entries_array.children));
  const auto &layout = map_array_layout.repeated_layout;
  SAN_RETURN_NOT_OK(
      configure_native_struct_array(&entries_array, layout.element_count, 0));
  SAN_RETURN_NOT_OK(configure_native_recursive_list_array(
      &map_array, layout, &entries_array.array));
  return &map_array.array;
}

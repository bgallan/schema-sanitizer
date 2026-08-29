// Allocates recursive Arrow struct, list, and map shells and fills their
// container validity. This fragment is included by the consolidated footer
// reader inside its private implementation namespace.

/// Builds one struct node's validity bitmap in its row, list-element, or
/// map-entry domain.
sanitize::Result<NativeRecursiveStructValidity>
materialize_native_recursive_struct_validity_for_node(
    const RowGroupInfo &row_group, const ColumnChunkInfo &layout_column,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t struct_node_index, const NativeFlatRowWindow &window,
    std::int16_t defined_level, std::vector<std::uint8_t> *validity) {
  (void)row_group;
  if (!tree.valid || struct_node_index >= tree.nodes.size() ||
      tree.nodes[struct_node_index].kind !=
          NativeRecursiveMaterializationNodeKind::Struct) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive struct validity node is invalid");
  }
  const auto &struct_node = tree.nodes[struct_node_index];
  switch (struct_node.struct_validity_domain) {
  case NativeRecursiveStructValidityDomain::MapEntry: {
    if (!struct_node.struct_validity_repeated_layout_index.has_value()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive map-entry struct validity layout "
          "is missing");
    }
    return materialize_native_recursive_map_value_struct_validity(
        layout_column,
        struct_node.struct_validity_repeated_layout_index.value(), window,
        defined_level, validity);
  }
  case NativeRecursiveStructValidityDomain::ListElement: {
    if (!struct_node.struct_validity_repeated_layout_index.has_value()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive list-element struct validity "
          "layout is missing");
    }
    return materialize_native_recursive_list_element_struct_validity(
        layout_column,
        struct_node.struct_validity_repeated_layout_index.value(), window,
        defined_level, validity);
  }
  case NativeRecursiveStructValidityDomain::RowGroup: {
    SAN_ASSIGN_OR_RAISE(const auto null_count,
                        materialize_struct_validity_at_definition_level(
                            layout_column, window, defined_level, validity));
    return NativeRecursiveStructValidity{window.row_count, null_count};
  }
  case NativeRecursiveStructValidityDomain::None:
    break;
  }
  return sanitize::Status::Invalid(
      "native Parquet reader: recursive struct validity context is invalid");
}

/// Selects recursive struct layout column for node deterministically from
/// supported layout and resource candidates.
sanitize::Result<const ColumnChunkInfo *>
select_recursive_struct_layout_column_for_node(
    const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t struct_node_index) {
  if (!tree.valid || struct_node_index >= tree.nodes.size() ||
      tree.nodes[struct_node_index].kind !=
          NativeRecursiveMaterializationNodeKind::Struct) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive struct layout node is invalid");
  }
  return native_recursive_layout_column_for_node(row_group, tree,
                                                 struct_node_index);
}

/// Builds one list node's Arrow shell and recursively materializes its element
/// child.
sanitize::Result<ArrowArray *> materialize_native_recursive_list_node(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree, std::size_t list_node_index,
    const NativeFlatRowWindow &window, NativeRecursiveArrayCursor *cursor) {
  if (!state || !cursor || !tree.valid ||
      list_node_index >= tree.nodes.size() ||
      tree.nodes[list_node_index].kind !=
          NativeRecursiveMaterializationNodeKind::List ||
      tree.nodes[list_node_index].children.size() != 1) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid recursive list-struct child");
  }
  const auto child_node_index = tree.nodes[list_node_index].children.front();
  if (child_node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive complex list child is invalid");
  }
  SAN_ASSIGN_OR_RAISE(
      const auto list_layout_index,
      native_recursive_repeated_layout_index_for_node(tree, list_node_index));
  SAN_ASSIGN_OR_RAISE(const auto *layout_column,
                      select_recursive_repeated_layout_column_for_node(
                          row_group, tree, list_node_index));
  SAN_ASSIGN_OR_RAISE(const auto list_array_layout,
                      native_recursive_list_array_layout(
                          *layout_column, list_layout_index, window));
  const auto &list_layout = list_array_layout.repeated_layout;
  if (cursor->list_index >= state->lists.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive complex list allocation mismatch");
  }
  auto &list_array = state->lists[cursor->list_index++];
  SAN_ASSIGN_OR_RAISE(
      auto *child_array,
      materialize_native_recursive_child_node(
          state, row_group, tree, child_node_index, window, cursor));
  SAN_RETURN_NOT_OK(configure_native_recursive_list_array(
      &list_array, list_layout, child_array));
  return &list_array.array;
}

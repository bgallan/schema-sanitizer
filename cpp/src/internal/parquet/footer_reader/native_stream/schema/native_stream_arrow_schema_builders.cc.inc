// Builds recursive Arrow map, struct, and list schemas for native Parquet
// output. This fragment is included by the consolidated footer reader inside
// its private implementation namespace.

sanitize::Status build_native_recursive_map_schema(
    const NativeRecursiveSchemaSource &source,
    const NativeRecursiveMaterializationTree &tree,
    const std::vector<NativeRecursiveMaterializationResourceCounts>
        &subtree_counts,
    std::size_t node_index, NativeParquetMapSchema *map_schema, bool nullable) {
  if (!map_schema || node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive map schema node is invalid");
  }
  const auto &node = tree.nodes[node_index];
  if (node.kind != NativeRecursiveMaterializationNodeKind::Map) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive map schema node is not a map");
  }
  map_schema->name = node.name;
  auto &entries = map_schema->entries;
  entries.name = "entries";
  entries.child_ptrs.reserve(node.children.size());
  SAN_ASSIGN_OR_RAISE(
      const auto counts,
      native_recursive_schema_resource_counts(subtree_counts, node_index));
  entries.children.reserve(counts.leaf_count);
  entries.struct_children.reserve(counts.struct_count);
  entries.map_children.reserve(counts.list_count);
  for (const auto child_index : node.children) {
    SAN_ASSIGN_OR_RAISE(auto *child_schema, build_native_recursive_schema_node(
                                                source, tree, subtree_counts,
                                                child_index, &entries));
    entries.child_ptrs.push_back(child_schema);
  }
  finalize_native_struct_schema(&entries, false);
  finalize_native_map_schema(map_schema, nullable);
  return {};
}

/// Builds native recursive struct schema from validated Parquet metadata while
/// preserving Arrow ownership and nesting contracts.
sanitize::Status build_native_recursive_struct_schema(
    const NativeRecursiveSchemaSource &source,
    const NativeRecursiveMaterializationTree &tree,
    const std::vector<NativeRecursiveMaterializationResourceCounts>
        &subtree_counts,
    std::size_t node_index, NativeParquetStructSchema *struct_schema,
    bool nullable) {
  if (!struct_schema || node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive struct schema node is invalid");
  }
  const auto &node = tree.nodes[node_index];
  if (node.kind != NativeRecursiveMaterializationNodeKind::Struct) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive schema node is not a struct");
  }
  struct_schema->name = node.name;
  struct_schema->child_ptrs.reserve(node.children.size());
  SAN_ASSIGN_OR_RAISE(
      const auto counts,
      native_recursive_schema_resource_counts(subtree_counts, node_index));
  struct_schema->children.reserve(counts.leaf_count);
  struct_schema->struct_children.reserve(counts.struct_count);
  struct_schema->map_children.reserve(counts.list_count);
  struct_schema->complex_list_children.reserve(counts.list_count);
  for (const auto child_index : node.children) {
    SAN_ASSIGN_OR_RAISE(auto *child_schema, build_native_recursive_schema_node(
                                                source, tree, subtree_counts,
                                                child_index, struct_schema));
    struct_schema->child_ptrs.push_back(child_schema);
  }
  finalize_native_struct_schema(struct_schema, nullable);
  return {};
}

/// Builds native recursive complex list schema from validated Parquet metadata
/// while preserving Arrow ownership and nesting contracts.
sanitize::Status build_native_recursive_complex_list_schema(
    const NativeRecursiveSchemaSource &source,
    const NativeRecursiveMaterializationTree &tree,
    const std::vector<NativeRecursiveMaterializationResourceCounts>
        &subtree_counts,
    std::size_t node_index, NativeParquetListSchema *list_schema,
    bool nullable) {
  if (!list_schema || node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive complex list schema node is "
        "invalid");
  }
  const auto &node = tree.nodes[node_index];
  if (node.kind != NativeRecursiveMaterializationNodeKind::List ||
      node.children.size() != 1) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive complex list schema node is not a "
        "single-child list");
  }
  list_schema->name = node.name;
  SAN_ASSIGN_OR_RAISE(
      const auto counts,
      native_recursive_schema_resource_counts(subtree_counts, node_index));
  list_schema->complex_list_children.reserve(counts.list_count);
  const auto child_index = node.children.front();
  if (child_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive complex list schema child is "
        "invalid");
  }
  const auto &child = tree.nodes[child_index];
  if (child.kind == NativeRecursiveMaterializationNodeKind::Leaf) {
    if (!child.leaf_column_index.has_value()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive list leaf has no column");
    }
    SAN_ASSIGN_OR_RAISE(const auto leaf,
                        native_recursive_schema_leaf_info(
                            source, child.leaf_column_index.value()));
    SAN_ASSIGN_OR_RAISE(
        const auto required_definition_level,
        native_recursive_schema_leaf_required_definition_level(leaf));
    initialize_native_column_schema(&list_schema->child, child.name,
                                    leaf.format, leaf.max_definition_level,
                                    required_definition_level);
    list_schema->child_ptrs[0] = &list_schema->child.schema;
  } else if (child.kind == NativeRecursiveMaterializationNodeKind::Struct) {
    SAN_RETURN_NOT_OK(build_native_recursive_struct_schema(
        source, tree, subtree_counts, child_index, &list_schema->struct_child,
        child.nullable));
    list_schema->child_ptrs[0] = &list_schema->struct_child.schema;
  } else if (child.kind == NativeRecursiveMaterializationNodeKind::Map) {
    SAN_RETURN_NOT_OK(build_native_recursive_map_schema(
        source, tree, subtree_counts, child_index, &list_schema->map_child,
        child.nullable));
    list_schema->child_ptrs[0] = &list_schema->map_child.schema;
  } else if (child.kind == NativeRecursiveMaterializationNodeKind::List) {
    auto nested_list = std::unique_ptr<NativeParquetListSchema>(
        new (std::nothrow) NativeParquetListSchema());
    if (!nested_list) {
      return sanitize::Status::OutOfMemory(
          "native Parquet reader recursive nested list schema OOM");
    }
    auto *raw_nested_list = nested_list.get();
    SAN_RETURN_NOT_OK(build_native_recursive_complex_list_schema(
        source, tree, subtree_counts, child_index, raw_nested_list,
        child.nullable));
    list_schema->child_ptrs[0] = &raw_nested_list->schema;
    list_schema->complex_list_children.push_back(std::move(nested_list));
  } else {
    return sanitize::Status::Invalid(
        "native Parquet reader: unknown recursive complex list schema child");
  }
  finalize_native_list_schema(list_schema, nullable);
  return {};
}

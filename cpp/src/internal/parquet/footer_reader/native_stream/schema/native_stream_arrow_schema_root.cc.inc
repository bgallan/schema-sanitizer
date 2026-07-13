sanitize::Result<ArrowSchema *> build_native_recursive_schema_node(
    const NativeRecursiveSchemaSource &source,
    const NativeRecursiveMaterializationTree &tree,
    const std::vector<NativeRecursiveMaterializationResourceCounts>
        &subtree_counts,
    std::size_t node_index, NativeParquetStructSchema *owner) {
  if (!owner || node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive schema child node is invalid");
  }
  const auto &node = tree.nodes[node_index];
  switch (node.kind) {
  case NativeRecursiveMaterializationNodeKind::Leaf: {
    if (!node.leaf_column_index.has_value()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive schema leaf has no column index");
    }
    SAN_ASSIGN_OR_RAISE(const auto leaf,
                        native_recursive_schema_leaf_info(
                            source, node.leaf_column_index.value()));
    SAN_ASSIGN_OR_RAISE(
        const auto required_definition_level,
        native_recursive_schema_leaf_required_definition_level(leaf));
    auto &child = owner->children.emplace_back();
    initialize_native_column_schema(&child, node.name, leaf.format,
                                    leaf.max_definition_level,
                                    required_definition_level);
    return &child.schema;
  }
  case NativeRecursiveMaterializationNodeKind::List: {
    if (node.children.size() != 1) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: recursive schema complex list is ambiguous");
    }
    auto list_child = std::unique_ptr<NativeParquetListSchema>(
        new (std::nothrow) NativeParquetListSchema());
    if (!list_child) {
      return sanitize::Status::OutOfMemory(
          "native Parquet reader recursive list schema OOM");
    }
    auto *raw_list_child = list_child.get();
    SAN_RETURN_NOT_OK(build_native_recursive_complex_list_schema(
        source, tree, subtree_counts, node_index, raw_list_child,
        node.nullable));
    owner->complex_list_children.push_back(std::move(list_child));
    return &raw_list_child->schema;
  }
  case NativeRecursiveMaterializationNodeKind::Struct: {
    auto &child = owner->struct_children.emplace_back();
    SAN_RETURN_NOT_OK(build_native_recursive_struct_schema(
        source, tree, subtree_counts, node_index, &child, node.nullable));
    return &child.schema;
  }
  case NativeRecursiveMaterializationNodeKind::Map: {
    auto map_child = std::unique_ptr<NativeParquetMapSchema>(
        new (std::nothrow) NativeParquetMapSchema());
    if (!map_child) {
      return sanitize::Status::OutOfMemory(
          "native Parquet reader recursive map schema OOM");
    }
    auto *raw_map_child = map_child.get();
    owner->map_children.push_back(std::move(map_child));
    SAN_RETURN_NOT_OK(build_native_recursive_map_schema(
        source, tree, subtree_counts, node_index, raw_map_child,
        node.nullable));
    return &raw_map_child->schema;
  }
  }
  return sanitize::Status::Invalid(
      "native Parquet reader: unknown recursive schema node kind");
}

sanitize::Status
finalize_native_stream_output_layout_plan(NativeParquetStreamState *stream) {
  if (!stream) {
    return sanitize::Status::Invalid(
        "native Parquet reader: stream state is null");
  }
  stream->recursive_struct_array_count = std::accumulate(
      stream->output_layout.begin(), stream->output_layout.end(),
      std::size_t{0},
      [](std::size_t total, const NativeParquetOutputField &field) {
        return total + field.recursive_counts.struct_count;
      });
  stream->recursive_list_array_count = std::accumulate(
      stream->output_layout.begin(), stream->output_layout.end(),
      std::size_t{0},
      [](std::size_t total, const NativeParquetOutputField &field) {
        return total + field.recursive_counts.list_count;
      });
  stream->output_layout_initialized = true;
  return {};
}

sanitize::Status
initialize_native_stream_output_layout(NativeParquetStreamState *stream) {
  if (!stream) {
    return sanitize::Status::Invalid(
        "native Parquet reader: stream state is null");
  }
  if (stream->output_layout_initialized) {
    return {};
  }

  if (!stream->footer.row_groups.empty()) {
    SAN_RETURN_NOT_OK(build_native_output_layout(
        stream->footer.row_groups.front().columns, &stream->output_layout));
  } else {
    SAN_ASSIGN_OR_RAISE(stream->empty_file_leaves,
                        schema_leaf_levels(stream->footer.schema_elements));
    std::vector<LeafLevelInfo> projected_leaves;
    SAN_RETURN_NOT_OK(project_leaf_levels_for_columns(
        stream->empty_file_leaves, stream->footer.projected_columns,
        &projected_leaves));
    stream->empty_file_leaves = std::move(projected_leaves);
    for (const auto &leaf : stream->empty_file_leaves) {
      if (!native_plain_path_is_materializable(
              leaf.path, leaf.max_repetition_level, leaf.top_level_required) ||
          leaf.native_arrow_format.empty()) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: empty file schema is not materializable");
      }
    }
    SAN_RETURN_NOT_OK(build_native_output_layout(stream->empty_file_leaves,
                                                 &stream->output_layout));
  }

  return finalize_native_stream_output_layout_plan(stream);
}

sanitize::Status build_native_schema(NativeParquetStreamState *stream,
                                     ArrowSchema *out) {
  if (!stream || !out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output schema is null");
  }
  auto state = std::unique_ptr<NativeParquetSchemaState>(
      new (std::nothrow) NativeParquetSchemaState());
  if (!state) {
    return sanitize::Status::OutOfMemory("native Parquet reader schema OOM");
  }
  SAN_RETURN_NOT_OK(initialize_native_stream_output_layout(stream));
  const RowGroupInfo *row_group = stream->footer.row_groups.empty()
                                      ? nullptr
                                      : &stream->footer.row_groups.front();
  const auto &layout = stream->output_layout;
  state->fields.resize(layout.size());
  state->children.reserve(layout.size());
  const NativeRecursiveSchemaSource schema_source{row_group,
                                                  &stream->empty_file_leaves};
  for (std::size_t field_index = 0; field_index < layout.size();
       ++field_index) {
    const auto &field = layout[field_index];
    auto &top_level = state->fields[field_index];
    if (!field.recursive_tree.valid ||
        field.recursive_tree.root >= field.recursive_tree.nodes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive schema tree is invalid");
    }
    const auto &root_node =
        field.recursive_tree.nodes[field.recursive_tree.root];
    const auto &subtree_counts = field.recursive_subtree_counts;
    if (subtree_counts.size() != field.recursive_tree.nodes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive schema resource plan is invalid");
    }
    if (root_node.kind == NativeRecursiveMaterializationNodeKind::Map) {
      auto &map_node = top_level.map_node;
      SAN_RETURN_NOT_OK(build_native_recursive_map_schema(
          schema_source, field.recursive_tree, subtree_counts,
          field.recursive_tree.root, &map_node, root_node.nullable));
      state->children.push_back(&map_node.schema);
      continue;
    }
    if (root_node.kind == NativeRecursiveMaterializationNodeKind::List) {
      auto &list_node = top_level.list_node;
      SAN_RETURN_NOT_OK(build_native_recursive_complex_list_schema(
          schema_source, field.recursive_tree, subtree_counts,
          field.recursive_tree.root, &list_node, root_node.nullable));
      state->children.push_back(&list_node.schema);
      continue;
    }
    if (root_node.kind == NativeRecursiveMaterializationNodeKind::Struct) {
      auto &struct_node = top_level.struct_node;
      SAN_RETURN_NOT_OK(build_native_recursive_struct_schema(
          schema_source, field.recursive_tree, subtree_counts,
          field.recursive_tree.root, &struct_node, root_node.nullable));
      state->children.push_back(&struct_node.schema);
      continue;
    }
    if (root_node.kind != NativeRecursiveMaterializationNodeKind::Leaf) {
      return sanitize::Status::Invalid(
          "native Parquet reader: unknown recursive schema root kind");
    }
    if (field.column_indices.size() != 1) {
      return sanitize::Status::Invalid(
          "native Parquet reader: scalar schema field has invalid leaf count");
    }
    SAN_ASSIGN_OR_RAISE(const auto leaf,
                        native_recursive_schema_leaf_info(
                            schema_source, field.column_indices.front()));
    SAN_ASSIGN_OR_RAISE(
        const auto required_definition_level,
        native_recursive_schema_leaf_required_definition_level(leaf));
    auto &child = top_level.leaf;
    initialize_native_column_schema(&child, field.name, leaf.format,
                                    leaf.max_definition_level,
                                    required_definition_level);
    state->children.push_back(&child.schema);
  }
  sanitize::internal::cdata_stream::clear_schema(out);
  out->format = state->root_format.c_str();
  out->name = nullptr;
  out->metadata = nullptr;
  out->flags = 0;
  out->n_children = static_cast<std::int64_t>(state->children.size());
  out->children = state->children.empty() ? nullptr : state->children.data();
  out->dictionary = nullptr;
  out->private_data = state.release();
  out->release = &native_parquet_schema_release;
  return {};
}

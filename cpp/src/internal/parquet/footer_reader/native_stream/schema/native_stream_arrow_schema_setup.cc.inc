// Native stream Arrow schema construction helpers.
// Included directly by footer_reader.cc.

struct NativeRecursiveSchemaLeafInfo {
  std::vector<std::string> path;
  std::vector<std::int16_t> path_definition_levels;
  std::string format;
  std::int16_t max_definition_level = 0;
  std::int16_t max_repetition_level = 0;
  bool top_level_required = true;
};

struct NativeRecursiveSchemaSource {
  const RowGroupInfo *row_group = nullptr;
  const std::vector<LeafLevelInfo> *empty_file_leaves = nullptr;
};

sanitize::Result<NativeRecursiveSchemaLeafInfo>
native_recursive_schema_leaf_info(const NativeRecursiveSchemaSource &source,
                                  std::size_t column_index) {
  if (source.row_group) {
    if (column_index >= source.row_group->columns.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive schema column index is invalid");
    }
    const auto &column = source.row_group->columns[column_index];
    return NativeRecursiveSchemaLeafInfo{
        column.path_in_schema,       column.path_definition_levels,
        column.native_arrow_format,  column.max_definition_level,
        column.max_repetition_level, column.top_level_required};
  }
  if (!source.empty_file_leaves ||
      column_index >= source.empty_file_leaves->size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive empty-file schema column index is "
        "invalid");
  }
  const auto &leaf = (*source.empty_file_leaves)[column_index];
  return NativeRecursiveSchemaLeafInfo{leaf.path,
                                       leaf.path_definition_levels,
                                       leaf.native_arrow_format,
                                       leaf.max_definition_level,
                                       leaf.max_repetition_level,
                                       leaf.top_level_required};
}

sanitize::Result<std::int16_t>
native_recursive_schema_leaf_required_definition_level(
    const NativeRecursiveSchemaLeafInfo &leaf) {
  const auto &path = leaf.path;
  if (path.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive schema leaf path is empty");
  }
  if (path.size() == 1) {
    return std::int16_t{0};
  }
  const auto parent_level_index = path.size() - 2;
  if (parent_level_index >= leaf.path_definition_levels.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive schema leaf definition levels are "
        "invalid");
  }
  return leaf.path_definition_levels[parent_level_index];
}

void initialize_native_column_schema(NativeParquetColumnSchema *column,
                                     std::string name, std::string format,
                                     std::int16_t max_definition_level,
                                     std::int16_t required_definition_level) {
  column->name = std::move(name);
  column->format = std::move(format);
  sanitize::internal::cdata_stream::clear_schema(&column->schema);
  column->schema.format = column->format.c_str();
  column->schema.name = column->name.c_str();
  column->schema.metadata = nullptr;
  column->schema.flags = max_definition_level > required_definition_level
                             ? ARROW_FLAG_NULLABLE
                             : 0;
  column->schema.n_children = 0;
  column->schema.children = nullptr;
  column->schema.dictionary = nullptr;
  column->schema.private_data = nullptr;
  column->schema.release = &native_parquet_schema_child_release;
}

void finalize_native_struct_schema(NativeParquetStructSchema *schema,
                                   bool nullable) {
  sanitize::internal::cdata_stream::clear_schema(&schema->schema);
  schema->schema.format = schema->format.c_str();
  schema->schema.name = schema->name.c_str();
  schema->schema.metadata = nullptr;
  schema->schema.flags = nullable ? ARROW_FLAG_NULLABLE : 0;
  schema->schema.n_children =
      static_cast<std::int64_t>(schema->child_ptrs.size());
  schema->schema.children =
      schema->child_ptrs.empty() ? nullptr : schema->child_ptrs.data();
  schema->schema.dictionary = nullptr;
  schema->schema.private_data = nullptr;
  schema->schema.release = &native_parquet_schema_child_release;
}

void finalize_native_map_schema(NativeParquetMapSchema *schema, bool nullable) {
  schema->child_ptrs[0] = &schema->entries.schema;
  sanitize::internal::cdata_stream::clear_schema(&schema->schema);
  schema->schema.format = schema->format.c_str();
  schema->schema.name = schema->name.c_str();
  schema->schema.metadata = nullptr;
  schema->schema.flags = nullable ? ARROW_FLAG_NULLABLE : 0;
  schema->schema.n_children = 1;
  schema->schema.children = schema->child_ptrs.data();
  schema->schema.dictionary = nullptr;
  schema->schema.private_data = nullptr;
  schema->schema.release = &native_parquet_schema_child_release;
}

void finalize_native_list_schema(NativeParquetListSchema *schema,
                                 bool nullable) {
  sanitize::internal::cdata_stream::clear_schema(&schema->schema);
  schema->schema.format = schema->format.c_str();
  schema->schema.name = schema->name.c_str();
  schema->schema.metadata = nullptr;
  schema->schema.flags = nullable ? ARROW_FLAG_NULLABLE : 0;
  schema->schema.n_children = 1;
  schema->schema.children = schema->child_ptrs.data();
  schema->schema.dictionary = nullptr;
  schema->schema.private_data = nullptr;
  schema->schema.release = &native_parquet_schema_child_release;
}

sanitize::Result<NativeRecursiveMaterializationResourceCounts>
native_recursive_schema_resource_counts(
    const std::vector<NativeRecursiveMaterializationResourceCounts>
        &subtree_counts,
    std::size_t node_index) {
  if (node_index >= subtree_counts.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive schema resource count node is "
        "invalid");
  }
  return subtree_counts[node_index];
}

sanitize::Result<ArrowSchema *> build_native_recursive_schema_node(
    const NativeRecursiveSchemaSource &source,
    const NativeRecursiveMaterializationTree &tree,
    const std::vector<NativeRecursiveMaterializationResourceCounts>
        &subtree_counts,
    std::size_t node_index, NativeParquetStructSchema *owner);

sanitize::Status build_native_recursive_struct_schema(
    const NativeRecursiveSchemaSource &source,
    const NativeRecursiveMaterializationTree &tree,
    const std::vector<NativeRecursiveMaterializationResourceCounts>
        &subtree_counts,
    std::size_t node_index, NativeParquetStructSchema *struct_schema,
    bool nullable);

sanitize::Status build_native_recursive_map_schema(
    const NativeRecursiveSchemaSource &source,
    const NativeRecursiveMaterializationTree &tree,
    const std::vector<NativeRecursiveMaterializationResourceCounts>
        &subtree_counts,
    std::size_t node_index, NativeParquetMapSchema *map_schema, bool nullable);

sanitize::Status build_native_recursive_complex_list_schema(
    const NativeRecursiveSchemaSource &source,
    const NativeRecursiveMaterializationTree &tree,
    const std::vector<NativeRecursiveMaterializationResourceCounts>
        &subtree_counts,
    std::size_t node_index, NativeParquetListSchema *list_schema,
    bool nullable);

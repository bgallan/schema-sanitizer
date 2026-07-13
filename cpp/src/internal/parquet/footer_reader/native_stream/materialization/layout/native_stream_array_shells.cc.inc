// Native recursive Arrow array shells and materialization cursor declarations.
// Included directly by footer_reader.cc.

struct NativeRepeatedLevelLayoutView {
  bool decoded = false;
  std::int64_t row_count = 0;
  std::int64_t null_count = 0;
  std::int64_t element_count = 0;
  const std::vector<std::int32_t> *offsets = nullptr;
  const std::vector<std::uint8_t> *validity_bitmap = nullptr;
};

sanitize::Result<NativeRepeatedLevelLayoutView>
native_repeated_level_layout(const ColumnChunkInfo &column,
                             std::size_t layout_index) {
  if (layout_index < column.repeated_level_layouts.size()) {
    const auto &layout = column.repeated_level_layouts[layout_index];
    if (layout.decoded) {
      return NativeRepeatedLevelLayoutView{true,
                                           layout.row_count,
                                           layout.null_count,
                                           layout.element_count,
                                           &layout.offsets,
                                           &layout.validity_bitmap};
    }
  }
  return sanitize::Status::NotImplemented(
      "native Parquet reader: recursive repeated layout was not decoded");
}

sanitize::Status
configure_native_list_array(NativeParquetListArray *list_array,
                            const std::vector<std::uint8_t> &validity,
                            const std::vector<std::int32_t> &offsets,
                            ArrowArray *child, std::int64_t length,
                            std::int64_t null_count) {
  if (!list_array || !child || length < 0 || null_count < 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid list array shell");
  }
  list_array->validity = validity;
  list_array->offsets = offsets;
  list_array->children[0] = child;
  list_array->array.length = length;
  list_array->array.null_count = null_count;
  list_array->array.offset = 0;
  list_array->array.n_buffers = 2;
  list_array->buffers[0] =
      list_array->validity.empty() ? nullptr : list_array->validity.data();
  list_array->buffers[1] =
      list_array->offsets.empty() ? nullptr : list_array->offsets.data();
  list_array->array.buffers = list_array->buffers.data();
  list_array->array.n_children = 1;
  list_array->array.children = list_array->children.data();
  list_array->array.dictionary = nullptr;
  list_array->array.private_data = nullptr;
  list_array->array.release = &native_parquet_array_child_release;
  return {};
}

sanitize::Status
configure_native_struct_array(NativeParquetStructArray *struct_array,
                              std::int64_t length, std::int64_t null_count) {
  if (!struct_array || length < 0 || null_count < 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid struct array shell");
  }
  struct_array->array.length = length;
  struct_array->array.null_count = null_count;
  struct_array->array.offset = 0;
  struct_array->array.n_buffers = 1;
  struct_array->buffers[0] =
      struct_array->validity.empty() ? nullptr : struct_array->validity.data();
  struct_array->array.buffers = struct_array->buffers.data();
  struct_array->array.n_children =
      static_cast<std::int64_t>(struct_array->children.size());
  struct_array->array.children =
      struct_array->children.empty() ? nullptr : struct_array->children.data();
  struct_array->array.dictionary = nullptr;
  struct_array->array.private_data = nullptr;
  struct_array->array.release = &native_parquet_array_child_release;
  return {};
}

struct NativeRecursiveArrayCursor {
  std::size_t struct_index = 0;
  std::size_t list_index = 0;
};

sanitize::Result<ArrowArray *> materialize_native_recursive_map_node(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree, std::size_t map_node_index,
    NativeRecursiveArrayCursor *cursor);

sanitize::Result<ArrowArray *> materialize_native_recursive_child_node(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t child_node_index, NativeRecursiveArrayCursor *cursor);

sanitize::Result<ArrowArray *> materialize_native_recursive_struct_node(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t struct_node_index, NativeRecursiveArrayCursor *cursor);

sanitize::Status append_native_recursive_node_children(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t parent_node_index, std::size_t *struct_index,
    std::size_t *list_index, std::vector<ArrowArray *> *children);

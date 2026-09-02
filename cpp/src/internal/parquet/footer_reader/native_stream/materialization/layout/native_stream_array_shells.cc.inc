// Declares native recursive Arrow array shells and materialization cursors.
// This fragment is included directly by footer_reader.cc.

struct NativeRepeatedLevelLayoutView {
  bool decoded = false;
  std::int64_t row_count = 0;
  std::int64_t null_count = 0;
  std::int64_t element_count = 0;
  const std::vector<std::int32_t> *offsets = nullptr;
  const std::vector<std::uint8_t> *validity_bitmap = nullptr;
};

struct NativeRepeatedLevelLayoutWindow {
  bool decoded = false;
  std::int64_t row_count = 0;
  std::int64_t null_count = 0;
  std::int64_t element_count = 0;
  std::vector<std::int32_t> offsets;
  std::vector<std::uint8_t> validity_bitmap;
};

/// Derives the repeated-level domain covering the selected flat row window.
sanitize::Result<NativeElementRange>
native_repeated_domain_range(const ColumnChunkInfo &column,
                             std::size_t layout_index,
                             const NativeFlatRowWindow &window) {
  SAN_RETURN_NOT_OK(validate_native_flat_row_window(window));
  if (layout_index >= column.repeated_level_layouts.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: repeated layout index is invalid");
  }
  NativeElementRange range{window.first_row, window.row_count};
  for (std::size_t index = 0; index < layout_index; ++index) {
    const auto &layout = column.repeated_level_layouts[index];
    if (!layout.decoded || range.first < 0 || range.count < 0 ||
        range.end() < range.first ||
        range.end() >= static_cast<std::int64_t>(layout.offsets.size())) {
      return sanitize::Status::Invalid(
          "native Parquet reader: repeated window domain is invalid");
    }
    const auto first = layout.offsets[static_cast<std::size_t>(range.first)];
    const auto last = layout.offsets[static_cast<std::size_t>(range.end())];
    if (first < 0 || last < first) {
      return sanitize::Status::Invalid(
          "native Parquet reader: repeated window offsets are invalid");
    }
    range = NativeElementRange{first, last - first};
  }
  return range;
}

/// Derives the repeated element interval represented by a bounded level-layout
/// window.
sanitize::Result<NativeElementRange>
native_repeated_element_range(const ColumnChunkInfo &column,
                              std::size_t layout_index,
                              const NativeFlatRowWindow &window) {
  SAN_ASSIGN_OR_RAISE(
      auto domain, native_repeated_domain_range(column, layout_index, window));
  const auto &layout = column.repeated_level_layouts[layout_index];
  if (!layout.decoded || domain.end() < domain.first ||
      domain.end() >= static_cast<std::int64_t>(layout.offsets.size())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: repeated element window is invalid");
  }
  const auto first = layout.offsets[static_cast<std::size_t>(domain.first)];
  const auto last = layout.offsets[static_cast<std::size_t>(domain.end())];
  if (first < 0 || last < first) {
    return sanitize::Status::Invalid(
        "native Parquet reader: repeated element offsets are invalid");
  }
  return NativeElementRange{first, last - first};
}

/// Maps a repeated element interval to the corresponding leaf-value window.
sanitize::Result<NativeRepeatedLeafWindow>
native_repeated_leaf_window(const ColumnChunkInfo &column,
                            const NativeFlatRowWindow &window) {
  if (column.repeated_level_layouts.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: repeated leaf has no layouts");
  }
  SAN_ASSIGN_OR_RAISE(
      const auto elements,
      native_repeated_element_range(
          column, column.repeated_level_layouts.size() - 1, window));
  const auto &deepest = column.repeated_level_layouts.back();
  if (!deepest.decoded || deepest.element_count < 0 ||
      elements.end() > deepest.element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: repeated leaf window exceeds element count");
  }
  return NativeRepeatedLeafWindow{deepest.element_count, elements};
}

/// Copies validity bitmap range with bounds checks and the required Arrow
/// physical representation.
void copy_validity_bitmap_range(const std::vector<std::uint8_t> &source,
                                NativeElementRange range,
                                std::vector<std::uint8_t> *target,
                                std::int64_t *null_count) {
  if (!target || !null_count) {
    return;
  }
  target->clear();
  *null_count = 0;
  if (source.empty() || range.count <= 0) {
    return;
  }
  target->assign(static_cast<std::size_t>((range.count + 7) / 8), 0);
  std::int64_t valid = 0;
  for (std::int64_t index = 0; index < range.count; ++index) {
    const auto source_index = range.first + index;
    const auto byte = static_cast<std::size_t>(source_index / 8);
    const auto bit = static_cast<std::uint8_t>(1U << (source_index % 8));
    if (byte < source.size() && (source[byte] & bit) != 0) {
      set_output_validity_bit(target, index);
      ++valid;
    }
  }
  *null_count = range.count - valid;
  if (*null_count == 0) {
    target->clear();
  }
}

/// Slices repeated-level layout metadata to a bounded row window.
sanitize::Result<NativeRepeatedLevelLayoutWindow>
native_repeated_level_layout_window(const ColumnChunkInfo &column,
                                    std::size_t layout_index,
                                    const NativeFlatRowWindow &window) {
  SAN_ASSIGN_OR_RAISE(const auto domain, native_repeated_domain_range(
                                             column, layout_index, window));
  const auto &layout = column.repeated_level_layouts[layout_index];
  if (!layout.decoded || domain.end() < domain.first ||
      domain.end() >= static_cast<std::int64_t>(layout.offsets.size())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: repeated layout window is invalid");
  }
  NativeRepeatedLevelLayoutWindow result;
  result.decoded = true;
  result.row_count = domain.count;
  result.offsets.reserve(static_cast<std::size_t>(domain.count + 1));
  const auto base = layout.offsets[static_cast<std::size_t>(domain.first)];
  for (auto index = domain.first; index <= domain.end(); ++index) {
    const auto value = layout.offsets[static_cast<std::size_t>(index)];
    if (value < base ||
        value - base > std::numeric_limits<std::int32_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: repeated layout window offset overflow");
    }
    result.offsets.push_back(static_cast<std::int32_t>(value - base));
  }
  result.element_count = result.offsets.empty() ? 0 : result.offsets.back();
  copy_validity_bitmap_range(layout.validity_bitmap, domain,
                             &result.validity_bitmap, &result.null_count);
  return result;
}

/// Builds complete repeated-level offsets and validity state for one leaf
/// column.
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

/// Configures native list array buffers and child pointers under the Arrow C
/// Data ownership contract.
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

/// Configures native struct array buffers and child pointers under the Arrow C
/// Data ownership contract.
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
    const NativeFlatRowWindow &window, NativeRecursiveArrayCursor *cursor);

/// Dispatches one recursive child to its leaf, struct, list, or map
/// materializer.
sanitize::Result<ArrowArray *> materialize_native_recursive_child_node(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t child_node_index, const NativeFlatRowWindow &window,
    NativeRecursiveArrayCursor *cursor);

sanitize::Result<ArrowArray *> materialize_native_recursive_struct_node(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t struct_node_index, const NativeFlatRowWindow &window,
    NativeRecursiveArrayCursor *cursor);

sanitize::Status append_native_recursive_node_children(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t parent_node_index, const NativeFlatRowWindow &window,
    std::size_t *struct_index, std::size_t *list_index,
    std::vector<ArrowArray *> *children);

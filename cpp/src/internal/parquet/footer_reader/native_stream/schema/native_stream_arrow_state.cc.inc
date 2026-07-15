// Included by footer_reader.cc inside its implementation namespace.

struct NativeParquetColumnSchema {
  ArrowSchema schema{};
  std::string name;
  std::string format;
};

struct NativeParquetMapSchema;
struct NativeParquetListSchema;

struct NativeParquetStructSchema {
  ArrowSchema schema{};
  std::string name;
  std::string format = "+s";
  std::vector<NativeParquetColumnSchema> children;
  std::vector<NativeParquetStructSchema> struct_children;
  std::vector<std::unique_ptr<NativeParquetMapSchema>> map_children;
  std::vector<std::unique_ptr<NativeParquetListSchema>> complex_list_children;
  std::vector<ArrowSchema *> child_ptrs;
};

struct NativeParquetMapSchema {
  ArrowSchema schema{};
  std::string name;
  std::string format = "+m";
  NativeParquetStructSchema entries;
  std::array<ArrowSchema *, 1> child_ptrs{nullptr};
};

struct NativeParquetListSchema {
  ArrowSchema schema{};
  std::string name;
  std::string format = "+l";
  NativeParquetColumnSchema child;
  NativeParquetStructSchema struct_child;
  std::vector<std::unique_ptr<NativeParquetListSchema>> complex_list_children;
  NativeParquetMapSchema map_child;
  std::array<ArrowSchema *, 1> child_ptrs{nullptr};
};

struct NativeParquetTopLevelSchema {
  NativeParquetColumnSchema leaf;
  NativeParquetStructSchema struct_node;
  NativeParquetListSchema list_node;
  NativeParquetMapSchema map_node;
};

struct NativeParquetSchemaState {
  std::string root_format = "+s";
  std::vector<NativeParquetTopLevelSchema> fields;
  std::vector<ArrowSchema *> children;
};

struct NativeParquetChildArray {
  ArrowArray array{};
  std::vector<std::uint8_t> validity;
  std::vector<std::int32_t> offsets;
  std::vector<std::uint8_t> values;
  std::array<const void *, 3> buffers{nullptr, nullptr, nullptr};
};

struct NativeParquetStructArray {
  ArrowArray array{};
  std::vector<std::uint8_t> validity;
  std::vector<ArrowArray *> children;
  std::array<const void *, 1> buffers{nullptr};
};

struct NativeParquetListArray {
  ArrowArray array{};
  std::vector<std::uint8_t> validity;
  std::vector<std::int32_t> offsets;
  std::array<ArrowArray *, 1> children{nullptr};
  std::array<const void *, 2> buffers{nullptr, nullptr};
};

struct NativeElementRange {
  std::int64_t first = 0;
  std::int64_t count = 0;

  [[nodiscard]] std::int64_t end() const noexcept {
    if (first < 0 || count < 0 ||
        count > std::numeric_limits<std::int64_t>::max() - first) {
      return std::numeric_limits<std::int64_t>::max();
    }
    return first + count;
  }
  [[nodiscard]] bool contains(std::int64_t index) const noexcept {
    return first >= 0 && count >= 0 && index >= first && index < end();
  }
  [[nodiscard]] std::int64_t local(std::int64_t index) const noexcept {
    return index - first;
  }
};

struct NativeRepeatedLeafWindow {
  std::int64_t total_elements = 0;
  NativeElementRange elements;
};

struct NativeFlatRowWindow {
  std::int64_t row_group_rows = 0;
  std::int64_t first_row = 0;
  std::int64_t row_count = 0;

  [[nodiscard]] std::int64_t end_row() const noexcept {
    if (first_row < 0 || row_count < 0 ||
        row_count > std::numeric_limits<std::int64_t>::max() - first_row) {
      return std::numeric_limits<std::int64_t>::max();
    }
    return first_row + row_count;
  }

  [[nodiscard]] bool contains(std::int64_t row) const noexcept {
    return row >= first_row && row < end_row();
  }

  [[nodiscard]] std::int64_t local_row(std::int64_t row) const noexcept {
    return row - first_row;
  }

  [[nodiscard]] bool overlaps(std::int64_t first,
                              std::int64_t count) const noexcept {
    if (first < 0 || count <= 0 ||
        count > std::numeric_limits<std::int64_t>::max() - first) {
      return false;
    }
    return first < end_row() && first + count > first_row;
  }
};

sanitize::Status
validate_native_flat_row_window(const NativeFlatRowWindow &window) {
  if (window.row_group_rows < 0 || window.first_row < 0 ||
      window.row_count < 0 || window.first_row > window.row_group_rows ||
      window.row_count > window.row_group_rows - window.first_row ||
      window.row_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid flat row window");
  }
  return {};
}

bool native_recursive_tree_supports_row_windows(
    const NativeRecursiveMaterializationTree &tree) {
  return tree.valid && tree.traversal_plan_valid &&
         tree.root < tree.nodes.size() &&
         std::ranges::all_of(
             tree.nodes,
             [](const auto &node) {
               switch (node.kind) {
               case NativeRecursiveMaterializationNodeKind::Leaf:
               case NativeRecursiveMaterializationNodeKind::Struct:
               case NativeRecursiveMaterializationNodeKind::List:
               case NativeRecursiveMaterializationNodeKind::Map:
                 return true;
               }
               return false;
             });
}

bool native_output_layout_supports_flat_windows(
    const std::vector<NativeParquetOutputField> &output_layout,
    const RowGroupInfo &row_group) {
  if (output_layout.empty() && row_group.num_rows > 0) {
    return false;
  }
  return std::ranges::all_of(output_layout, [](const auto &field) {
    return native_recursive_tree_supports_row_windows(field.recursive_tree);
  });
}

sanitize::Result<NativeFlatRowWindow> next_native_flat_row_window(
    const RowGroupInfo &row_group,
    const std::vector<NativeParquetOutputField> &output_layout,
    std::int64_t first_row, std::int64_t max_buffer_bytes,
    std::int64_t max_batch_rows) {
  if (!row_group.has_num_rows || row_group.num_rows < 0 || first_row < 0 ||
      first_row > row_group.num_rows) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid row-group window position");
  }
  const bool windowed =
      native_output_layout_supports_flat_windows(output_layout, row_group);
  if (!windowed) {
    SAN_ASSIGN_OR_RAISE(const auto bytes,
                        native_reader_row_group_buffer_bytes(row_group));
    const auto limit = max_buffer_bytes;
    if (bytes > limit) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: nested row group buffer estimate ", bytes,
          " exceeds configured limit ", limit);
    }
    return NativeFlatRowWindow{row_group.num_rows, 0, row_group.num_rows};
  }
  const auto remaining = row_group.num_rows - first_row;
  auto rows = std::min(remaining, max_batch_rows);
  const auto limit = max_buffer_bytes;
  if (rows > 0) {
    SAN_ASSIGN_OR_RAISE(
        const auto one_row_bytes,
        native_reader_flat_window_buffer_bytes(row_group, first_row, 1));
    if (one_row_bytes > limit) {
      return sanitize::Status::NotImplemented(
          "native Parquet reader: one-row buffer estimate ", one_row_bytes,
          " exceeds configured limit ", limit);
    }
  }
  while (rows > 1) {
    SAN_ASSIGN_OR_RAISE(
        const auto bytes,
        native_reader_flat_window_buffer_bytes(row_group, first_row, rows));
    if (bytes <= limit) {
      break;
    }
    rows = std::max<std::int64_t>(1, rows / 2);
  }
  return NativeFlatRowWindow{row_group.num_rows, first_row, rows};
}

std::int64_t native_window_null_count(const NativeParquetChildArray &child,
                                      std::int64_t row_count) {
  if (child.validity.empty()) {
    return 0;
  }
  std::int64_t valid = 0;
  for (std::int64_t row = 0; row < row_count; ++row) {
    if ((child.validity[static_cast<std::size_t>(row / 8)] &
         static_cast<std::uint8_t>(1U << (row % 8))) != 0) {
      ++valid;
    }
  }
  return row_count - valid;
}

struct NativeParquetArrayState {
  std::vector<NativeParquetChildArray> columns;
  std::vector<NativeParquetStructArray> structs;
  std::vector<NativeParquetListArray> lists;
  std::vector<ArrowArray *> children;
  std::array<const void *, 1> struct_buffers{nullptr};
};

struct NativeParquetPageScratch {
  std::string compressed_payload;
  std::string decompressed_payload;
  std::vector<std::uint32_t> dictionary_indices;
  std::vector<std::int64_t> delta_i64_values;
  std::vector<std::int32_t> delta_i32_lengths;
};

struct NativeParquetStreamState {
  std::string path;
  std::ifstream file;
  std::int64_t max_buffer_bytes = 0;
  std::int64_t max_batch_rows = 0;
  NativeParquetPageScratch page_scratch;
  FooterInfo footer;
  std::vector<LeafLevelInfo> empty_file_leaves;
  std::vector<NativeParquetOutputField> output_layout;
  bool output_layout_initialized = false;
  std::size_t recursive_struct_array_count = 0;
  std::size_t recursive_list_array_count = 0;
  std::size_t row_group_index = 0;
  std::int64_t row_group_row_offset = 0;
  std::string last_error;
};

void native_parquet_schema_child_release(ArrowSchema *schema) {
  if (!schema || !schema->release) {
    return;
  }
  sanitize::internal::cdata_stream::clear_schema(schema);
}

void native_parquet_schema_release(ArrowSchema *schema) {
  if (!schema || !schema->release) {
    return;
  }
  auto *state = static_cast<NativeParquetSchemaState *>(schema->private_data);
  delete state;
  sanitize::internal::cdata_stream::clear_schema(schema);
}

void native_parquet_array_child_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  sanitize::internal::cdata_stream::clear_array(array);
}

void native_parquet_array_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  auto *state = static_cast<NativeParquetArrayState *>(array->private_data);
  delete state;
  sanitize::internal::cdata_stream::clear_array(array);
}

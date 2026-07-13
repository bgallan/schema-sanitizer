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
  NativeParquetPageScratch page_scratch;
  FooterInfo footer;
  std::vector<LeafLevelInfo> empty_file_leaves;
  std::vector<NativeParquetOutputField> output_layout;
  bool output_layout_initialized = false;
  std::size_t recursive_struct_array_count = 0;
  std::size_t recursive_list_array_count = 0;
  std::size_t row_group_index = 0;
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

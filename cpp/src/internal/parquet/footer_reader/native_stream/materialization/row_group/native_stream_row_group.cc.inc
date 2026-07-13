// ---- native_stream_output_field ----

sanitize::Result<ArrowArray *> materialize_native_recursive_output_field(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeParquetOutputField &field, NativeRecursiveArrayCursor *cursor) {
  if (!state || !cursor || field.column_indices.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid recursive output field");
  }
  const auto column_index = field.column_indices.front();
  if (column_index >= row_group.columns.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive output column is invalid");
  }
  if (!field.recursive_tree.valid ||
      field.recursive_tree.root >= field.recursive_tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive output tree is invalid");
  }
  const auto &root_node = field.recursive_tree.nodes[field.recursive_tree.root];
  if (root_node.kind == NativeRecursiveMaterializationNodeKind::Map) {
    return materialize_native_recursive_map_node(
        state, row_group, field.recursive_tree, field.recursive_tree.root,
        cursor);
  }
  if (root_node.kind == NativeRecursiveMaterializationNodeKind::List) {
    return materialize_native_recursive_list_node(
        state, row_group, field.recursive_tree, field.recursive_tree.root,
        cursor);
  }
  if (root_node.kind == NativeRecursiveMaterializationNodeKind::Struct) {
    return materialize_native_recursive_struct_node(
        state, row_group, field.recursive_tree, field.recursive_tree.root,
        cursor);
  }
  if (root_node.kind != NativeRecursiveMaterializationNodeKind::Leaf) {
    return sanitize::Status::Invalid(
        "native Parquet reader: unknown recursive output root kind");
  }
  return &state->columns[column_index].array;
}

// ---- native_stream_repeated_column ----

sanitize::Status materialize_native_repeated_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    NativeParquetPageScratch *page_scratch, NativeParquetChildArray *child,
    std::int64_t *arrow_length, std::int64_t *arrow_null_count) {
  if (!page_scratch || !child || !arrow_length || !arrow_null_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: repeated column state is null");
  }
  if (column.native_read_value_buffer_kind ==
      NativeValueBufferKind::fixed_width) {
    SAN_RETURN_NOT_OK(materialize_simple_list_fixed_width_column(
        file, column, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::bit_packed_boolean) {
    SAN_RETURN_NOT_OK(materialize_simple_list_boolean_column(
        file, column, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::byte_stream_split) {
    SAN_RETURN_NOT_OK(materialize_simple_list_byte_stream_split_column(
        file, column, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::delta_binary_packed) {
    SAN_RETURN_NOT_OK(materialize_simple_list_delta_binary_packed_column(
        file, column, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::delta_length_byte_array) {
    SAN_RETURN_NOT_OK(materialize_simple_list_delta_length_byte_array_column(
        file, column, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::plain_byte_array) {
    SAN_RETURN_NOT_OK(materialize_simple_list_plain_byte_array_column(
        file, column, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::dictionary_byte_array) {
    SAN_RETURN_NOT_OK(materialize_simple_list_dictionary_byte_array_column(
        file, column, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::dictionary_fixed_width) {
    SAN_RETURN_NOT_OK(materialize_simple_list_dictionary_fixed_width_column(
        file, column, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: unsupported list value buffer kind");
  }
  *arrow_length = list_leaf_value_count(column);
  *arrow_null_count = column.native_read_total_nulls;
  return {};
}

// ---- native_stream_flat_column ----

sanitize::Status materialize_native_flat_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *page_scratch, NativeParquetChildArray *child) {
  if (!page_scratch || !child) {
    return sanitize::Status::Invalid(
        "native Parquet reader: flat column state is null");
  }
  if (column.native_read_value_buffer_kind ==
      NativeValueBufferKind::fixed_width) {
    SAN_RETURN_NOT_OK(materialize_fixed_width_column(file, column, row_count,
                                                     page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::bit_packed_boolean) {
    SAN_RETURN_NOT_OK(materialize_boolean_column(file, column, row_count,
                                                 page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::delta_binary_packed) {
    SAN_RETURN_NOT_OK(materialize_delta_binary_packed_column(
        file, column, row_count, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::byte_stream_split) {
    SAN_RETURN_NOT_OK(materialize_byte_stream_split_column(
        file, column, row_count, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::plain_byte_array) {
    SAN_RETURN_NOT_OK(materialize_byte_array_column(file, column, row_count,
                                                    page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::delta_length_byte_array) {
    SAN_RETURN_NOT_OK(materialize_delta_length_byte_array_column(
        file, column, row_count, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::dictionary_byte_array) {
    SAN_RETURN_NOT_OK(materialize_dictionary_byte_array_column(
        file, column, row_count, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::dictionary_fixed_width) {
    SAN_RETURN_NOT_OK(materialize_dictionary_fixed_width_column(
        file, column, row_count, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: unsupported value buffer kind");
  }
  return {};
}

// ---- native_stream_column ----

sanitize::Status materialize_native_row_group_column(
    std::ifstream &file, const ColumnChunkInfo &column, std::int64_t row_count,
    NativeParquetPageScratch *page_scratch, NativeParquetChildArray *child) {
  if (!child) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output column is null");
  }
  SAN_RETURN_NOT_OK(validate_native_plain_column(column));
  std::int64_t arrow_length = row_count;
  std::int64_t arrow_null_count = column.native_read_total_nulls;
  if (is_native_repeated_path_leaf(column)) {
    SAN_RETURN_NOT_OK(materialize_native_repeated_column(
        file, column, page_scratch, child, &arrow_length, &arrow_null_count));
  } else {
    SAN_RETURN_NOT_OK(materialize_native_flat_column(file, column, row_count,
                                                     page_scratch, child));
  }
  child->array.length = arrow_length;
  child->array.null_count = arrow_null_count;
  child->array.offset = 0;
  child->array.buffers = child->buffers.data();
  child->array.n_children = 0;
  child->array.children = nullptr;
  child->array.dictionary = nullptr;
  child->array.private_data = nullptr;
  child->array.release = &native_parquet_array_child_release;
  return {};
}

// ---- native_stream_array ----

sanitize::Status build_native_row_group_array(NativeParquetStreamState *stream,
                                              ArrowArray *out) {
  if (!stream || !out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output array is null");
  }
  if (stream->row_group_index >= stream->footer.row_groups.size()) {
    sanitize::internal::cdata_stream::clear_array(out);
    return {};
  }
  const auto &row_group = stream->footer.row_groups[stream->row_group_index++];
  if (!row_group.has_num_rows || row_group.num_rows < 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: row group is missing row count");
  }
  SAN_ASSIGN_OR_RAISE(const auto estimated_buffer_bytes,
                      native_reader_row_group_buffer_bytes(row_group));
  const auto max_buffer_bytes = configured_native_reader_max_buffer_bytes();
  if (estimated_buffer_bytes > max_buffer_bytes) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: row group buffer estimate ",
        estimated_buffer_bytes, " exceeds configured limit ", max_buffer_bytes);
  }
  auto state = std::unique_ptr<NativeParquetArrayState>(
      new (std::nothrow) NativeParquetArrayState());
  if (!state) {
    return sanitize::Status::OutOfMemory("native Parquet reader array OOM");
  }
  stream->file.clear();
  if (!stream->file) {
    return sanitize::Status::IOError(
        "native Parquet reader: input stream is not readable");
  }
  state->columns.resize(row_group.columns.size());
  for (std::size_t i = 0; i < row_group.columns.size(); ++i) {
    SAN_RETURN_NOT_OK(materialize_native_row_group_column(
        stream->file, row_group.columns[i], row_group.num_rows,
        &stream->page_scratch, &state->columns[i]));
  }
  SAN_RETURN_NOT_OK(initialize_native_stream_output_layout(stream));
  state->structs.resize(stream->recursive_struct_array_count);
  state->lists.resize(stream->recursive_list_array_count);
  state->children.reserve(stream->output_layout.size());
  NativeRecursiveArrayCursor cursor;
  for (const auto &field : stream->output_layout) {
    SAN_ASSIGN_OR_RAISE(auto *field_array,
                        materialize_native_recursive_output_field(
                            state.get(), row_group, field, &cursor));
    state->children.push_back(field_array);
  }
  sanitize::internal::cdata_stream::clear_array(out);
  out->length = row_group.num_rows;
  out->null_count = 0;
  out->offset = 0;
  out->n_buffers = 1;
  out->buffers = state->struct_buffers.data();
  out->n_children = static_cast<std::int64_t>(state->children.size());
  out->children = state->children.empty() ? nullptr : state->children.data();
  out->dictionary = nullptr;
  out->private_data = state.release();
  out->release = &native_parquet_array_release;
  return {};
}
const char *native_parquet_stream_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid native Parquet reader stream";
  }
  auto *state = static_cast<NativeParquetStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

void native_parquet_stream_release(ArrowArrayStream *stream) {
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<NativeParquetStreamState *>(stream->private_data);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int native_parquet_stream_get_schema(ArrowArrayStream *stream,
                                     ArrowSchema *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<NativeParquetStreamState *>(stream->private_data);
  if (!state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, state->last_error, "native_parquet_stream.get_schema",
      [&](ArrowSchema *schema) { return build_native_schema(state, schema); });
}

int native_parquet_stream_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<NativeParquetStreamState *>(stream->private_data);
  if (!state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_array_callback(
      out, state->last_error, "native_parquet_stream.get_next",
      [&](ArrowArray *array) {
        return build_native_row_group_array(state, array);
      });
}

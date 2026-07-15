// ---- native_stream_output_field ----

sanitize::Result<ArrowArray *> materialize_native_recursive_output_field(
    NativeParquetArrayState *state, const RowGroupInfo &row_group,
    const NativeParquetOutputField &field, const NativeFlatRowWindow &window,
    NativeRecursiveArrayCursor *cursor) {
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
        window, cursor);
  }
  if (root_node.kind == NativeRecursiveMaterializationNodeKind::List) {
    return materialize_native_recursive_list_node(
        state, row_group, field.recursive_tree, field.recursive_tree.root,
        window, cursor);
  }
  if (root_node.kind == NativeRecursiveMaterializationNodeKind::Struct) {
    return materialize_native_recursive_struct_node(
        state, row_group, field.recursive_tree, field.recursive_tree.root,
        window, cursor);
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
    const NativeFlatRowWindow &window, NativeParquetPageScratch *page_scratch,
    NativeParquetChildArray *child, std::int64_t *arrow_length,
    std::int64_t *arrow_null_count) {
  if (!page_scratch || !child || !arrow_length || !arrow_null_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: repeated column state is null");
  }
  SAN_ASSIGN_OR_RAISE(const auto leaf_window,
                      native_repeated_leaf_window(column, window));
  if (column.native_read_value_buffer_kind ==
      NativeValueBufferKind::fixed_width) {
    SAN_RETURN_NOT_OK(materialize_simple_list_fixed_width_column(
        file, column, leaf_window, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::bit_packed_boolean) {
    SAN_RETURN_NOT_OK(materialize_simple_list_boolean_column(
        file, column, leaf_window, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::byte_stream_split) {
    SAN_RETURN_NOT_OK(materialize_simple_list_byte_stream_split_column(
        file, column, leaf_window, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::delta_binary_packed) {
    SAN_RETURN_NOT_OK(materialize_simple_list_delta_binary_packed_column(
        file, column, leaf_window, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::delta_length_byte_array) {
    SAN_RETURN_NOT_OK(materialize_simple_list_delta_length_byte_array_column(
        file, column, leaf_window, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::plain_byte_array) {
    SAN_RETURN_NOT_OK(materialize_simple_list_plain_byte_array_column(
        file, column, leaf_window, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::dictionary_byte_array) {
    SAN_RETURN_NOT_OK(materialize_simple_list_dictionary_byte_array_column(
        file, column, leaf_window, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else if (column.native_read_value_buffer_kind ==
             NativeValueBufferKind::dictionary_fixed_width) {
    SAN_RETURN_NOT_OK(materialize_simple_list_dictionary_fixed_width_column(
        file, column, leaf_window, page_scratch, child));
    child->buffers[0] =
        child->validity.empty() ? nullptr : child->validity.data();
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  } else {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: unsupported list value buffer kind");
  }
  *arrow_length = leaf_window.elements.count;
  *arrow_null_count = native_window_null_count(*child, *arrow_length);
  return {};
}

// ---- native_stream_flat_column ----

sanitize::Status materialize_native_flat_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeFlatRowWindow &window, NativeParquetPageScratch *page_scratch,
    NativeParquetChildArray *child) {
  if (!page_scratch || !child) {
    return sanitize::Status::Invalid(
        "native Parquet reader: flat column state is null");
  }
  const auto kind = column.native_read_value_buffer_kind;
  if (kind == NativeValueBufferKind::fixed_width) {
    SAN_RETURN_NOT_OK(materialize_fixed_width_column(file, column, window,
                                                     page_scratch, child));
  } else if (kind == NativeValueBufferKind::bit_packed_boolean) {
    SAN_RETURN_NOT_OK(materialize_boolean_column(file, column, window,
                                                 page_scratch, child));
  } else if (kind == NativeValueBufferKind::delta_binary_packed) {
    SAN_RETURN_NOT_OK(materialize_delta_binary_packed_column(
        file, column, window, page_scratch, child));
  } else if (kind == NativeValueBufferKind::byte_stream_split) {
    SAN_RETURN_NOT_OK(materialize_byte_stream_split_column(
        file, column, window, page_scratch, child));
  } else if (kind == NativeValueBufferKind::plain_byte_array) {
    SAN_RETURN_NOT_OK(materialize_byte_array_column(file, column, window,
                                                    page_scratch, child));
  } else if (kind == NativeValueBufferKind::delta_length_byte_array) {
    SAN_RETURN_NOT_OK(materialize_delta_length_byte_array_column(
        file, column, window, page_scratch, child));
  } else if (kind == NativeValueBufferKind::dictionary_byte_array) {
    SAN_RETURN_NOT_OK(materialize_dictionary_byte_array_column(
        file, column, window, page_scratch, child));
  } else if (kind == NativeValueBufferKind::dictionary_fixed_width) {
    SAN_RETURN_NOT_OK(materialize_dictionary_fixed_width_column(
        file, column, window, page_scratch, child));
  } else {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: unsupported value buffer kind");
  }
  child->buffers[0] =
      child->validity.empty() ? nullptr : child->validity.data();
  if (kind == NativeValueBufferKind::plain_byte_array ||
      kind == NativeValueBufferKind::delta_length_byte_array ||
      kind == NativeValueBufferKind::dictionary_byte_array) {
    child->buffers[1] =
        child->offsets.empty() ? nullptr : child->offsets.data();
    child->buffers[2] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 3;
  } else {
    child->buffers[1] = child->values.empty() ? nullptr : child->values.data();
    child->array.n_buffers = 2;
  }
  return {};
}

// ---- native_stream_column ----

sanitize::Status materialize_native_row_group_column(
    std::ifstream &file, const ColumnChunkInfo &column,
    const NativeFlatRowWindow &window, NativeParquetPageScratch *page_scratch,
    NativeParquetChildArray *child) {
  if (!child) {
    return sanitize::Status::Invalid(
        "native Parquet reader: output column is null");
  }
  SAN_RETURN_NOT_OK(validate_native_plain_column(column));
  std::int64_t arrow_length = window.row_count;
  std::int64_t arrow_null_count = 0;
  if (is_native_repeated_path_leaf(column)) {
    SAN_RETURN_NOT_OK(materialize_native_repeated_column(
        file, column, window, page_scratch, child, &arrow_length,
        &arrow_null_count));
  } else {
    SAN_RETURN_NOT_OK(materialize_native_flat_column(
        file, column, window, page_scratch, child));
    arrow_null_count = native_window_null_count(*child, window.row_count);
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

// ---- lazy row-group preparation ----

void release_native_column_runtime_state(ColumnChunkInfo *column) {
  if (!column) {
    return;
  }
  std::vector<PageHeaderInfo>().swap(column->pages);
  column->column_index = {};
  column->offset_index = {};
  std::vector<std::string>().swap(column->decoded_dictionary_values);
  std::vector<std::uint8_t>().swap(
      column->decoded_dictionary_fixed_width_values);
  std::vector<NativeReadPageSpanInfo>().swap(column->native_read_page_spans);
  std::vector<RepeatedLevelLayoutInfo>().swap(column->repeated_level_layouts);
  column->native_read_plan_decoded = false;
}

void release_native_row_group_runtime_state(RowGroupInfo *row_group) {
  if (!row_group) {
    return;
  }
  for (auto &column : row_group->columns) {
    release_native_column_runtime_state(&column);
  }
}

void trim_native_page_scratch(NativeParquetPageScratch *scratch) {
  if (!scratch) {
    return;
  }
  constexpr std::size_t kRetainedScratchBytes = 4U << 20;
  const auto clear_string = [](std::string *value) {
    if (!value) {
      return;
    }
    if (value->capacity() > kRetainedScratchBytes) {
      std::string().swap(*value);
    } else {
      value->clear();
    }
  };
  clear_string(&scratch->compressed_payload);
  clear_string(&scratch->decompressed_payload);
  if (scratch->dictionary_indices.capacity() * sizeof(std::uint32_t) >
      kRetainedScratchBytes) {
    std::vector<std::uint32_t>().swap(scratch->dictionary_indices);
  } else {
    scratch->dictionary_indices.clear();
  }
  if (scratch->delta_i64_values.capacity() * sizeof(std::int64_t) >
      kRetainedScratchBytes) {
    std::vector<std::int64_t>().swap(scratch->delta_i64_values);
  } else {
    scratch->delta_i64_values.clear();
  }
  if (scratch->delta_i32_lengths.capacity() * sizeof(std::int32_t) >
      kRetainedScratchBytes) {
    std::vector<std::int32_t>().swap(scratch->delta_i32_lengths);
  } else {
    scratch->delta_i32_lengths.clear();
  }
}

sanitize::Status prepare_native_row_group(NativeParquetStreamState *stream,
                                          std::size_t row_group_index) {
  if (!stream || row_group_index >= stream->footer.row_groups.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid row group index");
  }
  auto &row_group = stream->footer.row_groups[row_group_index];
  if (!row_group.columns.empty() &&
      std::all_of(row_group.columns.begin(), row_group.columns.end(),
                  [](const ColumnChunkInfo &column) {
                    return column.native_read_plan_decoded;
                  })) {
    return {};
  }

  // Reuse the existing bounded verification pipeline on exactly one row group.
  // Moving metadata into a one-element footer avoids copying decoded page
  // vectors and prevents all row groups from being resident simultaneously.
  FooterInfo current;
  current.row_groups.push_back(std::move(row_group));
  auto status = read_page_headers(stream->file, &current);
  if (status.ok()) {
    status = read_page_indexes(stream->file, &current);
  }
  if (status.ok()) {
    status = assign_native_read_page_spans(&current);
  }
  row_group = std::move(current.row_groups.front());
  if (!status.ok()) {
    release_native_row_group_runtime_state(&row_group);
    return status;
  }
  for (const auto &column : row_group.columns) {
    status = validate_native_plain_column(column);
    if (!status.ok()) {
      release_native_row_group_runtime_state(&row_group);
      return status;
    }
  }
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
  auto &row_group = stream->footer.row_groups[stream->row_group_index];
  SAN_RETURN_NOT_OK(prepare_native_row_group(stream, stream->row_group_index));
  SAN_RETURN_NOT_OK(initialize_native_stream_output_layout(stream));
  const auto layout_status = validate_native_recursive_row_group_output_layout(
      row_group, stream->output_layout);
  if (!layout_status.ok()) {
    release_native_row_group_runtime_state(&row_group);
    trim_native_page_scratch(&stream->page_scratch);
    return layout_status;
  }
  SAN_ASSIGN_OR_RAISE(
      const auto window,
      next_native_flat_row_window(
          row_group, stream->output_layout, stream->row_group_row_offset,
          stream->max_buffer_bytes, stream->max_batch_rows));
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
        stream->file, row_group.columns[i], window,
        &stream->page_scratch, &state->columns[i]));
    SAN_RETURN_NOT_OK(enforce_native_array_retained_budget(*state, stream->max_buffer_bytes));
  }
  state->structs.resize(stream->recursive_struct_array_count);
  state->lists.resize(stream->recursive_list_array_count);
  state->children.reserve(stream->output_layout.size());
  NativeRecursiveArrayCursor cursor;
  for (const auto &field : stream->output_layout) {
    SAN_ASSIGN_OR_RAISE(auto *field_array,
                        materialize_native_recursive_output_field(
                            state.get(), row_group, field, window, &cursor));
    state->children.push_back(field_array);
    SAN_RETURN_NOT_OK(enforce_native_array_retained_budget(*state, stream->max_buffer_bytes));
  }
  sanitize::internal::cdata_stream::clear_array(out);
  out->length = window.row_count;
  out->null_count = 0;
  out->offset = 0;
  out->n_buffers = 1;
  out->buffers = state->struct_buffers.data();
  out->n_children = static_cast<std::int64_t>(state->children.size());
  out->children = state->children.empty() ? nullptr : state->children.data();
  out->dictionary = nullptr;
  out->private_data = state.release();
  out->release = &native_parquet_array_release;
  stream->row_group_row_offset += window.row_count;
  if (stream->row_group_row_offset >= row_group.num_rows) {
    release_native_row_group_runtime_state(&row_group);
    trim_native_page_scratch(&stream->page_scratch);
    stream->row_group_row_offset = 0;
    ++stream->row_group_index;
  }
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

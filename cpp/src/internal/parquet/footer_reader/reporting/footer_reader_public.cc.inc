
sanitize::Status project_footer_row_group_columns(
    FooterInfo *info, const std::vector<std::string> &projected_columns,
    const std::vector<LeafLevelInfo> &leaves);

sanitize::Result<FooterInfo>
read_footer_metadata_impl(const std::string &path,
                          const std::vector<std::string> &projected_columns,
                          std::int64_t memory_limit_bytes = -1) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    return sanitize::Status::IOError("Parquet footer: failed opening input");
  }
  file.seekg(0, std::ios::end);
  const auto end_pos = file.tellg();
  if (end_pos < static_cast<std::streampos>(8)) {
    return sanitize::Status::Invalid("Parquet footer: file is too small");
  }
  const auto file_size = static_cast<std::uint64_t>(end_pos);
  std::array<char, 8> trailer{};
  file.seekg(-8, std::ios::end);
  file.read(trailer.data(), static_cast<std::streamsize>(trailer.size()));
  if (!file) {
    return sanitize::Status::IOError("Parquet footer: failed reading trailer");
  }
  if (std::string_view(trailer.data() + 4, 4) != kParquetMagic) {
    return sanitize::Status::Invalid("Parquet footer: invalid trailing magic");
  }
  const auto footer_len =
      static_cast<std::uint64_t>(read_u32_le(trailer.data()));
  const auto budget =
      sanitize::internal::memory_budget_from_limit(memory_limit_bytes);
  const auto footer_budget = static_cast<std::uint64_t>(
      std::max<std::int64_t>(1, budget.parquet_footer_bytes));
  const auto effective_footer_limit = std::min<std::uint64_t>(
      static_cast<std::uint64_t>(kMaxFooterBytes), footer_budget);
  if (footer_len >= effective_footer_limit) {
    return sanitize::Status::Invalid(
        "Parquet footer: footer length ", footer_len,
        " leaves no metadata capacity within effective limit ",
        effective_footer_limit);
  }
  if (footer_len == 0 || footer_len > file_size - 8 ||
      file_size - 8 - footer_len < 4) {
    return sanitize::Status::Invalid("Parquet footer: invalid footer length");
  }
  std::string footer(static_cast<std::size_t>(footer_len), '\0');
  file.seekg(static_cast<std::streamoff>(file_size - 8 - footer_len),
             std::ios::beg);
  file.read(footer.data(), static_cast<std::streamsize>(footer.size()));
  if (!file) {
    return sanitize::Status::IOError("Parquet footer: failed reading footer");
  }
  FooterInfo info;
  const auto materialized_limit =
      static_cast<std::size_t>(effective_footer_limit - footer_len);
  SAN_ASSIGN_OR_RAISE(info, parse_footer(footer, materialized_limit));
  std::vector<LeafLevelInfo> leaves;
  SAN_ASSIGN_OR_RAISE(leaves, assign_column_levels(&info));
  SAN_RETURN_NOT_OK(
      project_footer_row_group_columns(&info, projected_columns, leaves));
  return info;
}

sanitize::Result<FooterInfo>
read_footer_info_impl(const std::string &path,
                      const std::vector<std::string> &projected_columns) {
  SAN_ASSIGN_OR_RAISE(auto info,
                      read_footer_metadata_impl(path, projected_columns));
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    return sanitize::Status::IOError("Parquet footer: failed opening input");
  }
  SAN_RETURN_NOT_OK(read_page_headers(file, &info));
  SAN_RETURN_NOT_OK(read_page_indexes(file, &info));
  SAN_RETURN_NOT_OK(assign_native_read_page_spans(&info));
  return info;
}

sanitize::Result<FooterInfo> read_footer_info(const std::string &path) {
  return read_footer_info_impl(path, {});
}

sanitize::Result<std::string>
read_footer_info_json(const std::string &path,
                      const std::vector<std::string> &projected_columns) {
  FooterInfo info;
  SAN_ASSIGN_OR_RAISE(info, read_footer_info_impl(path, projected_columns));
  std::string out;
  out.push_back('{');
  bool first = true;
  json_encoding::append_int_field(out, first, "version", info.version);
  json_encoding::append_int_field(out, first, "num_rows", info.num_rows);
  json_encoding::append_int_field(out, first, "schema_element_count",
                                  info.schema_element_count);
  json_encoding::append_int_field(out, first, "row_group_count",
                                  info.row_group_count);
  json_encoding::append_string_field(out, first, "created_by", info.created_by);
  const auto readiness = native_reader_readiness(info);
  json_encoding::append_int_field(out, first, "native_reader_ready",
                                  readiness.ready ? 1 : 0);
  json_encoding::append_key(out, first, "native_reader_blockers");
  append_string_array(out, readiness.blockers);
  append_footer_schema_elements_json(out, first, info);
  append_footer_row_groups_json(out, first, info);
  out.push_back('}');
  return out;
}

sanitize::Status project_footer_row_group_columns(
    FooterInfo *info, const std::vector<std::string> &projected_columns,
    const std::vector<LeafLevelInfo> &leaves) {
  if (!info || projected_columns.empty()) {
    return {};
  }

  std::vector<std::size_t> selected_indices;
  SAN_RETURN_NOT_OK(projected_top_level_leaf_indices(leaves, projected_columns,
                                                     &selected_indices));
  std::vector<std::uint8_t> selected_once(leaves.size(), 0);
  bool duplicate_selection = false;
  for (const auto index : selected_indices) {
    if (selected_once[index] != 0) {
      duplicate_selection = true;
      break;
    }
    selected_once[index] = 1;
  }

  info->projected_columns = projected_columns;
  for (auto &row_group : info->row_groups) {
    if (row_group.columns.size() != leaves.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: row group column count mismatch");
    }
    std::vector<ColumnChunkInfo> selected;
    selected.reserve(selected_indices.size());
    for (const auto index : selected_indices) {
      if (duplicate_selection) {
        selected.push_back(row_group.columns[index]);
      } else {
        selected.push_back(std::move(row_group.columns[index]));
      }
    }
    row_group.columns = std::move(selected);
  }
  return {};
}

sanitize::Result<ArrowArrayStream *>
make_arrow_stream(const std::string &path,
                  const std::vector<std::string> &projected_columns,
                  std::int64_t memory_limit_bytes) {
  FooterInfo info;
  SAN_ASSIGN_OR_RAISE(info, read_footer_metadata_impl(path, projected_columns,
                                                      memory_limit_bytes));
  if (info.created_by != "schema-sanitizer native parquet writer") {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: file was not written by schema-sanitizer "
        "native parquet writer");
  }
  if (info.schema_elements.empty()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: missing schema elements");
  }
  if (info.row_groups.empty() && info.num_rows > 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: non-empty file has no row groups");
  }
  auto state = std::unique_ptr<NativeParquetStreamState>(
      new (std::nothrow) NativeParquetStreamState());
  if (!state) {
    return sanitize::Status::OutOfMemory("native Parquet reader stream OOM");
  }
  state->path = path;
  state->max_buffer_bytes =
      configured_native_reader_max_buffer_bytes(memory_limit_bytes);
  state->max_batch_rows =
      configured_native_reader_max_batch_rows(memory_limit_bytes);
  const auto operation_budget =
      sanitize::internal::memory_budget_from_limit(memory_limit_bytes);
  state->max_page_bytes = operation_budget.parquet_page_bytes;
  state->max_row_group_bytes = operation_budget.parquet_row_group_bytes;
  state->page_scratch.max_page_bytes = static_cast<std::size_t>(
      std::max<std::int64_t>(1, state->max_page_bytes));
  state->file.open(path, std::ios::binary);
  if (!state->file) {
    return sanitize::Status::IOError(
        "native Parquet reader: failed opening input");
  }
  state->footer = std::move(info);
  // Output layout depends on schema/path metadata, not decoded page payloads.
  // Build it now while page headers, dictionaries and levels remain unloaded.
  SAN_RETURN_NOT_OK(initialize_native_stream_output_layout(state.get()));

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    return sanitize::Status::OutOfMemory("native Parquet reader stream OOM");
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &native_parquet_stream_get_schema;
  stream->get_next = &native_parquet_stream_get_next;
  stream->get_last_error = &native_parquet_stream_last_error;
  stream->release = &native_parquet_stream_release;
  stream->private_data = state.release();
  return stream;
}

NativeReadinessInfo
bounded_stream_preflight_readiness(NativeParquetStreamState *state) {
  NativeReadinessInfo readiness;
  if (!state) {
    add_readiness_blocker(&readiness, "stream state is null");
    return readiness;
  }
  auto &info = state->footer;
  if (info.created_by != "schema-sanitizer native parquet writer") {
    add_readiness_blocker(
        &readiness,
        "file was not written by schema-sanitizer native parquet writer");
  }
  if (info.schema_elements.empty()) {
    add_readiness_blocker(&readiness, "missing schema elements");
  }
  if (info.row_groups.empty()) {
    const auto empty_readiness =
        native_reader_readiness(info, nullptr, state->max_buffer_bytes);
    for (const auto &blocker : empty_readiness.blockers) {
      add_readiness_blocker(&readiness, blocker);
    }
    return readiness;
  }

  const auto layout_status = initialize_native_stream_output_layout(state);
  if (!layout_status.ok()) {
    add_readiness_blocker(&readiness, "output layout is not materializable: " +
                                          layout_status.message());
    return readiness;
  }

  const auto max_buffer_bytes = state->max_buffer_bytes;
  for (std::size_t index = 0; index < info.row_groups.size(); ++index) {
    auto &row_group = info.row_groups[index];
    const auto label = "row group " + std::to_string(index) + ": ";
    if (!row_group.has_num_rows || row_group.num_rows < 0) {
      add_readiness_blocker(&readiness, label + "missing row count");
      continue;
    }
    if (row_group.columns.empty() && row_group.num_rows > 0) {
      add_readiness_blocker(&readiness, label + "has rows but no columns");
      continue;
    }

    const auto prepare_status = prepare_native_row_group(state, index);
    if (!prepare_status.ok()) {
      for (const auto &column : row_group.columns) {
        append_column_readiness_blockers(column, &readiness);
      }
      add_readiness_blocker(&readiness, label + prepare_status.message());
      continue;
    }
    for (const auto &column : row_group.columns) {
      append_column_readiness_blockers(column, &readiness);
    }
    auto estimated = native_reader_row_group_buffer_bytes(row_group);
    if (!estimated.ok()) {
      add_readiness_blocker(&readiness, label +
                                            "native buffer estimate failed: " +
                                            estimated.status().message());
    } else if (*estimated > max_buffer_bytes) {
      add_readiness_blocker(&readiness, label + "native buffer estimate " +
                                            std::to_string(*estimated) +
                                            " exceeds configured limit " +
                                            std::to_string(max_buffer_bytes));
    }
    const auto row_layout_status =
        validate_native_recursive_row_group_output_layout(row_group,
                                                          state->output_layout);
    if (!row_layout_status.ok()) {
      add_readiness_blocker(
          &readiness, label +
                          "recursive output layout is not materializable: " +
                          row_layout_status.message());
    }
    release_native_row_group_runtime_state(&row_group);
    trim_native_page_scratch(&state->page_scratch);
  }
  return readiness;
}

void append_stream_preflight_row_groups_json(std::string &out, bool &first,
                                             const FooterInfo &info) {
  json_encoding::append_key(out, first, "row_groups");
  out.push_back('[');
  for (std::size_t index = 0; index < info.row_groups.size(); ++index) {
    if (index > 0) {
      out.push_back(',');
    }
    const auto &row_group = info.row_groups[index];
    out.push_back('{');
    bool row_first = true;
    if (row_group.has_num_rows) {
      json_encoding::append_int_field(out, row_first, "num_rows",
                                      row_group.num_rows);
    }
    if (row_group.has_total_byte_size) {
      json_encoding::append_int_field(out, row_first, "total_byte_size",
                                      row_group.total_byte_size);
    }
    out.push_back('}');
  }
  out.push_back(']');
}

sanitize::Result<std::string>
read_stream_preflight_json(const std::string &path,
                           const std::vector<std::string> &projected_columns,
                           std::int64_t memory_limit_bytes) {
  SAN_ASSIGN_OR_RAISE(
      auto info,
      read_footer_metadata_impl(path, projected_columns, memory_limit_bytes));
  NativeParquetStreamState state;
  state.path = path;
  state.max_buffer_bytes =
      configured_native_reader_max_buffer_bytes(memory_limit_bytes);
  state.max_batch_rows =
      configured_native_reader_max_batch_rows(memory_limit_bytes);
  const auto operation_budget =
      sanitize::internal::memory_budget_from_limit(memory_limit_bytes);
  state.max_page_bytes = operation_budget.parquet_page_bytes;
  state.max_row_group_bytes = operation_budget.parquet_row_group_bytes;
  state.page_scratch.max_page_bytes =
      static_cast<std::size_t>(std::max<std::int64_t>(1, state.max_page_bytes));
  state.file.open(path, std::ios::binary);
  if (!state.file) {
    return sanitize::Status::IOError(
        "native Parquet reader: failed opening input");
  }
  state.footer = std::move(info);
  const auto readiness = bounded_stream_preflight_readiness(&state);

  std::string out;
  out.push_back('{');
  bool first = true;
  json_encoding::append_int_field(out, first, "bounded_preflight", 1);
  json_encoding::append_int_field(out, first, "version", state.footer.version);
  json_encoding::append_int_field(out, first, "num_rows",
                                  state.footer.num_rows);
  json_encoding::append_int_field(out, first, "schema_element_count",
                                  state.footer.schema_element_count);
  json_encoding::append_int_field(out, first, "row_group_count",
                                  state.footer.row_group_count);
  json_encoding::append_string_field(out, first, "created_by",
                                     state.footer.created_by);
  json_encoding::append_int_field(out, first, "native_reader_ready",
                                  readiness.ready ? 1 : 0);
  json_encoding::append_key(out, first, "native_reader_blockers");
  append_string_array(out, readiness.blockers);
  append_stream_preflight_row_groups_json(out, first, state.footer);
  out.push_back('}');
  return out;
}

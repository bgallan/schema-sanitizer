
sanitize::Status project_footer_row_group_columns(
    FooterInfo *info, const std::vector<std::string> &projected_columns,
    const std::vector<LeafLevelInfo> &leaves);

sanitize::Result<FooterInfo>
read_footer_info_impl(const std::string &path,
                      const std::vector<std::string> &projected_columns) {
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
  SAN_ASSIGN_OR_RAISE(info, parse_footer(footer));
  std::vector<LeafLevelInfo> leaves;
  SAN_ASSIGN_OR_RAISE(leaves, assign_column_levels(&info));
  SAN_RETURN_NOT_OK(
      project_footer_row_group_columns(&info, projected_columns, leaves));
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
                  const std::vector<std::string> &projected_columns) {
  FooterInfo info;
  SAN_ASSIGN_OR_RAISE(info, read_footer_info_impl(path, projected_columns));
  std::vector<NativeParquetOutputField> planned_output_layout;
  const auto readiness = native_reader_readiness(info, &planned_output_layout);
  if (!readiness.ready) {
    std::string message = "native Parquet reader: file is not ready";
    if (!readiness.blockers.empty()) {
      message += ": ";
      message += readiness.blockers.front();
    }
    return sanitize::Status::NotImplemented(message);
  }
  for (const auto &row_group : info.row_groups) {
    for (const auto &column : row_group.columns) {
      SAN_RETURN_NOT_OK(validate_native_plain_column(column));
    }
  }
  auto state = std::unique_ptr<NativeParquetStreamState>(
      new (std::nothrow) NativeParquetStreamState());
  if (!state) {
    return sanitize::Status::OutOfMemory("native Parquet reader stream OOM");
  }
  state->path = path;
  state->file.open(path, std::ios::binary);
  if (!state->file) {
    return sanitize::Status::IOError(
        "native Parquet reader: failed opening input");
  }
  state->footer = std::move(info);
  if (!state->footer.row_groups.empty()) {
    state->output_layout = std::move(planned_output_layout);
    SAN_RETURN_NOT_OK(finalize_native_stream_output_layout_plan(state.get()));
  } else {
    SAN_RETURN_NOT_OK(initialize_native_stream_output_layout(state.get()));
  }

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

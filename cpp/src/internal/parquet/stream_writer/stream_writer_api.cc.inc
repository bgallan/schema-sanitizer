namespace {
// Returns the Arrow stream error string when available.
std::string stream_error_message(ArrowArrayStream *stream,
                                 std::string_view fallback) {
  if (stream && stream->get_last_error) {
    if (const char *message = stream->get_last_error(stream)) {
      if (*message) {
        return std::string(message);
      }
    }
  }
  return std::string(fallback);
}
} // namespace

sanitize::Status write_stream(ArrowArrayStream *stream, Output &out_file) {
  if (!stream) {
    return sanitize::Status::Invalid(
        "native Parquet writer: Arrow C stream is null");
  }

  sanitize::CSchemaGuard schema;
  const int schema_rc = stream->get_schema(stream, schema.get());
  if (schema_rc != 0) {
    return sanitize::Status::IOError(stream_error_message(
        stream, "native Parquet writer: get_schema failed"));
  }
  SAN_ASSIGN_OR_RAISE(auto root, parse_supported_root_schema(schema.value()));
  auto columns = analyze_schema(&root);

  CountingOutput out(out_file);
  SAN_RETURN_NOT_OK(out.Write(kMagic));

  std::vector<RowGroupInfo> row_groups;
  std::int64_t total_rows = 0;
  const auto max_rows_per_row_group = configured_max_rows_per_row_group();
  const auto max_bytes_per_row_group = adaptive_max_bytes_per_row_group(
      columns, configured_max_bytes_per_row_group());
  const auto target_page_bytes = configured_target_page_bytes();
  SAN_ASSIGN_OR_RAISE(const auto compression_codec,
                      configured_compression_codec());
  while (true) {
    sanitize::CArrayGuard batch;
    const int next_rc = stream->get_next(stream, batch.get());
    if (next_rc != 0) {
      return sanitize::Status::IOError(stream_error_message(
          stream, "native Parquet writer: get_next failed"));
    }
    if (!batch.value().release) {
      break;
    }
    SAN_RETURN_NOT_OK(validate_batch(root, batch.value()));
    if (batch.value().length == 0) {
      continue;
    }
    for (std::int64_t row_offset = 0; row_offset < batch.value().length;) {
      auto row_count =
          std::min(max_rows_per_row_group, batch.value().length - row_offset);
      SAN_ASSIGN_OR_RAISE(auto page_data,
                          collect_batch_pages(root, columns, batch.value(),
                                              row_offset, row_count));
      while (row_count > 1 &&
             static_cast<std::int64_t>(estimate_row_group_page_data_bytes(
                 page_data)) > max_bytes_per_row_group) {
        row_count = std::max<std::int64_t>(1, row_count / 2);
        SAN_ASSIGN_OR_RAISE(page_data,
                            collect_batch_pages(root, columns, batch.value(),
                                                row_offset, row_count));
      }
      RowGroupInfo row_group;
      row_group.num_rows = row_count;
      row_group.columns.reserve(columns.size());
      for (std::size_t i = 0; i < columns.size(); ++i) {
        SAN_ASSIGN_OR_RAISE(auto page,
                            write_column_pages(out, columns[i], page_data[i],
                                               compression_codec,
                                               target_page_bytes));
        row_group.columns.push_back(page);
      }
      total_rows += row_count;
      row_groups.push_back(std::move(row_group));
      row_offset += row_count;
    }
  }

  SAN_RETURN_NOT_OK(write_page_indexes(out, columns, &row_groups));
  std::string footer =
      encode_footer(root, columns, row_groups, total_rows, compression_codec);
  SAN_RETURN_NOT_OK(out.Write(footer));
  if (footer.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: footer is too large");
  }
  std::string trailer;
  append_u32_le(trailer, static_cast<std::uint32_t>(footer.size()));
  trailer.append(kMagic);
  SAN_RETURN_NOT_OK(out.Write(trailer));
  return out.Flush();
}

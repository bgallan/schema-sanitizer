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

std::size_t retained_optional_string_bytes(
    const std::optional<std::string> &value) noexcept {
  return value ? value->capacity() : 0;
}

std::size_t retained_page_metadata_bytes(const PageInfo &page) noexcept {
  auto total = retained_optional_string_bytes(page.min_value);
  return saturating_size_add(
      total, retained_optional_string_bytes(page.max_value));
}

std::size_t retained_row_group_metadata_bytes(
    const RowGroupInfo &row_group) noexcept {
  auto total = saturating_size_add(
      sizeof(RowGroupInfo),
      saturating_size_multiply(row_group.columns.capacity(),
                               sizeof(ColumnChunkInfo)));
  for (const auto &column : row_group.columns) {
    total = saturating_size_add(
        total, retained_page_metadata_bytes(column.aggregate));
    total = saturating_size_add(
        total, saturating_size_multiply(column.pages.capacity(),
                                        sizeof(PageInfo)));
    for (const auto &page : column.pages) {
      total = saturating_size_add(total,
                                  retained_page_metadata_bytes(page));
    }
  }
  return total;
}

bool metadata_budget_would_be_exceeded(std::size_t retained,
                                       std::size_t additional,
                                       std::size_t limit) noexcept {
  return additional > limit || retained > limit - additional;
}
} // namespace

sanitize::Status write_stream(ArrowArrayStream *stream, Output &out_file,
                              const WriterOptions &raw_options) {
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

  const auto options = normalized_writer_options(raw_options);
  const auto budget =
      sanitize::internal::memory_budget_from_limit(options.memory_limit_bytes);
  std::vector<RowGroupInfo> row_groups;
  std::int64_t total_rows = 0;
  const auto max_rows_per_row_group = budget.parquet_row_group_rows;
  const auto max_row_groups = std::min<std::int64_t>(
      kHardMaxRowGroups,
      std::max<std::int64_t>(1, budget.total_bytes / (64 * 1024)));
  const auto max_footer_metadata_bytes =
      static_cast<std::size_t>(budget.parquet_footer_bytes);
  std::size_t retained_footer_metadata_bytes = 0;
  const auto max_bytes_per_row_group = adaptive_max_bytes_per_row_group(
      columns, budget.parquet_row_group_bytes);
  const auto target_page_bytes = budget.parquet_page_bytes;
  SAN_ASSIGN_OR_RAISE(const auto compression_codec,
                      compression_codec_from_options(options));
  SAN_ASSIGN_OR_RAISE(const auto gzip_level,
                      gzip_level_from_options(options));
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
      if (row_groups.size() >= static_cast<std::size_t>(max_row_groups)) {
        return sanitize::Status::Invalid(
            "native Parquet writer: row-group count exceeds safety limit");
      }
      SAN_ASSIGN_OR_RAISE(
          const auto row_count,
          determine_bounded_row_group_count(
              root, columns, batch.value(), row_offset,
              max_rows_per_row_group, max_bytes_per_row_group));
      if (row_count <= 0) {
        return sanitize::Status::Invalid(
            "native Parquet writer: bounded row-group collector made no progress");
      }
      if (total_rows > std::numeric_limits<std::int64_t>::max() - row_count) {
        return sanitize::Status::Invalid(
            "native Parquet writer: total row count exceeds int64 range");
      }
      const auto fixed_row_group_metadata_bytes = saturating_size_add(
          sizeof(RowGroupInfo),
          saturating_size_multiply(columns.size(), sizeof(ColumnChunkInfo)));
      if (metadata_budget_would_be_exceeded(
              retained_footer_metadata_bytes, fixed_row_group_metadata_bytes,
              max_footer_metadata_bytes)) {
        return sanitize::Status::Invalid(
            "native Parquet writer: footer metadata exceeds safety limit");
      }
      RowGroupInfo row_group;
      row_group.num_rows = row_count;
      row_group.columns.reserve(columns.size());
      for (std::size_t i = 0; i < columns.size(); ++i) {
        SAN_ASSIGN_OR_RAISE(
            auto page_data,
            collect_column_page_data(root, columns[i], batch.value(),
                                     row_offset, row_count, i));
        SAN_ASSIGN_OR_RAISE(auto page,
                            write_column_pages(out, columns[i], page_data,
                                               compression_codec, gzip_level,
                                               target_page_bytes));
        row_group.columns.push_back(std::move(page));
      }
      // Page indexes belong immediately after the row group's column chunks.
      // Serialize them now and discard per-page metadata before retaining the
      // compact footer entry for the rest of the file.
      SAN_RETURN_NOT_OK(
          write_row_group_page_indexes(out, columns, &row_group));
      const auto row_group_metadata_bytes =
          retained_row_group_metadata_bytes(row_group);
      if (metadata_budget_would_be_exceeded(
              retained_footer_metadata_bytes, row_group_metadata_bytes,
              max_footer_metadata_bytes)) {
        return sanitize::Status::Invalid(
            "native Parquet writer: footer metadata exceeds safety limit");
      }
      retained_footer_metadata_bytes += row_group_metadata_bytes;
      total_rows += row_count;
      row_groups.push_back(std::move(row_group));
      row_offset += row_count;
    }
  }

  std::string footer =
      encode_footer(root, columns, row_groups, total_rows, compression_codec);
  if (footer.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: footer is too large");
  }
  SAN_RETURN_NOT_OK(out.Write(footer));
  std::string trailer;
  append_u32_le(trailer, static_cast<std::uint32_t>(footer.size()));
  trailer.append(kMagic);
  SAN_RETURN_NOT_OK(out.Write(trailer));
  return out.Flush();
}

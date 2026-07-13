std::optional<std::string> env_value(const char *name) {
  if (!name || *name == '\0') {
    return std::nullopt;
  }
#if defined(_MSC_VER)
  char *raw = nullptr;
  std::size_t size = 0;
  if (_dupenv_s(&raw, &size, name) != 0 || !raw) {
    return std::nullopt;
  }
  std::string value(raw, size > 0 ? size - 1 : 0);
  std::free(raw);
  return value;
#else
  const char *raw = std::getenv(name);
  if (!raw) {
    return std::nullopt;
  }
  return std::string(raw);
#endif
}

std::int64_t configured_positive_i64_env(const char *name,
                                         std::int64_t default_value) {
  const auto raw = env_value(name);
  if (!raw || raw->empty()) {
    return default_value;
  }
  char *end = nullptr;
  const char *begin = raw->c_str();
  const auto parsed = std::strtoll(begin, &end, 10);
  if (end == begin || (end && *end != '\0') || parsed <= 0) {
    return default_value;
  }
  return parsed;
}

std::int64_t configured_native_reader_max_buffer_bytes() {
  return configured_positive_i64_env(
      "SCHEMA_SANITIZER_NATIVE_PARQUET_READER_MAX_BUFFER_BYTES",
      kDefaultNativeReaderMaxBufferBytes);
}

sanitize::Status add_i64_checked(std::int64_t *target, std::int64_t value,
                                 std::string_view what) {
  if (!target || value < 0 ||
      *target > std::numeric_limits<std::int64_t>::max() - value) {
    return sanitize::Status::Invalid("Parquet native read plan: ", what,
                                     " exceeds int64");
  }
  *target += value;
  return {};
}

sanitize::Result<std::int64_t>
native_reader_column_buffer_bytes(const ColumnChunkInfo &column,
                                  std::int64_t row_count) {
  if (row_count < 0) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: negative row count");
  }
  std::int64_t total = 0;
  for (const auto &layout : column.repeated_level_layouts) {
    if (!layout.decoded) {
      continue;
    }
    SAN_RETURN_NOT_OK(add_i64_checked(&total, (layout.row_count + 1) * 4,
                                      "native repeated offset buffer bytes"));
    if (layout.null_count > 0) {
      SAN_RETURN_NOT_OK(
          add_i64_checked(&total, (layout.row_count + 7) / 8,
                          "native repeated validity buffer bytes"));
    }
  }
  std::int64_t repeated_value_row_count = row_count;
  if (!column.repeated_level_layouts.empty() &&
      column.repeated_level_layouts.back().decoded) {
    repeated_value_row_count =
        column.repeated_level_layouts.back().element_count;
  }
  const auto value_row_count = column.native_read_plan_decoded
                                   ? column.native_read_arrow_length
                                   : repeated_value_row_count;
  if (value_row_count < 0) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: negative value row count");
  }
  if (column.native_read_total_nulls > 0) {
    SAN_RETURN_NOT_OK(add_i64_checked(&total, (value_row_count + 7) / 8,
                                      "native validity buffer bytes"));
  }
  SAN_RETURN_NOT_OK(
      add_i64_checked(&total, column.native_read_materialized_offset_bytes,
                      "native offset buffer bytes"));
  SAN_RETURN_NOT_OK(add_i64_checked(&total,
                                    column.native_read_materialized_value_bytes,
                                    "native value buffer bytes"));
  return total;
}

sanitize::Result<std::int64_t>
native_reader_row_group_buffer_bytes(const RowGroupInfo &row_group) {
  if (!row_group.has_num_rows) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: row group is missing row count");
  }
  std::int64_t total = 0;
  for (const auto &column : row_group.columns) {
    if (!column.native_read_plan_decoded) {
      continue;
    }
    SAN_ASSIGN_OR_RAISE(
        const auto column_bytes,
        native_reader_column_buffer_bytes(column, row_group.num_rows));
    SAN_RETURN_NOT_OK(
        add_i64_checked(&total, column_bytes, "native row group buffer bytes"));
  }
  return total;
}

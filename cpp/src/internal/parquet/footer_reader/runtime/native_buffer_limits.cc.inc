// Estimates retained column, row-group, and flat-window buffers and enforces
// the native reader cap. This fragment is included by the consolidated footer
// reader inside its private implementation namespace.

/// Returns the configured native-reader buffer cap after applying its hard
/// bounds.
std::int64_t
configured_native_reader_max_buffer_bytes(std::int64_t memory_limit_bytes) {
  return sanitize::internal::memory_budget_from_limit(memory_limit_bytes)
      .parquet_reader_buffer_bytes;
}

/// Returns the configured native-reader batch row cap after applying its hard
/// bounds.
std::int64_t
configured_native_reader_max_batch_rows(std::int64_t memory_limit_bytes) {
  return sanitize::internal::memory_budget_from_limit(memory_limit_bytes)
      .parquet_reader_rows;
}

/// Adds 64-bit integer checked with checked offsets, counts, and
/// retained-capacity accounting.
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

/// Totals the Arrow offsets, validity, and value bytes retained for one decoded
/// column.
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

/// Totals retained Arrow buffer bytes across decoded columns in one row group.
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

/// Scales full-column value bytes to a row window using overflow-safe
/// arithmetic.
sanitize::Result<std::int64_t>
scale_window_value_bytes(std::int64_t full_bytes, std::int64_t selected_values,
                         std::int64_t total_values) {
  if (full_bytes < 0 || selected_values < 0 || total_values < 0 ||
      selected_values > total_values) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: invalid value-window estimate");
  }
  if (full_bytes == 0 || selected_values == 0 || total_values == 0) {
    return std::int64_t{0};
  }
  const auto scaled = static_cast<long double>(full_bytes) *
                      static_cast<long double>(selected_values) /
                      static_cast<long double>(total_values);
  if (scaled >
      static_cast<long double>(std::numeric_limits<std::int64_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: value-window estimate overflow");
  }
  auto estimated = static_cast<std::int64_t>(scaled);
  if (static_cast<long double>(estimated) < scaled) {
    ++estimated;
  }
  return estimated;
}

/// Estimates retained buffers for one column restricted to a flat row window.
sanitize::Result<std::int64_t> native_reader_flat_column_window_buffer_bytes(
    const ColumnChunkInfo &column, std::int64_t row_group_rows,
    std::int64_t first_row, std::int64_t row_count) {
  if (row_group_rows < 0 || first_row < 0 || row_count < 0 ||
      first_row > row_group_rows || row_count > row_group_rows - first_row) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: invalid column row window");
  }
  if (!column.native_read_plan_decoded) {
    return std::int64_t{0};
  }

  std::int64_t total = 0;
  auto value_first = first_row;
  auto value_count = row_count;
  for (const auto &layout : column.repeated_level_layouts) {
    if (!layout.decoded || value_first < 0 || value_count < 0 ||
        value_first > layout.row_count ||
        value_count > layout.row_count - value_first ||
        layout.offsets.size() !=
            static_cast<std::size_t>(layout.row_count + 1)) {
      return sanitize::Status::Invalid(
          "Parquet native read plan: invalid repeated row window");
    }
    SAN_RETURN_NOT_OK(
        add_i64_checked(&total, (value_count + 1) * 4,
                        "row-window repeated offset buffer bytes"));
    if (layout.null_count > 0) {
      SAN_RETURN_NOT_OK(
          add_i64_checked(&total, (value_count + 7) / 8,
                          "row-window repeated validity buffer bytes"));
    }
    const auto first = layout.offsets[static_cast<std::size_t>(value_first)];
    const auto last =
        layout.offsets[static_cast<std::size_t>(value_first + value_count)];
    if (first < 0 || last < first || last > layout.element_count) {
      return sanitize::Status::Invalid(
          "Parquet native read plan: invalid repeated window offsets");
    }
    value_first = first;
    value_count = static_cast<std::int64_t>(last) - first;
  }

  const auto total_values = column.native_read_arrow_length;
  if (total_values < 0 || value_first < 0 || value_count < 0 ||
      value_first > total_values || value_count > total_values - value_first) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: invalid leaf value window");
  }
  if (column.native_read_total_nulls > 0) {
    SAN_RETURN_NOT_OK(add_i64_checked(&total, (value_count + 7) / 8,
                                      "row-window validity bytes"));
  }
  switch (column.native_read_value_buffer_kind) {
  case NativeValueBufferKind::bit_packed_boolean:
    SAN_RETURN_NOT_OK(add_i64_checked(&total, (value_count + 7) / 8,
                                      "row-window boolean bytes"));
    break;
  case NativeValueBufferKind::plain_byte_array:
  case NativeValueBufferKind::dictionary_byte_array:
  case NativeValueBufferKind::delta_length_byte_array: {
    SAN_RETURN_NOT_OK(add_i64_checked(&total, (value_count + 1) * 4,
                                      "row-window offset bytes"));
    SAN_ASSIGN_OR_RAISE(
        const auto estimated,
        scale_window_value_bytes(column.native_read_materialized_value_bytes,
                                 value_count, total_values));
    SAN_RETURN_NOT_OK(
        add_i64_checked(&total, estimated, "row-window value bytes"));
    break;
  }
  case NativeValueBufferKind::fixed_width:
  case NativeValueBufferKind::dictionary_fixed_width:
  case NativeValueBufferKind::delta_binary_packed:
  case NativeValueBufferKind::byte_stream_split: {
    const auto width = column.native_read_value_width_bytes;
    if (width < 0 ||
        (width > 0 &&
         value_count > std::numeric_limits<std::int64_t>::max() / width)) {
      return sanitize::Status::Invalid(
          "Parquet native read plan: row-window fixed-width overflow");
    }
    SAN_RETURN_NOT_OK(add_i64_checked(&total, value_count * width,
                                      "row-window fixed-width bytes"));
    break;
  }
  case NativeValueBufferKind::none:
    break;
  }
  return total;
}

/// Totals the per-column buffer estimates for a flat row-group window.
sanitize::Result<std::int64_t>
native_reader_flat_window_buffer_bytes(const RowGroupInfo &row_group,
                                       std::int64_t first_row,
                                       std::int64_t row_count) {
  if (!row_group.has_num_rows || row_group.num_rows < 0 || first_row < 0 ||
      row_count < 0 || first_row > row_group.num_rows ||
      row_count > row_group.num_rows - first_row) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: invalid row window");
  }
  std::int64_t total = 0;
  for (const auto &column : row_group.columns) {
    SAN_ASSIGN_OR_RAISE(const auto column_bytes,
                        native_reader_flat_column_window_buffer_bytes(
                            column, row_group.num_rows, first_row, row_count));
    SAN_RETURN_NOT_OK(
        add_i64_checked(&total, column_bytes, "row-window column bytes"));
  }
  return total;
}

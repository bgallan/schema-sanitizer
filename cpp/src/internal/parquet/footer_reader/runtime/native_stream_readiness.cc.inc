// Explains whether footer metadata, encodings, compression, and schema shape
// support native streaming. This fragment is included by the consolidated
// footer reader inside its private implementation namespace.

/// Joins a column's schema path for readiness diagnostics.
std::string column_path_label(const ColumnChunkInfo &column) {
  if (column.path_in_schema.empty()) {
    return "<unknown>";
  }
  std::string out;
  for (std::size_t i = 0; i < column.path_in_schema.size(); ++i) {
    if (i > 0) {
      out.push_back('.');
    }
    out += column.path_in_schema[i];
  }
  return out;
}

/// Reports whether a Parquet physical type has a bounded native Arrow decoder.
bool supported_native_reader_physical_type(const ColumnChunkInfo &column) {
  if (!column.has_physical_type) {
    return false;
  }
  switch (column.physical_type) {
  case kPhysicalBoolean:
  case kPhysicalInt32:
  case kPhysicalInt64:
  case kPhysicalFloat:
  case kPhysicalDouble:
  case kPhysicalByteArray:
  case kPhysicalFixedLenByteArray:
    return true;
  default:
    return false;
  }
}

/// Adds readiness blocker with checked offsets, counts, and retained-capacity
/// accounting.
void add_readiness_blocker(NativeReadinessInfo *info, std::string blocker) {
  if (!info) {
    return;
  }
  info->ready = false;
  info->blockers.push_back(std::move(blocker));
}

sanitize::Status
native_plain_path_materializability_status(const std::vector<std::string> &path,
                                           std::int16_t max_repetition_level,
                                           bool top_level_required);

// ---- empty_file ----

/// Adds readiness blockers when an empty file's schema cannot produce an Arrow
/// layout.
void append_empty_file_readiness_blockers(const FooterInfo &info,
                                          NativeReadinessInfo *readiness) {
  if (!info.row_groups.empty() || info.num_rows != 0 ||
      info.schema_elements.empty()) {
    return;
  }
  auto leaves = schema_leaf_levels(info.schema_elements);
  if (!leaves.ok()) {
    add_readiness_blocker(readiness,
                          "empty file schema is not materializable yet");
    return;
  }
  std::vector<LeafLevelInfo> projected_leaves;
  const auto projection_status = project_leaf_levels_for_columns(
      leaves.ValueOrDie(), info.projected_columns, &projected_leaves);
  if (!projection_status.ok()) {
    add_readiness_blocker(readiness, "empty file schema projection failed");
  }
  for (const auto &leaf : projected_leaves) {
    std::string label;
    for (std::size_t i = 0; i < leaf.path.size(); ++i) {
      if (i > 0) {
        label.push_back('.');
      }
      label += leaf.path[i];
    }
    if (label.empty()) {
      label = "<unknown>";
    }
    const auto materializability_status =
        native_plain_path_materializability_status(
            leaf.path, leaf.max_repetition_level, leaf.top_level_required);
    if (!materializability_status.ok()) {
      add_readiness_blocker(
          readiness, label + ": nested path is not materializable yet: " +
                         materializability_status.message());
    }
    if (leaf.native_arrow_format.empty()) {
      add_readiness_blocker(readiness,
                            label + ": native Arrow format was not planned");
    }
  }
}

// ---- column ----

/// Adds blockers for unsupported, incomplete, or inconsistent pages in one
/// column chunk.
void append_column_page_readiness_blockers(const ColumnChunkInfo &column,
                                           std::string_view label,
                                           NativeReadinessInfo *readiness) {
  std::int64_t data_values = 0;
  bool saw_data_page = false;
  for (const auto &page : column.pages) {
    if (page.payload_verification_skipped) {
      add_readiness_blocker(readiness,
                            std::string(label) +
                                ": page payload verification skipped");
    }
    if (!page.payload_verified) {
      add_readiness_blocker(readiness, std::string(label) +
                                           ": page payload was not verified");
    }
    if (!page.values_decoded || page.values_decode_skipped) {
      add_readiness_blocker(readiness, std::string(label) +
                                           ": page values were not decoded");
    }
    if (page.is_dictionary_page) {
      continue;
    }
    saw_data_page = true;
    if (!page.levels_decoded) {
      add_readiness_blocker(readiness, std::string(label) +
                                           ": page levels were not decoded");
    }
    if (!page.validity_bitmap_decoded) {
      add_readiness_blocker(readiness,
                            std::string(label) +
                                ": page validity bitmap was not decoded");
    }
    if (page.has_num_values) {
      data_values += page.num_values;
    } else {
      add_readiness_blocker(
          readiness, std::string(label) + ": data page is missing value count");
    }
  }
  if (column.has_num_values && data_values != column.num_values) {
    add_readiness_blocker(
        readiness, std::string(label) +
                       ": decoded page values do not match column value count");
  }
  if (!saw_data_page && column.has_num_values && column.num_values > 0) {
    add_readiness_blocker(readiness,
                          std::string(label) + ": column has no data pages");
  }
}

/// Adds all native-reader blockers for one projected Parquet column to the
/// diagnostics list.
void append_column_readiness_blockers(const ColumnChunkInfo &column,
                                      NativeReadinessInfo *readiness) {
  const auto label = column_path_label(column);
  const bool repeated_path = column.max_repetition_level != 0;
  const bool recursive_layouts_decoded =
      repeated_path &&
      column.repeated_level_layouts.size() ==
          static_cast<std::size_t>(column.max_repetition_level) &&
      std::all_of(
          column.repeated_level_layouts.begin(),
          column.repeated_level_layouts.end(),
          [](const RepeatedLevelLayoutInfo &layout) { return layout.decoded; });
  const bool repeated_layout_ready =
      !repeated_path ||
      (recursive_layouts_decoded && column.native_read_plan_decoded);
  const auto materializability_status =
      native_plain_path_materializability_status(column.path_in_schema,
                                                 column.max_repetition_level,
                                                 column.top_level_required);
  if (!materializability_status.ok()) {
    add_readiness_blocker(readiness,
                          label + ": nested path is not materializable yet: " +
                              materializability_status.message());
  }
  if (repeated_path && !repeated_layout_ready) {
    add_readiness_blocker(
        readiness, label + ": repeated levels are not materializable yet");
  }
  if (!supported_native_reader_physical_type(column)) {
    add_readiness_blocker(readiness, label + ": unsupported physical type");
  }
  if (column.native_arrow_format.empty()) {
    add_readiness_blocker(readiness,
                          label + ": native Arrow format was not planned");
  }
  if (!column.has_codec || (column.codec != kCompressionUncompressed &&
                            column.codec != kCompressionSnappy &&
                            column.codec != kCompressionGzip)) {
    add_readiness_blocker(readiness, label + ": unsupported compression");
  }
  if (!repeated_path && !column.offset_index.decoded) {
    add_readiness_blocker(readiness, label + ": offset index was not decoded");
  }
  if (!repeated_path && !column.column_index.decoded) {
    add_readiness_blocker(readiness, label + ": column index was not decoded");
  }
  if (!column.native_read_plan_decoded) {
    add_readiness_blocker(readiness,
                          label + ": native read page plan was not decoded");
  }
  append_column_page_readiness_blockers(column, label, readiness);
}

// ---- gate ----

/// Evaluates native-reader readiness and optionally returns the planned output
/// layout.
NativeReadinessInfo native_reader_readiness(
    const FooterInfo &info,
    std::vector<NativeParquetOutputField> *planned_output_layout = nullptr,
    std::int64_t max_buffer_bytes =
        configured_native_reader_max_buffer_bytes(-1)) {
  NativeReadinessInfo readiness;
  std::vector<NativeParquetOutputField> local_output_layout;
  auto *output_layout =
      planned_output_layout ? planned_output_layout : &local_output_layout;
  output_layout->clear();
  bool output_layout_ready = info.row_groups.empty();
  if (!info.row_groups.empty()) {
    const auto output_layout_status = build_native_output_layout(
        info.row_groups.front().columns, output_layout);
    if (!output_layout_status.ok()) {
      add_readiness_blocker(
          &readiness,
          "row group 0: recursive output layout is not materializable: " +
              output_layout_status.message());
      output_layout->clear();
    } else {
      output_layout_ready = true;
    }
  }
  if (info.created_by != "schema-sanitizer native parquet writer") {
    add_readiness_blocker(&readiness,
                          "file was not written by schema-sanitizer native "
                          "parquet writer");
  }
  if (info.schema_elements.empty()) {
    add_readiness_blocker(&readiness, "missing schema elements");
  }
  if (info.row_groups.empty() && info.num_rows > 0) {
    add_readiness_blocker(&readiness, "non-empty file has no row groups");
  }
  append_empty_file_readiness_blockers(info, &readiness);

  for (std::size_t row_group_index = 0;
       row_group_index < info.row_groups.size(); ++row_group_index) {
    const auto &row_group = info.row_groups[row_group_index];
    if (row_group.columns.empty() && row_group.has_num_rows &&
        row_group.num_rows > 0) {
      add_readiness_blocker(&readiness, "row group " +
                                            std::to_string(row_group_index) +
                                            " has rows but no columns");
    }
    for (const auto &column : row_group.columns) {
      append_column_readiness_blockers(column, &readiness);
    }
    auto estimated = native_reader_row_group_buffer_bytes(row_group);
    if (!estimated.ok()) {
      add_readiness_blocker(&readiness,
                            "row group " + std::to_string(row_group_index) +
                                ": native buffer estimate failed: " +
                                estimated.status().message());
    } else if (*estimated > max_buffer_bytes) {
      bool flat_window_fits = false;
      if (output_layout_ready &&
          native_output_layout_supports_flat_windows(*output_layout,
                                                     row_group) &&
          row_group.num_rows > 0) {
        auto one_row = native_reader_flat_window_buffer_bytes(row_group, 0, 1);
        flat_window_fits = one_row.ok() && *one_row <= max_buffer_bytes;
      }
      if (!flat_window_fits) {
        add_readiness_blocker(&readiness, "row group " +
                                              std::to_string(row_group_index) +
                                              ": native buffer estimate " +
                                              std::to_string(*estimated) +
                                              " exceeds configured limit " +
                                              std::to_string(max_buffer_bytes));
      }
    }
    if (output_layout_ready) {
      const auto output_layout_status =
          validate_native_recursive_row_group_output_layout(row_group,
                                                            *output_layout);
      if (!output_layout_status.ok()) {
        add_readiness_blocker(
            &readiness,
            "row group " + std::to_string(row_group_index) +
                ": recursive output layout is not materializable: " +
                output_layout_status.message());
      }
    }
  }
  return readiness;
}

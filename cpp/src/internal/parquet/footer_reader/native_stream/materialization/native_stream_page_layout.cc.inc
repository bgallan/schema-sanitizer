sanitize::Status materialization_payload(std::ifstream &file,
                                         const ColumnChunkInfo &column,
                                         const PageHeaderInfo &page,
                                         NativeParquetPageScratch *scratch,
                                         std::string_view *out) {
  if (!scratch || !out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: page scratch is null");
  }
  if (!column.has_codec || !page.has_compressed_page_size ||
      !page.has_uncompressed_page_size) {
    return sanitize::Status::Invalid(
        "native Parquet reader: page payload sizes are incomplete");
  }
  if (page.compressed_page_size < 0 || page.uncompressed_page_size < 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: negative page payload size");
  }
  const auto compressed_size =
      static_cast<std::size_t>(page.compressed_page_size);
  const auto uncompressed_size =
      static_cast<std::size_t>(page.uncompressed_page_size);
  if (compressed_size > scratch->max_page_bytes ||
      uncompressed_size > scratch->max_page_bytes ||
      (column.codec != kCompressionUncompressed &&
       compressed_size > scratch->max_page_bytes - uncompressed_size)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: page scratch exceeds effective operation "
        "limit ",
        scratch->max_page_bytes);
  }
  SAN_RETURN_NOT_OK(read_exact_payload_into(
      file, page.compressed_payload_offset, page.compressed_page_size,
      &scratch->compressed_payload));
  SAN_RETURN_NOT_OK(
      validate_parquet_page_checksum(page, scratch->compressed_payload));
  if (column.codec == kCompressionUncompressed) {
    if (page.compressed_page_size != page.uncompressed_page_size) {
      return sanitize::Status::Invalid(
          "native Parquet reader: uncompressed page size mismatch");
    }
    *out = scratch->compressed_payload;
    return {};
  }
  if (column.codec == kCompressionSnappy) {
    SAN_RETURN_NOT_OK(snappy_decompress_payload_into(
        scratch->compressed_payload, page.uncompressed_page_size,
        &scratch->decompressed_payload));
    *out = scratch->decompressed_payload;
    return {};
  }
  if (column.codec == kCompressionGzip) {
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
    SAN_RETURN_NOT_OK(gzip_decompress_payload_into(
        scratch->compressed_payload, page.uncompressed_page_size,
        &scratch->decompressed_payload));
    *out = scratch->decompressed_payload;
    return {};
#else
    return sanitize::Status::NotImplemented(
        "native Parquet reader: gzip support was not compiled in");
#endif
  }
  return sanitize::Status::NotImplemented(
      "native Parquet reader: unsupported compression");
}

sanitize::Result<std::int64_t> materialize_row_struct_validity_from_levels(
    const ColumnChunkInfo &column, std::int64_t row_count,
    std::int16_t defined_level, std::vector<std::uint8_t> *validity) {
  if (!validity) {
    return sanitize::Status::Invalid(
        "native Parquet reader: row struct validity output is null");
  }
  if (row_count < 0 ||
      row_count > std::numeric_limits<std::int64_t>::max() - 7) {
    return sanitize::Status::Invalid(
        "native Parquet reader: row struct row count is invalid");
  }
  if (defined_level <= 0) {
    validity->clear();
    return std::int64_t{0};
  }
  const auto validity_bytes = (row_count + 7) / 8;
  validity->assign(static_cast<std::size_t>(validity_bytes), 0);
  std::int64_t null_count = row_count;
  std::int64_t current_row = -1;
  bool saw_level = false;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (!page.levels_decoded || !page.has_num_values ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values) ||
        page.decoded_repetition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: row struct level count mismatch");
    }
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      saw_level = true;
      const auto repetition =
          page.decoded_repetition_level_values[static_cast<std::size_t>(row)];
      if (repetition != 0) {
        continue;
      }
      ++current_row;
      if (current_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: row struct row count exceeds row group");
      }
      if (page.decoded_definition_level_values[static_cast<std::size_t>(row)] >=
          defined_level) {
        set_output_validity_bit(validity, current_row);
        --null_count;
      }
    }
  }
  if (row_count > 0 && (!saw_level || current_row + 1 != row_count)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: row struct row count mismatch");
  }
  if (null_count == 0) {
    validity->clear();
  }
  return null_count;
}

bool repeated_level_layouts_match(const RepeatedLevelLayoutInfo &left,
                                  const RepeatedLevelLayoutInfo &right) {
  return left.decoded == right.decoded && left.row_count == right.row_count &&
         left.null_count == right.null_count &&
         left.element_count == right.element_count &&
         left.offsets == right.offsets &&
         left.validity_bitmap == right.validity_bitmap;
}

sanitize::Result<std::size_t> native_recursive_repeated_layout_index_for_node(
    const NativeRecursiveMaterializationTree &tree, std::size_t node_index);

sanitize::Result<const RepeatedLevelLayoutInfo *>
decoded_repeated_level_layout_info(const ColumnChunkInfo &column,
                                   std::size_t layout_index) {
  if (layout_index >= column.repeated_level_layouts.size() ||
      !column.repeated_level_layouts[layout_index].decoded) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: recursive repeated layout was not decoded");
  }
  return &column.repeated_level_layouts[layout_index];
}

// Cached container-column lookup and row-group repeated-layout validation.
// Included directly by footer_reader.cc after page-layout helpers.

sanitize::Result<std::size_t> native_recursive_repeated_layout_index_for_node(
    const NativeRecursiveMaterializationTree &tree, std::size_t node_index) {
  if (!tree.valid || node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive repeated layout node is invalid");
  }
  const auto &node = tree.nodes[node_index];
  if (node.kind != NativeRecursiveMaterializationNodeKind::List &&
      node.kind != NativeRecursiveMaterializationNodeKind::Map) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive node has no repeated layout");
  }
  if (!node.repeated_layout_index.has_value()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive repeated layout index is missing");
  }
  return node.repeated_layout_index.value();
}

sanitize::Result<const ColumnChunkInfo *>
native_recursive_layout_column_for_node(
    const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree, std::size_t node_index) {
  if (!tree.valid || !tree.traversal_plan_valid ||
      node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive layout column node is invalid");
  }
  const auto column_index = tree.nodes[node_index].layout_column_index;
  if (!column_index.has_value() ||
      column_index.value() >= row_group.columns.size()) {
    return sanitize::Status::Invalid("native Parquet reader: recursive layout "
                                     "column plan is missing or invalid");
  }
  return &row_group.columns[column_index.value()];
}

sanitize::Result<const ColumnChunkInfo *>
select_recursive_repeated_layout_column_for_node(
    const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree,
    std::size_t repeated_node_index) {
  if (!tree.valid || repeated_node_index >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive repeated layout node is invalid");
  }
  const auto &node = tree.nodes[repeated_node_index];
  if (node.kind != NativeRecursiveMaterializationNodeKind::List &&
      node.kind != NativeRecursiveMaterializationNodeKind::Map) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive layout node is not repeated");
  }
  return native_recursive_layout_column_for_node(row_group, tree,
                                                 repeated_node_index);
}

sanitize::Status validate_native_recursive_repeated_node_layouts(
    const RowGroupInfo &row_group,
    const NativeRecursiveMaterializationTree &tree) {
  if (!tree.valid || !tree.traversal_plan_valid) {
    return sanitize::Status::Invalid("native Parquet reader: recursive "
                                     "repetition validation plan is invalid");
  }
  for (const std::size_t node_index : tree.repeated_node_indices) {
    if (node_index >= tree.nodes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive repeated node plan is invalid");
    }
    const auto &node = tree.nodes[node_index];
    if (node.kind != NativeRecursiveMaterializationNodeKind::List &&
        node.kind != NativeRecursiveMaterializationNodeKind::Map) {
      return sanitize::Status::Invalid("native Parquet reader: recursive "
                                       "repeated node plan kind is invalid");
    }
    SAN_ASSIGN_OR_RAISE(
        const auto repeated_layout_index,
        native_recursive_repeated_layout_index_for_node(tree, node_index));
    SAN_ASSIGN_OR_RAISE(const auto leaf_columns,
                        native_recursive_materialization_leaf_columns_from_node(
                            tree, node_index));
    if (leaf_columns.empty()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive repeated node has no leaves");
    }
    const RepeatedLevelLayoutInfo *reference_layout = nullptr;
    for (const std::size_t column_index : leaf_columns) {
      if (column_index >= row_group.columns.size()) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive repetition column is invalid");
      }
      const auto &column = row_group.columns[column_index];
      if (column.max_repetition_level <= 0 ||
          repeated_layout_index >=
              static_cast<std::size_t>(column.max_repetition_level)) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: recursive repeated node has no matching "
            "column layout");
      }
      SAN_ASSIGN_OR_RAISE(
          const auto *layout,
          decoded_repeated_level_layout_info(column, repeated_layout_index));
      if (!reference_layout) {
        reference_layout = layout;
      } else if (!repeated_level_layouts_match(*reference_layout, *layout)) {
        return sanitize::Status::NotImplemented(
            "native Parquet reader: recursive repeated leaf layouts differ");
      }
    }
  }
  return {};
}

sanitize::Status validate_native_recursive_row_group_output_layout(
    const RowGroupInfo &row_group,
    const std::vector<NativeParquetOutputField> &layout) {
  for (const auto &field : layout) {
    if (field.column_indices.empty() ||
        field.column_indices.front() >= row_group.columns.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: output field has invalid columns");
    }
    if (field.recursive_tree.repetition_depth > 0) {
      SAN_RETURN_NOT_OK(validate_native_recursive_repeated_node_layouts(
          row_group, field.recursive_tree));
    }
  }
  return {};
}

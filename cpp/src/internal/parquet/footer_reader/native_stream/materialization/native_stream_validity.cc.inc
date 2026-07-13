// ---- struct_rows ----

sanitize::Result<std::int64_t> materialize_struct_validity_at_definition_level(
    const ColumnChunkInfo &column, std::int64_t row_count,
    std::int16_t defined_level, std::vector<std::uint8_t> *validity) {
  if (column.max_repetition_level > 0) {
    return materialize_row_struct_validity_from_levels(column, row_count,
                                                       defined_level, validity);
  }
  if (!validity) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive struct validity output is null");
  }
  if (defined_level <= 0) {
    validity->clear();
    return std::int64_t{0};
  }
  if (row_count < 0 ||
      row_count > std::numeric_limits<std::int64_t>::max() - 7) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive struct row count is invalid");
  }
  const auto validity_bytes = (row_count + 7) / 8;
  validity->assign(static_cast<std::size_t>(validity_bytes), 0);
  std::int64_t null_count = row_count;
  for (const auto &span : column.native_read_page_spans) {
    if (span.page_index < 0 ||
        static_cast<std::size_t>(span.page_index) >= column.pages.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive struct page span is invalid");
    }
    const auto &page = column.pages[static_cast<std::size_t>(span.page_index)];
    if (span.row_count < 0 || page.decoded_definition_level_values.size() !=
                                  static_cast<std::size_t>(span.row_count)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive struct definition level count "
          "mismatch");
    }
    for (std::int32_t row = 0; row < span.row_count; ++row) {
      const auto global_row = span.first_row_index + row;
      if (global_row < 0 || global_row >= row_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive struct row span exceeds row "
            "group");
      }
      if (page.decoded_definition_level_values[static_cast<std::size_t>(row)] >=
          defined_level) {
        set_output_validity_bit(validity, global_row);
        --null_count;
      }
    }
  }
  if (null_count == 0) {
    validity->clear();
  }
  return null_count;
}

// ---- map_entries ----

struct NativeRecursiveMapEntryLevelContext {
  std::int16_t entry_present_definition_level = 0;
  std::int16_t entry_repetition_level = 1;
};

sanitize::Result<NativeRecursiveMapEntryLevelContext>
native_recursive_map_entry_level_context(const ColumnChunkInfo &column,
                                         std::size_t map_layout_index) {
  const auto generic_levels = generic_list_defined_levels_from_path(column);
  if (generic_levels.empty() || map_layout_index >= generic_levels.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive generic map definition levels are "
        "invalid");
  }
  return NativeRecursiveMapEntryLevelContext{
      generic_levels[map_layout_index],
      static_cast<std::int16_t>(map_layout_index + 1)};
}

sanitize::Result<std::int64_t> materialize_map_entry_struct_validity_at_levels(
    const ColumnChunkInfo &column,
    NativeRecursiveMapEntryLevelContext entry_levels,
    std::int16_t defined_level, std::int64_t element_count,
    std::vector<std::uint8_t> *validity) {
  if (!validity || !list_leaf_value_count_is_materializable(column)) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid recursive map-entry struct validity "
        "layout");
  }
  const auto validity_bytes = (element_count + 7) / 8;
  if (element_count < 0 ||
      static_cast<std::uint64_t>(validity_bytes) > kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive map-entry struct validity bitmap "
        "exceeds memory limit");
  }
  if (defined_level <= entry_levels.entry_present_definition_level) {
    validity->clear();
    return std::int64_t{0};
  }
  validity->assign(static_cast<std::size_t>(validity_bytes), 0);
  std::int64_t null_count = 0;
  std::int64_t element_index = 0;
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values) ||
        page.decoded_repetition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive map-entry struct level "
          "count mismatch");
    }
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      const auto repetition =
          page.decoded_repetition_level_values[static_cast<std::size_t>(row)];
      if (definition <= entry_levels.entry_present_definition_level ||
          repetition > entry_levels.entry_repetition_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive map-entry struct validity "
            "exceeds element count");
      }
      if (definition >= defined_level) {
        set_output_validity_bit(validity,
                                static_cast<std::int32_t>(element_index));
      } else {
        ++null_count;
      }
      ++element_index;
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive map-entry struct validity element "
        "count mismatch");
  }
  if (null_count == 0) {
    validity->clear();
  }
  return null_count;
}

// ---- list_elements ----

sanitize::Result<std::int64_t>
materialize_list_struct_child_validity_at_definition_level(
    const ColumnChunkInfo &column, std::size_t list_layout_index,
    std::int16_t defined_level, std::vector<std::uint8_t> *validity) {
  if (!validity) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid recursive list-struct child validity "
        "layout");
  }
  SAN_ASSIGN_OR_RAISE(const auto layout,
                      native_repeated_level_layout(column, list_layout_index));
  if (!layout.decoded || layout.element_count < 0 ||
      layout.element_count >
          static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid recursive list-struct child validity "
        "layout");
  }
  const auto element_count = layout.element_count;
  const auto validity_bytes = (element_count + 7) / 8;
  if (static_cast<std::uint64_t>(validity_bytes) > kMaxValidityBitmapBytes) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive list-struct child validity bitmap "
        "exceeds memory limit");
  }
  const auto generic_levels = generic_list_defined_levels_from_path(column);
  if (generic_levels.empty() || list_layout_index >= generic_levels.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive list-struct child definition levels "
        "are invalid");
  }
  const auto list_defined_level = generic_levels[list_layout_index];
  if (defined_level <= list_defined_level) {
    validity->clear();
    return std::int64_t{0};
  }
  validity->assign(static_cast<std::size_t>(validity_bytes), 0);
  std::int64_t null_count = 0;
  std::int64_t element_index = 0;
  const auto element_repetition_level =
      static_cast<std::int16_t>(list_layout_index + 1);
  for (const auto &page : column.pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    if (page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values) ||
        page.decoded_repetition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive list-struct child level count "
          "mismatch");
    }
    for (std::int32_t row = 0; row < page.num_values; ++row) {
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(row)];
      const auto repetition =
          page.decoded_repetition_level_values[static_cast<std::size_t>(row)];
      if (definition <= list_defined_level ||
          repetition > element_repetition_level) {
        continue;
      }
      if (element_index >= element_count) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive list-struct child validity "
            "exceeds element count");
      }
      if (definition >= defined_level) {
        set_output_validity_bit(validity,
                                static_cast<std::int32_t>(element_index));
      } else {
        ++null_count;
      }
      ++element_index;
    }
  }
  if (element_index != element_count) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive list-struct child validity element "
        "count mismatch");
  }
  if (null_count == 0) {
    validity->clear();
  }
  return null_count;
}

// ---- native_stream_container_validity ----

struct NativeRecursiveMapArrayLayout {
  NativeRepeatedLevelLayoutView repeated_layout;
};

struct NativeRecursiveListArrayLayout {
  NativeRepeatedLevelLayoutView repeated_layout;
};

sanitize::Result<NativeRecursiveMapArrayLayout>
native_recursive_map_array_layout(const ColumnChunkInfo &column,
                                  std::size_t layout_index) {
  SAN_ASSIGN_OR_RAISE(const auto layout,
                      native_repeated_level_layout(column, layout_index));
  if (!layout.decoded) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: recursive map layout was not decoded");
  }
  return NativeRecursiveMapArrayLayout{layout};
}

sanitize::Result<NativeRecursiveListArrayLayout>
native_recursive_list_array_layout(const ColumnChunkInfo &column,
                                   std::size_t layout_index) {
  SAN_ASSIGN_OR_RAISE(const auto layout,
                      native_repeated_level_layout(column, layout_index));
  if (!layout.decoded) {
    return sanitize::Status::NotImplemented(
        "native Parquet reader: recursive list layout was not decoded");
  }
  return NativeRecursiveListArrayLayout{layout};
}

sanitize::Status configure_native_recursive_list_array(
    NativeParquetListArray *list_array,
    const NativeRepeatedLevelLayoutView &layout, ArrowArray *child) {
  if (!layout.validity_bitmap || !layout.offsets) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive list layout buffers are invalid");
  }
  return configure_native_list_array(list_array, *layout.validity_bitmap,
                                     *layout.offsets, child, layout.row_count,
                                     layout.null_count);
}

struct NativeRecursiveStructValidity {
  std::int64_t length = 0;
  std::int64_t null_count = 0;
};

sanitize::Result<NativeRecursiveStructValidity>
materialize_native_recursive_map_value_struct_validity(
    const ColumnChunkInfo &column, std::size_t map_layout_index,
    std::int16_t defined_level, std::vector<std::uint8_t> *validity) {
  SAN_ASSIGN_OR_RAISE(const auto map_layout, native_recursive_map_array_layout(
                                                 column, map_layout_index));
  const auto &layout = map_layout.repeated_layout;
  SAN_ASSIGN_OR_RAISE(
      const auto entry_levels,
      native_recursive_map_entry_level_context(column, map_layout_index));
  SAN_ASSIGN_OR_RAISE(
      const auto null_count,
      materialize_map_entry_struct_validity_at_levels(
          column, entry_levels, defined_level, layout.element_count, validity));
  return NativeRecursiveStructValidity{layout.element_count, null_count};
}

sanitize::Result<NativeRecursiveStructValidity>
materialize_native_recursive_list_element_struct_validity(
    const ColumnChunkInfo &column, std::size_t list_layout_index,
    std::int16_t defined_level, std::vector<std::uint8_t> *validity) {
  SAN_ASSIGN_OR_RAISE(
      const auto list_array_layout,
      native_recursive_list_array_layout(column, list_layout_index));
  const auto &list_layout = list_array_layout.repeated_layout;
  SAN_ASSIGN_OR_RAISE(
      const auto null_count,
      materialize_list_struct_child_validity_at_definition_level(
          column, list_layout_index, defined_level, validity));
  return NativeRecursiveStructValidity{list_layout.element_count, null_count};
}

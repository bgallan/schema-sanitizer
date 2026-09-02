// Derives element intervals, offsets, and validity from Parquet repetition and
// definition levels. This fragment is included by the consolidated footer
// reader inside its private implementation namespace.

/// Selects the native decoded-value buffer representation for one Parquet page
/// encoding.
NativeValueBufferKind value_buffer_kind_for_page(const ColumnChunkInfo &column,
                                                 const PageHeaderInfo &page) {
  if (!page.has_value_encoding || !column.has_physical_type) {
    return NativeValueBufferKind::none;
  }
  if (page.value_encoding == kEncodingPlain) {
    if (column.physical_type == kPhysicalBoolean) {
      return NativeValueBufferKind::bit_packed_boolean;
    }
    if (column.physical_type == kPhysicalByteArray) {
      return NativeValueBufferKind::plain_byte_array;
    }
    if (column.physical_type == kPhysicalInt32 ||
        column.physical_type == kPhysicalInt64 ||
        column.physical_type == kPhysicalFloat ||
        column.physical_type == kPhysicalDouble ||
        column.physical_type == kPhysicalFixedLenByteArray) {
      return NativeValueBufferKind::fixed_width;
    }
    return NativeValueBufferKind::none;
  }
  if (page.value_encoding == kEncodingDeltaBinaryPacked) {
    return NativeValueBufferKind::delta_binary_packed;
  }
  if (page.value_encoding == kEncodingDeltaLengthByteArray) {
    return NativeValueBufferKind::delta_length_byte_array;
  }
  if (page.value_encoding == kEncodingRleDictionary) {
    if (column.physical_type == kPhysicalByteArray &&
        !column.decoded_dictionary_values.empty()) {
      return NativeValueBufferKind::dictionary_byte_array;
    }
    if ((column.physical_type == kPhysicalInt32 ||
         column.physical_type == kPhysicalInt64 ||
         column.physical_type == kPhysicalFloat ||
         column.physical_type == kPhysicalDouble ||
         column.physical_type == kPhysicalFixedLenByteArray) &&
        !column.decoded_dictionary_fixed_width_values.empty()) {
      return NativeValueBufferKind::dictionary_fixed_width;
    }
    return NativeValueBufferKind::none;
  }
  if (page.value_encoding == kEncodingByteStreamSplit) {
    return NativeValueBufferKind::byte_stream_split;
  }
  return NativeValueBufferKind::none;
}

/// Returns the checked physical value width required by one native page plan.
std::int32_t value_width_bytes_for_page(const ColumnChunkInfo &column,
                                        const PageHeaderInfo &page) {
  if (!page.has_value_encoding) {
    return 0;
  }
  if (page.value_encoding == kEncodingByteStreamSplit ||
      page.value_encoding == kEncodingDeltaBinaryPacked ||
      page.value_encoding == kEncodingRleDictionary ||
      page.value_encoding == kEncodingPlain) {
    return arrow_value_width_for_column(column).value_or(0);
  }
  return 0;
}

/// Returns the Arrow buffer count required by a decoded native value
/// representation.
constexpr std::int32_t
arrow_buffer_count_for_value_kind(NativeValueBufferKind kind) noexcept {
  switch (kind) {
  case NativeValueBufferKind::plain_byte_array:
  case NativeValueBufferKind::dictionary_byte_array:
  case NativeValueBufferKind::delta_length_byte_array:
    return 3;
  case NativeValueBufferKind::fixed_width:
  case NativeValueBufferKind::delta_binary_packed:
  case NativeValueBufferKind::dictionary_fixed_width:
  case NativeValueBufferKind::byte_stream_split:
  case NativeValueBufferKind::bit_packed_boolean:
    return 2;
  case NativeValueBufferKind::none:
    return 0;
  }
  return 0;
}
// Repeated-level layout decoding and native leaf read planning.

struct RepeatedLeafReadPlanState {
  bool complete = true;
  bool saw_data_page = false;
  NativeValueBufferKind value_kind = NativeValueBufferKind::none;
  std::int32_t value_width = 0;
  std::int32_t dictionary_index_bit_width = 0;
};

/// Reports whether a repeated Parquet leaf has a native decoded-value
/// representation.
constexpr bool
is_supported_repeated_leaf_value_kind(NativeValueBufferKind kind,
                                      std::int32_t width) noexcept {
  return (kind == NativeValueBufferKind::fixed_width && width > 0) ||
         kind == NativeValueBufferKind::plain_byte_array ||
         kind == NativeValueBufferKind::dictionary_byte_array ||
         (kind == NativeValueBufferKind::dictionary_fixed_width && width > 0) ||
         (kind == NativeValueBufferKind::delta_binary_packed && width > 0) ||
         kind == NativeValueBufferKind::delta_length_byte_array ||
         (kind == NativeValueBufferKind::byte_stream_split && width > 0) ||
         kind == NativeValueBufferKind::bit_packed_boolean;
}

/// Accumulates page levels and value spans needed to plan one repeated Parquet
/// leaf.
void observe_repeated_leaf_data_page(const ColumnChunkInfo &column,
                                     const PageHeaderInfo &page,
                                     RepeatedLeafReadPlanState *state) {
  state->saw_data_page = true;
  if (!page.has_value_encoding || !page.values_decoded ||
      page.values_decode_skipped || !page.has_compressed_page_size) {
    state->complete = false;
  }

  const auto value_kind = value_buffer_kind_for_page(column, page);
  const auto value_width = value_width_bytes_for_page(column, page);
  if (!is_supported_repeated_leaf_value_kind(value_kind, value_width)) {
    state->complete = false;
  } else if (state->value_kind == NativeValueBufferKind::none) {
    state->value_kind = value_kind;
    state->value_width = value_width;
  } else if (state->value_kind != value_kind ||
             state->value_width != value_width) {
    state->complete = false;
  }

  if (value_kind != NativeValueBufferKind::dictionary_byte_array &&
      value_kind != NativeValueBufferKind::dictionary_fixed_width) {
    return;
  }
  if (state->dictionary_index_bit_width == 0) {
    state->dictionary_index_bit_width = page.dictionary_index_bit_width;
  } else if (page.dictionary_index_bit_width != 0 &&
             state->dictionary_index_bit_width !=
                 page.dictionary_index_bit_width) {
    state->complete = false;
  }
}

/// Assigns repeated leaf native read plan from validated schema and page
/// metadata for later materialization.
sanitize::Status assign_repeated_leaf_native_read_plan(
    ColumnChunkInfo *column, const RepeatedLevelLayoutInfo &leaf_layout,
    const RepeatedLeafReadPlanState &state) {
  if (!state.complete || !state.saw_data_page ||
      !is_supported_repeated_leaf_value_kind(state.value_kind,
                                             state.value_width)) {
    return {};
  }

  column->native_read_plan_decoded = true;
  column->native_read_data_page_count = 0;
  column->native_read_total_rows = leaf_layout.element_count;
  column->native_read_total_non_nulls = leaf_layout.non_null_value_count;
  column->native_read_total_nulls =
      leaf_layout.element_count - leaf_layout.non_null_value_count;
  column->native_read_validity_bitmap_bytes =
      column->native_read_total_nulls > 0 ? (leaf_layout.element_count + 7) / 8
                                          : 0;
  column->native_read_value_payload_bytes = 0;
  for (const auto &page : column->pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    ++column->native_read_data_page_count;
    column->native_read_value_payload_bytes += page.decoded_value_bytes;
  }

  column->native_read_materialized_value_bytes =
      (state.value_kind == NativeValueBufferKind::fixed_width ||
       state.value_kind == NativeValueBufferKind::dictionary_fixed_width ||
       state.value_kind == NativeValueBufferKind::byte_stream_split ||
       state.value_kind == NativeValueBufferKind::delta_binary_packed)
          ? leaf_layout.element_count * state.value_width
          : std::int64_t{0};
  if (state.value_kind == NativeValueBufferKind::bit_packed_boolean) {
    SAN_ASSIGN_OR_RAISE(
        const auto boolean_value_bytes,
        arrow_boolean_value_buffer_bytes(
            static_cast<std::int32_t>(leaf_layout.element_count)));
    column->native_read_materialized_value_bytes = boolean_value_bytes;
  }

  const bool has_offsets =
      state.value_kind == NativeValueBufferKind::plain_byte_array ||
      state.value_kind == NativeValueBufferKind::delta_length_byte_array ||
      state.value_kind == NativeValueBufferKind::dictionary_byte_array;
  column->native_read_materialized_offset_bytes =
      has_offsets ? (leaf_layout.element_count + 1) * 4 : std::int64_t{0};
  if (has_offsets) {
    for (const auto &page : column->pages) {
      if (!page.is_dictionary_page) {
        column->native_read_materialized_value_bytes +=
            page.materialized_value_bytes;
      }
    }
  }

  column->native_read_value_width_bytes = state.value_width;
  column->native_read_dictionary_index_bit_width =
      state.dictionary_index_bit_width;
  column->native_read_value_buffer_kind = state.value_kind;
  column->native_read_arrow_length = leaf_layout.element_count;
  column->native_read_arrow_null_count = column->native_read_total_nulls;
  column->native_read_arrow_n_buffers = has_offsets ? 3 : 2;
  column->native_read_arrow_n_children = 0;
  column->native_read_has_validity_buffer =
      column->native_read_total_nulls > 0 ? 1 : 0;
  column->native_read_has_offsets_buffer = 0;
  column->native_read_has_values_buffer = 1;
  return {};
}

/// Assigns generic list chain level layout from validated schema and page
/// metadata for later materialization.
sanitize::Status
assign_generic_list_chain_level_layout(std::int64_t row_count,
                                       ColumnChunkInfo *column) {
  if (!column) {
    return {};
  }
  auto list_defined_levels = generic_list_defined_levels_from_path(*column);
  auto depth = static_cast<std::int16_t>(list_defined_levels.size());
  if (depth <= 0 || list_defined_levels.empty()) {
    return {};
  }
  if (row_count < 0 ||
      row_count > std::numeric_limits<std::int32_t>::max() - 1) {
    return sanitize::Status::Invalid(
        "Parquet generic repeated levels: row count is not materializable");
  }

  column->repeated_level_layouts.clear();
  column->repeated_level_layouts.resize(static_cast<std::size_t>(depth));
  auto &outer = column->repeated_level_layouts[0];
  outer.offsets.reserve(static_cast<std::size_t>(row_count) + 1U);
  outer.offsets.push_back(0);
  outer.row_count = row_count;
  outer.validity_bitmap.assign(static_cast<std::size_t>((row_count + 7) / 8),
                               0);
  for (std::size_t level = 1; level < column->repeated_level_layouts.size();
       ++level) {
    column->repeated_level_layouts[level].offsets.push_back(0);
  }

  RepeatedLeafReadPlanState native_read_plan;
  std::vector<std::int32_t> current_child_counts(
      static_cast<std::size_t>(depth), 0);
  std::vector<bool> saw_layout_row(static_cast<std::size_t>(depth), false);
  std::int64_t current_row = -1;
  bool saw_level = false;

  for (const auto &page : column->pages) {
    if (page.is_dictionary_page) {
      continue;
    }
    observe_repeated_leaf_data_page(*column, page, &native_read_plan);
    if (!page.levels_decoded || !page.has_num_values ||
        page.decoded_definition_level_values.size() !=
            static_cast<std::size_t>(page.num_values) ||
        page.decoded_repetition_level_values.size() !=
            static_cast<std::size_t>(page.num_values)) {
      column->repeated_level_layouts.clear();
      return {};
    }
    std::int32_t page_non_null_values = 0;
    for (std::int32_t i = 0; i < page.num_values; ++i) {
      saw_level = true;
      const auto definition =
          page.decoded_definition_level_values[static_cast<std::size_t>(i)];
      const auto repetition =
          page.decoded_repetition_level_values[static_cast<std::size_t>(i)];
      if (definition < 0 || definition > column->max_definition_level ||
          repetition < 0 || repetition > column->max_repetition_level) {
        return sanitize::Status::Invalid(
            "Parquet generic repeated levels: level exceeds schema maximum");
      }
      if (repetition == 0) {
        if (current_row >= 0) {
          column->repeated_level_layouts[0].offsets.push_back(
              current_child_counts[0]);
        }
        ++current_row;
        if (current_row >= row_count) {
          return sanitize::Status::Invalid(
              "Parquet generic repeated levels: row count exceeds row group");
        }
        saw_layout_row[0] = true;
        const bool list_valid = definition >= list_defined_levels[0];
        if (list_valid) {
          set_validity_bit(&column->repeated_level_layouts[0].validity_bitmap,
                           static_cast<std::int32_t>(current_row), true);
        } else {
          ++column->repeated_level_layouts[0].null_count;
        }
      } else if (current_row < 0) {
        return sanitize::Status::Invalid(
            "Parquet generic repeated levels: first value does not start a "
            "row");
      } else if (definition <= list_defined_levels[0]) {
        return sanitize::Status::Invalid(
            "Parquet generic repeated levels: repeated null or empty outer "
            "list marker");
      }

      const bool outer_valid = definition >= list_defined_levels[0];
      if (!outer_valid) {
        continue;
      }
      for (std::int16_t level = 1; level < depth; ++level) {
        const auto parent_level = static_cast<std::size_t>(level - 1);
        const auto layout_level = static_cast<std::size_t>(level);
        if (definition <= list_defined_levels[parent_level] ||
            repetition > level) {
          continue;
        }
        if (saw_layout_row[layout_level]) {
          column->repeated_level_layouts[layout_level].offsets.push_back(
              current_child_counts[layout_level]);
        }
        saw_layout_row[layout_level] = true;
        SAN_RETURN_NOT_OK(increment_i32_counter(
            &current_child_counts[parent_level], "child offset"));
        ++column->repeated_level_layouts[parent_level].element_count;
        ++column->repeated_level_layouts[layout_level].row_count;
        SAN_RETURN_NOT_OK(ensure_repeated_layout_validity_capacity(
            &column->repeated_level_layouts[layout_level]));
        const bool list_valid = definition >= list_defined_levels[layout_level];
        if (list_valid) {
          set_validity_bit(
              &column->repeated_level_layouts[layout_level].validity_bitmap,
              static_cast<std::int32_t>(
                  column->repeated_level_layouts[layout_level].row_count - 1),
              true);
        } else {
          ++column->repeated_level_layouts[layout_level].null_count;
        }
      }
      const auto deepest_level = static_cast<std::size_t>(depth - 1);
      if (definition > list_defined_levels[deepest_level]) {
        SAN_RETURN_NOT_OK(increment_i32_counter(
            &current_child_counts[deepest_level], "leaf offset"));
        ++column->repeated_level_layouts[deepest_level].element_count;
        if (definition == column->max_definition_level) {
          ++page_non_null_values;
          ++column->repeated_level_layouts[deepest_level].non_null_value_count;
        }
      }
    }
    if (page_non_null_values != page.decoded_non_null_values) {
      return sanitize::Status::Invalid(
          "Parquet generic repeated levels: non-null value count mismatch");
    }
  }
  if (row_count == 0) {
    for (auto &layout : column->repeated_level_layouts) {
      layout.decoded = true;
    }
    return {};
  }
  if (!saw_level || current_row + 1 != row_count) {
    return sanitize::Status::Invalid(
        "Parquet generic repeated levels: row count mismatch");
  }
  column->repeated_level_layouts[0].offsets.push_back(current_child_counts[0]);
  for (std::int16_t level = 1; level < depth; ++level) {
    const auto layout_level = static_cast<std::size_t>(level);
    if (saw_layout_row[layout_level]) {
      column->repeated_level_layouts[layout_level].offsets.push_back(
          current_child_counts[layout_level]);
    }
  }
  for (std::int16_t level = 0; level < depth; ++level) {
    auto &layout =
        column->repeated_level_layouts[static_cast<std::size_t>(level)];
    if (layout.offsets.size() !=
        static_cast<std::size_t>(layout.row_count + 1)) {
      return sanitize::Status::Invalid(
          "Parquet generic repeated levels: offset count mismatch");
    }
    if (layout.null_count == 0) {
      layout.validity_bitmap.clear();
    } else {
      const auto validity_bytes = (layout.row_count + 7) / 8;
      if (static_cast<std::uint64_t>(validity_bytes) >
          kMaxValidityBitmapBytes) {
        return sanitize::Status::Invalid(
            "Parquet generic repeated levels: validity bitmap exceeds memory "
            "limit");
      }
      layout.validity_bitmap.resize(static_cast<std::size_t>(validity_bytes),
                                    0);
    }
    layout.decoded = true;
  }
  const auto &leaf_layout = column->repeated_level_layouts.back();
  SAN_RETURN_NOT_OK(assign_repeated_leaf_native_read_plan(column, leaf_layout,
                                                          native_read_plan));
  return {};
}

/// Assigns simple list level layout from validated schema and page metadata for
/// later materialization.
sanitize::Status assign_simple_list_level_layout(std::int64_t row_count,
                                                 ColumnChunkInfo *column) {
  if (!column || !is_native_repeated_path_leaf(*column)) {
    return {};
  }
  return assign_generic_list_chain_level_layout(row_count, column);
}

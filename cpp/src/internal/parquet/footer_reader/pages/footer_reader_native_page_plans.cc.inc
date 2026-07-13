sanitize::Status assign_native_read_page_spans(FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid(
        "Parquet native read plan: internal error");
  }
  for (auto &row_group : info->row_groups) {
    for (auto &column : row_group.columns) {
      column.native_read_plan_decoded = false;
      column.native_read_data_page_count = 0;
      column.native_read_total_rows = 0;
      column.native_read_total_non_nulls = 0;
      column.native_read_total_nulls = 0;
      column.native_read_validity_bitmap_bytes = 0;
      column.native_read_value_payload_bytes = 0;
      column.native_read_materialized_value_bytes = 0;
      column.native_read_materialized_offset_bytes = 0;
      column.native_read_value_width_bytes = 0;
      column.native_read_dictionary_index_bit_width = 0;
      column.native_read_value_buffer_kind = NativeValueBufferKind::none;
      column.native_read_arrow_length = 0;
      column.native_read_arrow_null_count = 0;
      column.native_read_arrow_n_buffers = 0;
      column.native_read_arrow_n_children = 0;
      column.native_read_has_validity_buffer = 0;
      column.native_read_has_offsets_buffer = 0;
      column.native_read_has_values_buffer = 0;
      column.native_read_page_spans.clear();
      column.repeated_level_layouts.clear();
      if (column.max_repetition_level != 0) {
        SAN_RETURN_NOT_OK(assign_simple_list_level_layout(
            row_group.has_num_rows ? row_group.num_rows : -1, &column));
        continue;
      }
      bool complete = true;
      bool saw_data_page = false;
      std::int64_t cumulative_rows = 0;
      std::size_t data_page_index = 0;
      column.native_read_page_spans.reserve(column.pages.size());
      for (std::size_t page_index = 0; page_index < column.pages.size();
           ++page_index) {
        const auto &page = column.pages[page_index];
        if (page.is_dictionary_page) {
          continue;
        }
        saw_data_page = true;
        if (!page.has_num_values || !page.levels_decoded ||
            !page.validity_bitmap_decoded || !page.has_value_encoding ||
            !page.has_compressed_page_size || !page.values_decoded ||
            page.values_decode_skipped) {
          complete = false;
          break;
        }
        const auto value_buffer_kind = value_buffer_kind_for_page(column, page);
        if (value_buffer_kind == NativeValueBufferKind::none) {
          complete = false;
          break;
        }
        const auto value_width_bytes = value_width_bytes_for_page(column, page);
        if (column.native_read_value_buffer_kind ==
            NativeValueBufferKind::none) {
          column.native_read_value_buffer_kind = value_buffer_kind;
        } else if (column.native_read_value_buffer_kind != value_buffer_kind) {
          complete = false;
          break;
        }
        if (column.native_read_value_width_bytes == 0) {
          column.native_read_value_width_bytes = value_width_bytes;
        } else if (value_width_bytes != 0 &&
                   column.native_read_value_width_bytes != value_width_bytes) {
          complete = false;
          break;
        }
        if (column.native_read_dictionary_index_bit_width == 0) {
          column.native_read_dictionary_index_bit_width =
              page.dictionary_index_bit_width;
        } else if (page.dictionary_index_bit_width != 0 &&
                   column.native_read_dictionary_index_bit_width !=
                       page.dictionary_index_bit_width) {
          complete = false;
          break;
        }
        SAN_RETURN_NOT_OK(add_i64_checked(&column.native_read_total_rows,
                                          page.num_values, "row count"));
        SAN_RETURN_NOT_OK(add_i64_checked(&column.native_read_total_non_nulls,
                                          page.decoded_non_null_values,
                                          "non-null count"));
        SAN_RETURN_NOT_OK(add_i64_checked(&column.native_read_total_nulls,
                                          page.decoded_null_values,
                                          "null count"));
        SAN_RETURN_NOT_OK(add_i64_checked(
            &column.native_read_validity_bitmap_bytes,
            page.decoded_validity_bytes, "validity bitmap bytes"));
        SAN_RETURN_NOT_OK(
            add_i64_checked(&column.native_read_value_payload_bytes,
                            page.decoded_value_bytes, "value payload bytes"));
        SAN_RETURN_NOT_OK(add_i64_checked(
            &column.native_read_materialized_value_bytes,
            page.materialized_value_bytes, "materialized value bytes"));
        SAN_RETURN_NOT_OK(add_i64_checked(
            &column.native_read_materialized_offset_bytes,
            page.materialized_offset_bytes, "materialized offset bytes"));
        std::int64_t first_row_index = cumulative_rows;
        if (column.offset_index.decoded &&
            data_page_index < column.offset_index.locations.size()) {
          first_row_index =
              column.offset_index.locations[data_page_index].first_row_index;
        }
        column.native_read_page_spans.push_back(NativeReadPageSpanInfo{
            .page_index = static_cast<std::int32_t>(page_index),
            .first_row_index = first_row_index,
            .row_count = page.num_values,
            .non_null_count = page.decoded_non_null_values,
            .null_count = page.decoded_null_values,
            .value_encoding = page.value_encoding,
            .payload_offset = page.compressed_payload_offset,
            .payload_size = page.compressed_page_size,
            .validity_bitmap_bytes = page.decoded_validity_bytes,
            .value_payload_offset = page.value_payload_offset,
            .value_payload_bytes = page.decoded_value_bytes,
            .value_width_bytes = value_width_bytes,
            .materialized_value_bytes = page.materialized_value_bytes,
            .materialized_offset_bytes = page.materialized_offset_bytes,
            .dictionary_index_bit_width = page.dictionary_index_bit_width,
            .value_buffer_kind = value_buffer_kind,
        });
        cumulative_rows += page.num_values;
        ++column.native_read_data_page_count;
        ++data_page_index;
      }
      if (column.has_num_values && cumulative_rows != column.num_values) {
        complete = false;
      }
      if (!saw_data_page && column.has_num_values && column.num_values > 0) {
        complete = false;
      }
      if (complete) {
        const auto arrow_buffer_count = arrow_buffer_count_for_value_kind(
            column.native_read_value_buffer_kind);
        if (arrow_buffer_count == 0) {
          complete = false;
        } else {
          column.native_read_arrow_length = column.native_read_total_rows;
          column.native_read_arrow_null_count = column.native_read_total_nulls;
          column.native_read_arrow_n_buffers = arrow_buffer_count;
          column.native_read_arrow_n_children = 0;
          column.native_read_has_validity_buffer =
              column.native_read_total_nulls > 0 ? 1 : 0;
          column.native_read_has_offsets_buffer =
              column.native_read_materialized_offset_bytes > 0 ? 1 : 0;
          column.native_read_has_values_buffer = 1;
        }
      }
      column.native_read_plan_decoded = complete;
    }
  }
  return {};
}

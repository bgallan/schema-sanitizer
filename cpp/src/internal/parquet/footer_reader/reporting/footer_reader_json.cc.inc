// JSON helpers for public footer diagnostics.

void append_footer_schema_elements_json(std::string &out, bool &first,
                                        const FooterInfo &info) {
  json_encoding::append_key(out, first, "schema_elements");
  out.push_back('[');
  for (std::size_t i = 0; i < info.schema_elements.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &element = info.schema_elements[i];
    out.push_back('{');
    bool element_first = true;
    json_encoding::append_string_field(out, element_first, "name",
                                       element.name);
    if (element.has_physical_type) {
      json_encoding::append_int_field(out, element_first, "physical_type",
                                      element.physical_type);
    }
    if (element.has_type_length) {
      json_encoding::append_int_field(out, element_first, "type_length",
                                      element.type_length);
    }
    if (element.has_repetition_type) {
      json_encoding::append_int_field(out, element_first, "repetition_type",
                                      element.repetition_type);
    }
    if (element.has_num_children) {
      json_encoding::append_int_field(out, element_first, "num_children",
                                      element.num_children);
    }
    if (element.has_converted_type) {
      json_encoding::append_int_field(out, element_first, "converted_type",
                                      element.converted_type);
    }
    if (element.has_decimal_scale) {
      json_encoding::append_int_field(out, element_first, "decimal_scale",
                                      element.decimal_scale);
    }
    if (element.has_decimal_precision) {
      json_encoding::append_int_field(out, element_first, "decimal_precision",
                                      element.decimal_precision);
    }
    if (!element.logical_type.empty()) {
      json_encoding::append_string_field(out, element_first, "logical_type",
                                         element.logical_type);
    }
    if (!element.logical_type_time_unit.empty()) {
      json_encoding::append_string_field(out, element_first,
                                         "logical_type_time_unit",
                                         element.logical_type_time_unit);
    }
    if (element.has_logical_type_is_adjusted_to_utc) {
      json_encoding::append_int_field(
          out, element_first, "logical_type_is_adjusted_to_utc",
          element.logical_type_is_adjusted_to_utc ? 1 : 0);
    }
    if (element.has_logical_type_integer_bit_width) {
      json_encoding::append_int_field(out, element_first,
                                      "logical_type_integer_bit_width",
                                      element.logical_type_integer_bit_width);
    }
    if (element.has_logical_type_integer_is_signed) {
      json_encoding::append_int_field(
          out, element_first, "logical_type_integer_is_signed",
          element.logical_type_integer_is_signed ? 1 : 0);
    }
    out.push_back('}');
  }
  out.push_back(']');
}
void append_footer_column_metadata_json(std::string &out, bool &column_first,
                                        const ColumnChunkInfo &column) {
  json_encoding::append_key(out, column_first, "path_in_schema");
  append_string_array(out, column.path_in_schema);
  if (column.has_physical_type) {
    json_encoding::append_int_field(out, column_first, "physical_type",
                                    column.physical_type);
  }
  json_encoding::append_key(out, column_first, "encodings");
  append_int_array(out, column.encodings);
  if (column.has_codec) {
    json_encoding::append_int_field(out, column_first, "codec", column.codec);
  }
  if (column.has_num_values) {
    json_encoding::append_int_field(out, column_first, "num_values",
                                    column.num_values);
  }
  if (column.has_total_uncompressed_size) {
    json_encoding::append_int_field(out, column_first,
                                    "total_uncompressed_size",
                                    column.total_uncompressed_size);
  }
  if (column.has_total_compressed_size) {
    json_encoding::append_int_field(out, column_first, "total_compressed_size",
                                    column.total_compressed_size);
  }
  if (column.has_file_offset) {
    json_encoding::append_int_field(out, column_first, "file_offset",
                                    column.file_offset);
  }
  if (column.has_data_page_offset) {
    json_encoding::append_int_field(out, column_first, "data_page_offset",
                                    column.data_page_offset);
  }
  if (column.has_dictionary_page_offset) {
    json_encoding::append_int_field(out, column_first, "dictionary_page_offset",
                                    column.dictionary_page_offset);
  }
  if (column.has_column_index_offset) {
    json_encoding::append_int_field(out, column_first, "column_index_offset",
                                    column.column_index_offset);
  }
  if (column.has_column_index_length) {
    json_encoding::append_int_field(out, column_first, "column_index_length",
                                    column.column_index_length);
  }
  if (column.has_offset_index_offset) {
    json_encoding::append_int_field(out, column_first, "offset_index_offset",
                                    column.offset_index_offset);
  }
  if (column.has_offset_index_length) {
    json_encoding::append_int_field(out, column_first, "offset_index_length",
                                    column.offset_index_length);
  }
  json_encoding::append_int_field(out, column_first, "max_definition_level",
                                  column.max_definition_level);
  json_encoding::append_int_field(out, column_first, "max_repetition_level",
                                  column.max_repetition_level);
  json_encoding::append_int_field(out, column_first, "top_level_required",
                                  column.top_level_required ? 1 : 0);
  json_encoding::append_key(out, column_first, "path_definition_levels");
  append_int16_array(out, column.path_definition_levels);
  json_encoding::append_key(out, column_first, "path_repetition_levels");
  append_int16_array(out, column.path_repetition_levels);
  if (column.fixed_type_length > 0) {
    json_encoding::append_int_field(out, column_first, "fixed_type_length",
                                    column.fixed_type_length);
  }
  if (!column.native_arrow_format.empty()) {
    json_encoding::append_string_field(out, column_first, "native_arrow_format",
                                       column.native_arrow_format);
  }
  json_encoding::append_int_field(out, column_first, "column_index_decoded",
                                  column.column_index.decoded ? 1 : 0);
  if (column.column_index.decoded) {
    json_encoding::append_key(out, column_first, "column_index_null_pages");
    append_bool_array(out, column.column_index.null_pages);
    json_encoding::append_key(out, column_first, "column_index_min_hex");
    append_hex_string_array(out, column.column_index.min_values);
    json_encoding::append_key(out, column_first, "column_index_max_hex");
    append_hex_string_array(out, column.column_index.max_values);
    if (column.column_index.has_boundary_order) {
      json_encoding::append_int_field(out, column_first,
                                      "column_index_boundary_order",
                                      column.column_index.boundary_order);
    }
    json_encoding::append_key(out, column_first, "column_index_null_counts");
    append_int64_array(out, column.column_index.null_counts);
  }
  json_encoding::append_int_field(out, column_first, "offset_index_decoded",
                                  column.offset_index.decoded ? 1 : 0);
  if (column.offset_index.decoded) {
    json_encoding::append_key(out, column_first, "offset_index_locations");
    append_page_locations(out, column.offset_index.locations);
  }
  json_encoding::append_int_field(out, column_first, "native_read_plan_decoded",
                                  column.native_read_plan_decoded ? 1 : 0);
  if (column.native_read_plan_decoded) {
    json_encoding::append_int_field(out, column_first,
                                    "native_read_data_page_count",
                                    column.native_read_data_page_count);
    json_encoding::append_int_field(out, column_first, "native_read_total_rows",
                                    column.native_read_total_rows);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_total_non_nulls",
                                    column.native_read_total_non_nulls);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_total_nulls",
                                    column.native_read_total_nulls);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_validity_bitmap_bytes",
                                    column.native_read_validity_bitmap_bytes);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_value_payload_bytes",
                                    column.native_read_value_payload_bytes);
    json_encoding::append_int_field(
        out, column_first, "native_read_materialized_value_bytes",
        column.native_read_materialized_value_bytes);
    json_encoding::append_int_field(
        out, column_first, "native_read_materialized_offset_bytes",
        column.native_read_materialized_offset_bytes);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_value_width_bytes",
                                    column.native_read_value_width_bytes);
    json_encoding::append_int_field(
        out, column_first, "native_read_dictionary_index_bit_width",
        column.native_read_dictionary_index_bit_width);
    json_encoding::append_string_field(
        out, column_first, "native_read_value_buffer_kind",
        native_value_buffer_kind_name(column.native_read_value_buffer_kind));
    json_encoding::append_int_field(out, column_first,
                                    "native_read_arrow_length",
                                    column.native_read_arrow_length);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_arrow_null_count",
                                    column.native_read_arrow_null_count);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_arrow_n_buffers",
                                    column.native_read_arrow_n_buffers);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_arrow_n_children",
                                    column.native_read_arrow_n_children);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_has_validity_buffer",
                                    column.native_read_has_validity_buffer);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_has_offsets_buffer",
                                    column.native_read_has_offsets_buffer);
    json_encoding::append_int_field(out, column_first,
                                    "native_read_has_values_buffer",
                                    column.native_read_has_values_buffer);
    json_encoding::append_key(out, column_first, "native_read_page_spans");
    append_native_read_page_spans(out, column.native_read_page_spans);
  }
  json_encoding::append_key(out, column_first, "repeated_level_layouts");
  append_repeated_level_layout_infos(out, column.repeated_level_layouts);
}
void append_footer_column_pages_json(std::string &out, bool &column_first,
                                     const ColumnChunkInfo &column) {
  json_encoding::append_key(out, column_first, "pages");
  out.push_back('[');
  for (std::size_t k = 0; k < column.pages.size(); ++k) {
    if (k > 0) {
      out.push_back(',');
    }
    const auto &page = column.pages[k];
    out.push_back('{');
    bool page_first = true;
    json_encoding::append_int_field(out, page_first, "header_offset",
                                    page.header_offset);
    json_encoding::append_int_field(out, page_first, "header_size",
                                    page.header_size);
    json_encoding::append_int_field(out, page_first,
                                    "compressed_payload_offset",
                                    page.compressed_payload_offset);
    if (page.has_type) {
      json_encoding::append_int_field(out, page_first, "type", page.type);
    }
    if (page.has_uncompressed_page_size) {
      json_encoding::append_int_field(out, page_first, "uncompressed_page_size",
                                      page.uncompressed_page_size);
    }
    if (page.has_compressed_page_size) {
      json_encoding::append_int_field(out, page_first, "compressed_page_size",
                                      page.compressed_page_size);
    }
    json_encoding::append_int_field(out, page_first, "has_crc",
                                    page.has_crc ? 1 : 0);
    if (page.has_crc) {
      json_encoding::append_int_field(out, page_first, "crc", page.crc);
    }
    if (page.has_num_values) {
      json_encoding::append_int_field(out, page_first, "num_values",
                                      page.num_values);
    }
    if (page.has_value_encoding) {
      json_encoding::append_int_field(out, page_first, "value_encoding",
                                      page.value_encoding);
    }
    if (page.has_definition_level_encoding) {
      json_encoding::append_int_field(out, page_first,
                                      "definition_level_encoding",
                                      page.definition_level_encoding);
    }
    if (page.has_repetition_level_encoding) {
      json_encoding::append_int_field(out, page_first,
                                      "repetition_level_encoding",
                                      page.repetition_level_encoding);
    }
    if (page.has_decompressed_page_size) {
      json_encoding::append_int_field(out, page_first, "decompressed_page_size",
                                      page.decompressed_page_size);
    }
    json_encoding::append_int_field(out, page_first, "payload_verified",
                                    page.payload_verified ? 1 : 0);
    json_encoding::append_int_field(out, page_first,
                                    "payload_verification_skipped",
                                    page.payload_verification_skipped ? 1 : 0);
    json_encoding::append_int_field(out, page_first, "levels_decoded",
                                    page.levels_decoded ? 1 : 0);
    if (page.levels_decoded) {
      json_encoding::append_int_field(out, page_first,
                                      "decoded_definition_levels",
                                      page.decoded_definition_levels);
      if (!page.decoded_definition_level_values.empty()) {
        json_encoding::append_key(out, page_first,
                                  "decoded_definition_level_values");
        append_int16_array(out, page.decoded_definition_level_values);
      }
      json_encoding::append_int_field(out, page_first,
                                      "decoded_repetition_levels",
                                      page.decoded_repetition_levels);
      if (!page.decoded_repetition_level_values.empty()) {
        json_encoding::append_key(out, page_first,
                                  "decoded_repetition_level_values");
        append_int16_array(out, page.decoded_repetition_level_values);
      }
      json_encoding::append_int_field(out, page_first, "value_payload_offset",
                                      page.value_payload_offset);
      json_encoding::append_int_field(out, page_first,
                                      "decoded_non_null_values",
                                      page.decoded_non_null_values);
      json_encoding::append_int_field(out, page_first, "decoded_null_values",
                                      page.decoded_null_values);
      json_encoding::append_int_field(out, page_first,
                                      "validity_bitmap_decoded",
                                      page.validity_bitmap_decoded ? 1 : 0);
      if (page.validity_bitmap_decoded) {
        json_encoding::append_int_field(out, page_first,
                                        "decoded_validity_bytes",
                                        page.decoded_validity_bytes);
        json_encoding::append_string_field(
            out, page_first, "decoded_validity_hex_preview",
            hex_bytes_preview(page.decoded_validity_bitmap));
      }
    }
    json_encoding::append_int_field(out, page_first, "values_decoded",
                                    page.values_decoded ? 1 : 0);
    json_encoding::append_int_field(out, page_first, "values_decode_skipped",
                                    page.values_decode_skipped ? 1 : 0);
    if (page.values_decoded) {
      json_encoding::append_int_field(out, page_first, "decoded_value_bytes",
                                      page.decoded_value_bytes);
      json_encoding::append_int_field(out, page_first,
                                      "materialized_value_bytes",
                                      page.materialized_value_bytes);
      json_encoding::append_int_field(out, page_first,
                                      "materialized_offset_bytes",
                                      page.materialized_offset_bytes);
      json_encoding::append_int_field(out, page_first,
                                      "dictionary_index_bit_width",
                                      page.dictionary_index_bit_width);
      json_encoding::append_key(out, page_first, "decoded_value_preview");
      append_string_array(out, page.decoded_value_preview);
    }
    json_encoding::append_int_field(out, page_first, "is_dictionary_page",
                                    page.is_dictionary_page ? 1 : 0);
    if (page.is_dictionary_page) {
      json_encoding::append_int_field(out, page_first, "dictionary_is_sorted",
                                      page.dictionary_is_sorted ? 1 : 0);
    }
    out.push_back('}');
  }
  out.push_back(']');
}
void append_footer_row_groups_json(std::string &out, bool &first,
                                   const FooterInfo &info) {
  json_encoding::append_key(out, first, "row_groups");
  out.push_back('[');
  for (std::size_t i = 0; i < info.row_groups.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &row_group = info.row_groups[i];
    out.push_back('{');
    bool row_group_first = true;
    if (row_group.has_num_rows) {
      json_encoding::append_int_field(out, row_group_first, "num_rows",
                                      row_group.num_rows);
    }
    if (row_group.has_total_byte_size) {
      json_encoding::append_int_field(out, row_group_first, "total_byte_size",
                                      row_group.total_byte_size);
    }
    json_encoding::append_key(out, row_group_first, "columns");
    out.push_back('[');
    for (std::size_t j = 0; j < row_group.columns.size(); ++j) {
      if (j > 0) {
        out.push_back(',');
      }
      const auto &column = row_group.columns[j];
      out.push_back('{');
      bool column_first = true;
      append_footer_column_metadata_json(out, column_first, column);
      append_footer_column_pages_json(out, column_first, column);
      out.push_back('}');
    }
    out.push_back(']');
    json_encoding::append_key(out, row_group_first,
                              "native_recursive_output_layout");
    append_native_recursive_output_layout_diagnostics(out, row_group);
    out.push_back('}');
  }
  out.push_back(']');
}

void append_int_array(std::string &out,
                      const std::vector<std::int32_t> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    out += std::to_string(items[i]);
  }
  out.push_back(']');
}

void append_int16_array(std::string &out,
                        const std::vector<std::int16_t> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    out += std::to_string(items[i]);
  }
  out.push_back(']');
}

void append_int16_matrix(std::string &out,
                         const std::vector<std::vector<std::int16_t>> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    append_int16_array(out, items[i]);
  }
  out.push_back(']');
}

void append_int64_array(std::string &out,
                        const std::vector<std::int64_t> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    out += std::to_string(items[i]);
  }
  out.push_back(']');
}

void append_bool_array(std::string &out, const std::vector<bool> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    out += items[i] ? "true" : "false";
  }
  out.push_back(']');
}

void append_string_array(std::string &out,
                         const std::vector<std::string> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    json_encoding::append_string(out, items[i]);
  }
  out.push_back(']');
}

void append_string_matrix(std::string &out,
                          const std::vector<std::vector<std::string>> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    append_string_array(out, items[i]);
  }
  out.push_back(']');
}

void append_repeated_level_layout_infos(
    std::string &out, const std::vector<RepeatedLevelLayoutInfo> &layouts) {
  out.push_back('[');
  for (std::size_t i = 0; i < layouts.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &layout = layouts[i];
    out.push_back('{');
    bool first = true;
    json_encoding::append_int_field(out, first, "layout_index",
                                    static_cast<std::int64_t>(i));
    json_encoding::append_int_field(out, first, "decoded",
                                    layout.decoded ? 1 : 0);
    json_encoding::append_int_field(out, first, "row_count", layout.row_count);
    json_encoding::append_int_field(out, first, "null_count",
                                    layout.null_count);
    json_encoding::append_int_field(out, first, "element_count",
                                    layout.element_count);
    json_encoding::append_int_field(out, first, "non_null_value_count",
                                    layout.non_null_value_count);
    json_encoding::append_key(out, first, "offsets");
    append_int_array(out, layout.offsets);
    json_encoding::append_string_field(
        out, first, "validity_hex_preview",
        hex_bytes_preview(layout.validity_bitmap));
    out.push_back('}');
  }
  out.push_back(']');
}

void append_hex_string_array(std::string &out,
                             const std::vector<std::string> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    json_encoding::append_string(out, hex_bytes(items[i]));
  }
  out.push_back(']');
}

void append_page_locations(std::string &out,
                           const std::vector<PageLocationInfo> &locations) {
  out.push_back('[');
  for (std::size_t i = 0; i < locations.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &location = locations[i];
    out.push_back('{');
    bool first = true;
    json_encoding::append_int_field(out, first, "offset", location.offset);
    json_encoding::append_int_field(out, first, "compressed_page_size",
                                    location.compressed_page_size);
    json_encoding::append_int_field(out, first, "first_row_index",
                                    location.first_row_index);
    out.push_back('}');
  }
  out.push_back(']');
}

void append_native_read_page_spans(
    std::string &out, const std::vector<NativeReadPageSpanInfo> &spans) {
  out.push_back('[');
  for (std::size_t i = 0; i < spans.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &span = spans[i];
    out.push_back('{');
    bool first = true;
    json_encoding::append_int_field(out, first, "page_index", span.page_index);
    json_encoding::append_int_field(out, first, "first_row_index",
                                    span.first_row_index);
    json_encoding::append_int_field(out, first, "row_count", span.row_count);
    json_encoding::append_int_field(out, first, "non_null_count",
                                    span.non_null_count);
    json_encoding::append_int_field(out, first, "null_count", span.null_count);
    json_encoding::append_int_field(out, first, "value_encoding",
                                    span.value_encoding);
    json_encoding::append_int_field(out, first, "payload_offset",
                                    span.payload_offset);
    json_encoding::append_int_field(out, first, "payload_size",
                                    span.payload_size);
    json_encoding::append_int_field(out, first, "validity_bitmap_bytes",
                                    span.validity_bitmap_bytes);
    json_encoding::append_int_field(out, first, "value_payload_offset",
                                    span.value_payload_offset);
    json_encoding::append_int_field(out, first, "value_payload_bytes",
                                    span.value_payload_bytes);
    json_encoding::append_int_field(out, first, "value_width_bytes",
                                    span.value_width_bytes);
    json_encoding::append_int_field(out, first, "materialized_value_bytes",
                                    span.materialized_value_bytes);
    json_encoding::append_int_field(out, first, "materialized_offset_bytes",
                                    span.materialized_offset_bytes);
    json_encoding::append_int_field(out, first, "dictionary_index_bit_width",
                                    span.dictionary_index_bit_width);
    json_encoding::append_string_field(
        out, first, "value_buffer_kind",
        native_value_buffer_kind_name(span.value_buffer_kind));
    out.push_back('}');
  }
  out.push_back(']');
}

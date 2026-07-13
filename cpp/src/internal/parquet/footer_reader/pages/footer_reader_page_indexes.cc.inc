sanitize::Result<ColumnIndexInfo> parse_column_index(std::string_view bytes) {
  CompactReader reader(bytes);
  ColumnIndexInfo index;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      index.decoded = true;
      return index;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    switch (field_id) {
    case 1:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(index.null_pages, read_bool_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(index.min_values, read_string_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(index.max_values, read_string_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(index.boundary_order, reader.read_i32());
        index.has_boundary_order = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(index.null_counts, read_i64_list(reader));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Result<PageLocationInfo> read_page_location(CompactReader &reader) {
  PageLocationInfo location;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return location;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    switch (field_id) {
    case 1:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(location.offset, reader.read_i64());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(location.compressed_page_size, reader.read_i32());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(location.first_row_index, reader.read_i64());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    default:
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      break;
    }
  }
}

sanitize::Result<OffsetIndexInfo> parse_offset_index(std::string_view bytes) {
  CompactReader reader(bytes);
  OffsetIndexInfo index;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      index.decoded = true;
      return index;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    if (field_id == 1 && type == kTypeList) {
      std::uint8_t element_type = 0;
      std::uint64_t count = 0;
      SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
      index.locations.reserve(static_cast<std::size_t>(count));
      for (std::uint64_t i = 0; i < count; ++i) {
        if (element_type != kTypeStruct) {
          SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
          continue;
        }
        PageLocationInfo location;
        SAN_ASSIGN_OR_RAISE(location, read_page_location(reader));
        index.locations.push_back(location);
      }
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

auto parquet_data_pages(const ColumnChunkInfo &column) {
  return column.pages | std::views::filter([](const PageHeaderInfo &page) {
           return !page.is_dictionary_page;
         });
}

sanitize::Status validate_column_index(const ColumnChunkInfo &column) {
  if (!column.column_index.decoded) {
    return {};
  }
  auto pages = parquet_data_pages(column);
  const auto count = static_cast<std::size_t>(std::ranges::distance(pages));
  if (column.column_index.null_pages.size() != count ||
      column.column_index.min_values.size() != count ||
      column.column_index.max_values.size() != count ||
      column.column_index.null_counts.size() != count) {
    return sanitize::Status::Invalid(
        "Parquet column index: page count mismatch");
  }
  std::size_t index = 0;
  for (const auto &page : pages) {
    if (page.has_num_values &&
        column.column_index.null_counts[index] > page.num_values) {
      return sanitize::Status::Invalid(
          "Parquet column index: null count exceeds page values");
    }
    const bool expected_null_page =
        page.has_num_values &&
        column.column_index.null_counts[index] == page.num_values &&
        page.num_values > 0;
    if (column.column_index.null_pages[index] != expected_null_page) {
      return sanitize::Status::Invalid(
          "Parquet column index: null page flag mismatch");
    }
    ++index;
  }
  return {};
}

sanitize::Status validate_offset_index(const ColumnChunkInfo &column) {
  if (!column.offset_index.decoded) {
    return {};
  }
  auto pages = parquet_data_pages(column);
  if (column.offset_index.locations.size() !=
      static_cast<std::size_t>(std::ranges::distance(pages))) {
    return sanitize::Status::Invalid(
        "Parquet offset index: page count mismatch");
  }
  std::int64_t expected_first_row = 0;
  std::size_t index = 0;
  for (const auto &page : pages) {
    const auto &location = column.offset_index.locations[index];
    const auto expected_size =
        static_cast<std::int64_t>(page.header_size) + page.compressed_page_size;
    if (location.offset != page.header_offset ||
        location.compressed_page_size != expected_size) {
      return sanitize::Status::Invalid(
          "Parquet offset index: page location mismatch");
    }
    if (location.first_row_index != expected_first_row) {
      return sanitize::Status::Invalid(
          "Parquet offset index: first row index mismatch");
    }
    if (page.has_num_values) {
      expected_first_row += page.num_values;
    }
    ++index;
  }
  return {};
}

sanitize::Status read_page_indexes(std::ifstream &file, FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet page index: internal error");
  }
  for (auto &row_group : info->row_groups) {
    for (auto &column : row_group.columns) {
      if (column.has_column_index_offset && column.has_column_index_length &&
          column.column_index_length > 0) {
        std::string bytes;
        SAN_ASSIGN_OR_RAISE(bytes,
                            read_exact_payload(file, column.column_index_offset,
                                               column.column_index_length));
        SAN_ASSIGN_OR_RAISE(column.column_index, parse_column_index(bytes));
        SAN_RETURN_NOT_OK(validate_column_index(column));
      }
      if (column.has_offset_index_offset && column.has_offset_index_length &&
          column.offset_index_length > 0) {
        std::string bytes;
        SAN_ASSIGN_OR_RAISE(bytes,
                            read_exact_payload(file, column.offset_index_offset,
                                               column.offset_index_length));
        SAN_ASSIGN_OR_RAISE(column.offset_index, parse_offset_index(bytes));
        SAN_RETURN_NOT_OK(validate_offset_index(column));
      }
    }
  }
  return {};
}

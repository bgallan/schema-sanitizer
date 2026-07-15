sanitize::Result<ColumnChunkInfo> read_column_chunk(CompactReader &reader) {
  ColumnChunkInfo column;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return column;
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
    case 2:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.file_offset, reader.read_i64());
        column.has_file_offset = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeStruct) {
        SAN_ASSIGN_OR_RAISE(column, read_column_metadata(reader, column));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.offset_index_offset, reader.read_i64());
        column.has_offset_index_offset = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(column.offset_index_length, reader.read_i32());
        column.has_offset_index_length = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 6:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.column_index_offset, reader.read_i64());
        column.has_column_index_offset = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 7:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(column.column_index_length, reader.read_i32());
        column.has_column_index_length = true;
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

sanitize::Result<RowGroupInfo> read_row_group(CompactReader &reader) {
  RowGroupInfo row_group;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return row_group;
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
        std::uint8_t element_type = 0;
        std::uint64_t count = 0;
        SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
        if (count > kMaxColumnsPerRowGroup) {
          return sanitize::Status::Invalid(
              "Parquet footer: column count exceeds security limit");
        }
        SAN_RETURN_NOT_OK(validate_metadata_list(
            reader, count, kMaxColumnsPerRowGroup, sizeof(ColumnChunkInfo),
            "row-group columns"));
        row_group.columns.clear();
        row_group.columns.reserve(static_cast<std::size_t>(count));
        for (std::uint64_t i = 0; i < count; ++i) {
          if (element_type != kTypeStruct) {
            SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
            continue;
          }
          ColumnChunkInfo column;
          SAN_ASSIGN_OR_RAISE(column, read_column_chunk(reader));
          row_group.columns.push_back(std::move(column));
        }
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(row_group.total_byte_size, reader.read_i64());
        row_group.has_total_byte_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(row_group.num_rows, reader.read_i64());
        row_group.has_num_rows = true;
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

sanitize::Status read_row_groups(CompactReader &reader, FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid(
        "Parquet footer: internal row group error");
  }
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  if (count > kMaxRowGroups ||
      count > static_cast<std::uint64_t>(
                  std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet footer: row group count exceeds security limit");
  }
  SAN_RETURN_NOT_OK(validate_metadata_list(
      reader, count, kMaxRowGroups, sizeof(RowGroupInfo), "row groups"));
  info->row_group_count = static_cast<std::int32_t>(count);
  info->row_groups.clear();
  info->row_groups.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeStruct) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    RowGroupInfo row_group;
    SAN_ASSIGN_OR_RAISE(row_group, read_row_group(reader));
    info->row_groups.push_back(std::move(row_group));
  }
  return {};
}
sanitize::Result<FooterInfo> parse_footer(std::string_view footer) {
  CompactReader reader(footer);
  FooterInfo info;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return info;
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
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(info.version, reader.read_i32());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeList) {
        SAN_RETURN_NOT_OK(read_schema_elements(reader, &info));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(info.num_rows, reader.read_i64());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeList) {
        SAN_RETURN_NOT_OK(read_row_groups(reader, &info));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 6:
      if (type == kTypeBinary) {
        SAN_ASSIGN_OR_RAISE(info.created_by, reader.read_binary());
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

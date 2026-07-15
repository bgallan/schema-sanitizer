// Parses primitive lists and Parquet column metadata from compact Thrift.

sanitize::Result<std::vector<std::int32_t>>
read_i32_list(CompactReader &reader, std::uint64_t max_count,
              std::string_view label) {
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  SAN_RETURN_NOT_OK(validate_metadata_list(reader, count, max_count,
                                            sizeof(std::int32_t), label));
  std::vector<std::int32_t> out;
  out.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeI32) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    std::int32_t value = 0;
    SAN_ASSIGN_OR_RAISE(value, reader.read_i32());
    out.push_back(value);
  }
  return out;
}

sanitize::Result<std::vector<std::int64_t>>
read_i64_list(CompactReader &reader, std::uint64_t max_count,
              std::string_view label) {
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  SAN_RETURN_NOT_OK(validate_metadata_list(reader, count, max_count,
                                            sizeof(std::int64_t), label));
  std::vector<std::int64_t> out;
  out.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeI64) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    std::int64_t value = 0;
    SAN_ASSIGN_OR_RAISE(value, reader.read_i64());
    out.push_back(value);
  }
  return out;
}

sanitize::Result<std::vector<bool>>
read_bool_list(CompactReader &reader, std::uint64_t max_count,
               std::string_view label) {
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  // Charge one byte per element conservatively even though vector<bool> packs
  // bits; this leaves headroom for implementation-specific allocation.
  SAN_RETURN_NOT_OK(validate_metadata_list(reader, count, max_count, 1, label));
  std::vector<bool> out;
  out.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeBoolTrue && element_type != kTypeBoolFalse) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    std::uint8_t value = 0;
    SAN_ASSIGN_OR_RAISE(value, reader.read_byte());
    if (value == kTypeBoolTrue) {
      out.push_back(true);
    } else if (value == kTypeBoolFalse) {
      out.push_back(false);
    } else {
      return sanitize::Status::Invalid(
          "Parquet index: invalid bool list value");
    }
  }
  return out;
}

sanitize::Result<std::vector<std::string>>
read_string_list(CompactReader &reader, std::uint64_t max_count,
                 std::string_view label) {
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  SAN_RETURN_NOT_OK(validate_metadata_list(reader, count, max_count,
                                            sizeof(std::string), label));
  std::vector<std::string> out;
  out.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeBinary) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    std::string value;
    SAN_ASSIGN_OR_RAISE(value, reader.read_binary());
    out.push_back(std::move(value));
  }
  return out;
}

sanitize::Result<ColumnChunkInfo> read_column_metadata(CompactReader &reader,
                                                       ColumnChunkInfo column) {
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
    case 1:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(column.physical_type, reader.read_i32());
        column.has_physical_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(
            column.encodings,
            read_i32_list(reader, kMaxColumnEncodingEntries, "encoding list"));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeList) {
        SAN_ASSIGN_OR_RAISE(
            column.path_in_schema,
            read_string_list(reader, kMaxSchemaPathEntries, "schema path"));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(column.codec, reader.read_i32());
        column.has_codec = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.num_values, reader.read_i64());
        column.has_num_values = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 6:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.total_uncompressed_size, reader.read_i64());
        column.has_total_uncompressed_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 7:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.total_compressed_size, reader.read_i64());
        column.has_total_compressed_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 9:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.data_page_offset, reader.read_i64());
        column.has_data_page_offset = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 11:
      if (type == kTypeI64) {
        SAN_ASSIGN_OR_RAISE(column.dictionary_page_offset, reader.read_i64());
        column.has_dictionary_page_offset = true;
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

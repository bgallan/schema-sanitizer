// Parses compact-Thrift data, dictionary, and enclosing page headers with
// structural validation. This fragment is included by the consolidated footer
// reader inside its private implementation namespace.

/// Parses one compact-Thrift DataPageHeader into the common page record.
sanitize::Result<PageHeaderInfo> read_data_page_header(CompactReader &reader,
                                                       PageHeaderInfo page) {
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return page;
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
        SAN_ASSIGN_OR_RAISE(page.num_values, reader.read_i32());
        page.has_num_values = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.value_encoding, reader.read_i32());
        page.has_value_encoding = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.definition_level_encoding, reader.read_i32());
        page.has_definition_level_encoding = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.repetition_level_encoding, reader.read_i32());
        page.has_repetition_level_encoding = true;
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

/// Parses one compact-Thrift DictionaryPageHeader into the common page record.
sanitize::Result<PageHeaderInfo>
read_dictionary_page_header(CompactReader &reader, PageHeaderInfo page) {
  page.is_dictionary_page = true;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return page;
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
        SAN_ASSIGN_OR_RAISE(page.num_values, reader.read_i32());
        page.has_num_values = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.value_encoding, reader.read_i32());
        page.has_value_encoding = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeBoolTrue || type == kTypeBoolFalse) {
        page.dictionary_is_sorted = type == kTypeBoolTrue;
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

/// Parses a complete compact-Thrift PageHeader from bounded bytes.
sanitize::Result<PageHeaderInfo> read_page_header(std::string_view bytes) {
  CompactReader reader(bytes);
  PageHeaderInfo page;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      if (!page.has_compressed_page_size) {
        return sanitize::Status::Invalid(
            "Parquet page header: missing compressed page size");
      }
      if (page.compressed_page_size < 0) {
        return sanitize::Status::Invalid(
            "Parquet page header: negative compressed page size");
      }
      if (reader.position() >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
        return sanitize::Status::Invalid(
            "Parquet page header: header is too large");
      }
      page.header_size = static_cast<std::int32_t>(reader.position());
      return page;
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
        SAN_ASSIGN_OR_RAISE(page.type, reader.read_i32());
        page.has_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.uncompressed_page_size, reader.read_i32());
        page.has_uncompressed_page_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(page.compressed_page_size, reader.read_i32());
        page.has_compressed_page_size = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeI32) {
        std::int32_t crc = 0;
        SAN_ASSIGN_OR_RAISE(crc, reader.read_i32());
        page.crc = static_cast<std::uint32_t>(crc);
        page.has_crc = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeStruct) {
        SAN_ASSIGN_OR_RAISE(page, read_data_page_header(reader, page));
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 7:
      if (type == kTypeStruct) {
        SAN_ASSIGN_OR_RAISE(page, read_dictionary_page_header(reader, page));
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

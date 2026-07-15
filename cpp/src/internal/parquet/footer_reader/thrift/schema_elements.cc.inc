// Parses Parquet logical types and schema elements from compact Thrift.

sanitize::Result<LogicalTypeReadInfo> read_logical_type(CompactReader &reader) {
  std::int16_t last_field_id = 0;
  LogicalTypeReadInfo out;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return out;
    }
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    std::int16_t field_id = 0;
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(field_id, reader.read_i16());
    } else {
      field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    last_field_id = field_id;
    if (out.name.empty()) {
      out.name = logical_type_name(field_id);
    }
    if (field_id == 5 && type == kTypeStruct) {
      SAN_ASSIGN_OR_RAISE(out, read_decimal_logical_type(reader, out));
    } else if ((field_id == 7 || field_id == 8) && type == kTypeStruct) {
      SAN_ASSIGN_OR_RAISE(out, read_temporal_logical_type(reader, out));
    } else if (field_id == 10 && type == kTypeStruct) {
      SAN_ASSIGN_OR_RAISE(out, read_integer_logical_type(reader, out));
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

sanitize::Result<SchemaElementInfo> read_schema_element(CompactReader &reader) {
  SchemaElementInfo out;
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, reader.read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop) {
      return out;
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
        SAN_ASSIGN_OR_RAISE(out.physical_type, reader.read_i32());
        out.has_physical_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 2:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.type_length, reader.read_i32());
        out.has_type_length = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 3:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.repetition_type, reader.read_i32());
        out.has_repetition_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 4:
      if (type == kTypeBinary) {
        SAN_ASSIGN_OR_RAISE(out.name, reader.read_binary());
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 5:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.num_children, reader.read_i32());
        out.has_num_children = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 6:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.converted_type, reader.read_i32());
        out.has_converted_type = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 7:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.decimal_scale, reader.read_i32());
        out.has_decimal_scale = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 8:
      if (type == kTypeI32) {
        SAN_ASSIGN_OR_RAISE(out.decimal_precision, reader.read_i32());
        out.has_decimal_precision = true;
      } else {
        SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
      }
      break;
    case 10:
      if (type == kTypeStruct) {
        LogicalTypeReadInfo logical;
        SAN_ASSIGN_OR_RAISE(logical, read_logical_type(reader));
        out.logical_type = std::move(logical.name);
        out.logical_type_time_unit = std::move(logical.time_unit);
        out.has_logical_type_is_adjusted_to_utc =
            logical.has_is_adjusted_to_utc;
        out.logical_type_is_adjusted_to_utc = logical.is_adjusted_to_utc;
        out.has_logical_type_integer_bit_width = logical.has_integer_bit_width;
        out.logical_type_integer_bit_width = logical.integer_bit_width;
        out.has_logical_type_integer_is_signed = logical.has_integer_is_signed;
        out.logical_type_integer_is_signed = logical.integer_is_signed;
        if (logical.has_decimal_scale) {
          out.decimal_scale = logical.decimal_scale;
          out.has_decimal_scale = true;
        }
        if (logical.has_decimal_precision) {
          out.decimal_precision = logical.decimal_precision;
          out.has_decimal_precision = true;
        }
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

sanitize::Status read_schema_elements(CompactReader &reader, FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet footer: internal schema error");
  }
  std::uint8_t element_type = 0;
  std::uint64_t count = 0;
  SAN_RETURN_NOT_OK(reader.read_list_header(&element_type, &count));
  if (count > kMaxSchemaElements ||
      count > static_cast<std::uint64_t>(
                  std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "Parquet footer: schema element count exceeds security limit");
  }
  SAN_RETURN_NOT_OK(validate_metadata_list(reader, count, kMaxSchemaElements,
                                           sizeof(SchemaElementInfo),
                                           "schema elements"));
  info->schema_element_count = static_cast<std::int32_t>(count);
  info->schema_elements.clear();
  info->schema_elements.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t i = 0; i < count; ++i) {
    if (element_type != kTypeStruct) {
      SAN_RETURN_NOT_OK(reader.skip_container_value(element_type, 0));
      continue;
    }
    SchemaElementInfo element;
    SAN_ASSIGN_OR_RAISE(element, read_schema_element(reader));
    info->schema_elements.push_back(std::move(element));
  }
  return {};
}

// Decodes decimal, temporal, integer, and other Parquet logical-type unions
// from compact Thrift. This fragment is included by the consolidated footer
// reader inside its private implementation namespace.

/// Returns the diagnostic Parquet logical-type label for a compact-Thrift field
/// identifier.
std::string logical_type_name(std::int16_t field_id) {
  switch (field_id) {
  case 1:
    return "string";
  case 2:
    return "map";
  case 3:
    return "list";
  case 4:
    return "enum";
  case 5:
    return "decimal";
  case 6:
    return "date";
  case 7:
    return "time";
  case 8:
    return "timestamp";
  case 10:
    return "integer";
  case 11:
    return "null";
  case 12:
    return "json";
  case 13:
    return "bson";
  case 14:
    return "uuid";
  case 15:
    return "float16";
  default:
    return "unknown";
  }
}

/// Parses the compact-Thrift TimeUnit union into a stable unit label.
sanitize::Result<std::string> read_time_unit(CompactReader &reader) {
  std::int16_t last_field_id = 0;
  std::string out;
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
    if (out.empty()) {
      switch (field_id) {
      case 1:
        out = "millis";
        break;
      case 2:
        out = "micros";
        break;
      case 3:
        out = "nanos";
        break;
      default:
        out = "unknown";
        break;
      }
    }
    SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
  }
}

/// Parses scale and precision from a compact-Thrift DecimalType structure.
sanitize::Result<LogicalTypeReadInfo>
read_decimal_logical_type(CompactReader &reader, LogicalTypeReadInfo out) {
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
    if (field_id == 1 && type == kTypeI32) {
      SAN_ASSIGN_OR_RAISE(out.decimal_scale, reader.read_i32());
      out.has_decimal_scale = true;
    } else if (field_id == 2 && type == kTypeI32) {
      SAN_ASSIGN_OR_RAISE(out.decimal_precision, reader.read_i32());
      out.has_decimal_precision = true;
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

/// Parses UTC adjustment and units from a compact-Thrift time or timestamp
/// annotation.
sanitize::Result<LogicalTypeReadInfo>
read_temporal_logical_type(CompactReader &reader, LogicalTypeReadInfo out) {
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
    if (field_id == 1 && (type == kTypeBoolTrue || type == kTypeBoolFalse)) {
      out.has_is_adjusted_to_utc = true;
      out.is_adjusted_to_utc = type == kTypeBoolTrue;
    } else if (field_id == 2 && type == kTypeStruct) {
      SAN_ASSIGN_OR_RAISE(out.time_unit, read_time_unit(reader));
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

/// Parses bit width and signedness from a compact-Thrift IntType structure.
sanitize::Result<LogicalTypeReadInfo>
read_integer_logical_type(CompactReader &reader, LogicalTypeReadInfo out) {
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
    if (field_id == 1 && type == kTypeByte) {
      std::uint8_t width = 0;
      SAN_ASSIGN_OR_RAISE(width, reader.read_byte());
      out.integer_bit_width = static_cast<std::int32_t>(width);
      out.has_integer_bit_width = true;
    } else if (field_id == 2 &&
               (type == kTypeBoolTrue || type == kTypeBoolFalse)) {
      out.has_integer_is_signed = true;
      out.integer_is_signed = type == kTypeBoolTrue;
    } else {
      SAN_RETURN_NOT_OK(reader.skip_type(type, 0));
    }
  }
}

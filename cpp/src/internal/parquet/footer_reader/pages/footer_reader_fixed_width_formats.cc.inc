std::optional<std::int32_t>
fixed_width_for_plain_values(const ColumnChunkInfo &column) {
  if (!column.has_physical_type) {
    return std::nullopt;
  }
  switch (column.physical_type) {
  case kPhysicalBoolean:
    return 0;
  case kPhysicalInt32:
  case kPhysicalFloat:
    return 4;
  case kPhysicalInt64:
  case kPhysicalDouble:
    return 8;
  case kPhysicalFixedLenByteArray:
    if (column.fixed_type_length <= 0) {
      return std::nullopt;
    }
    return column.fixed_type_length;
  default:
    return std::nullopt;
  }
}

bool starts_with(std::string_view value, std::string_view prefix) {
  return value.size() >= prefix.size() &&
         value.substr(0, prefix.size()) == prefix;
}

std::optional<std::int32_t> parse_positive_i32(std::string_view value) {
  if (value.empty()) {
    return std::nullopt;
  }
  std::int64_t out = 0;
  for (const char ch : value) {
    if (ch < '0' || ch > '9') {
      return std::nullopt;
    }
    out = out * 10 + static_cast<std::int64_t>(ch - '0');
    if (out > std::numeric_limits<std::int32_t>::max()) {
      return std::nullopt;
    }
  }
  if (out <= 0) {
    return std::nullopt;
  }
  return static_cast<std::int32_t>(out);
}

bool is_decimal_arrow_format(std::string_view format) {
  return starts_with(format, "d:");
}

std::optional<std::int32_t> decimal_arrow_width_bytes(std::string_view format) {
  if (!is_decimal_arrow_format(format)) {
    return std::nullopt;
  }
  const auto last_comma = format.rfind(',');
  if (last_comma == std::string_view::npos || last_comma + 1 >= format.size()) {
    return std::nullopt;
  }
  auto bits = parse_positive_i32(format.substr(last_comma + 1));
  if (!bits || *bits % 8 != 0) {
    return std::nullopt;
  }
  return *bits / 8;
}

std::optional<std::int32_t>
arrow_value_width_for_column(const ColumnChunkInfo &column) {
  const auto format = std::string_view(column.native_arrow_format);
  if (format == "c" || format == "C") {
    return 1;
  }
  if (format == "s" || format == "S") {
    return 2;
  }
  if (format == "i" || format == "I" || format == "f" || format == "tdD" ||
      format == "ttm") {
    return 4;
  }
  if (format == "l" || format == "L" || format == "g" ||
      starts_with(format, "ts") || format == "ttu" || format == "ttn") {
    return 8;
  }
  if (auto decimal_width = decimal_arrow_width_bytes(format)) {
    return decimal_width;
  }
  if (starts_with(format, "w:")) {
    return parse_positive_i32(format.substr(2));
  }
  return fixed_width_for_plain_values(column);
}

template <class T> void copy_numeric_value(std::uint8_t *target, T value) {
  std::memcpy(target, &value, sizeof(T));
}

sanitize::Status write_arrow_integer_value(std::uint8_t *target,
                                           const ColumnChunkInfo &column,
                                           std::int64_t value) {
  if (!target) {
    return sanitize::Status::Invalid(
        "native Parquet reader: null fixed-width output buffer");
  }
  const auto format = std::string_view(column.native_arrow_format);
  if (format == "c") {
    if (value < std::numeric_limits<std::int8_t>::min() ||
        value > std::numeric_limits<std::int8_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: int8 value out of range");
    }
    copy_numeric_value(target, static_cast<std::int8_t>(value));
    return {};
  }
  if (format == "C") {
    if (value < 0 || value > std::numeric_limits<std::uint8_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: uint8 value out of range");
    }
    copy_numeric_value(target, static_cast<std::uint8_t>(value));
    return {};
  }
  if (format == "s") {
    if (value < std::numeric_limits<std::int16_t>::min() ||
        value > std::numeric_limits<std::int16_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: int16 value out of range");
    }
    copy_numeric_value(target, static_cast<std::int16_t>(value));
    return {};
  }
  if (format == "S") {
    if (value < 0 || value > std::numeric_limits<std::uint16_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: uint16 value out of range");
    }
    copy_numeric_value(target, static_cast<std::uint16_t>(value));
    return {};
  }
  if (format == "i" || format == "tdD" || format == "ttm") {
    if (value < std::numeric_limits<std::int32_t>::min() ||
        value > std::numeric_limits<std::int32_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: int32 value out of range");
    }
    copy_numeric_value(target, static_cast<std::int32_t>(value));
    return {};
  }
  if (format == "I") {
    if (value < 0 || static_cast<std::uint64_t>(value) >
                         std::numeric_limits<std::uint32_t>::max()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: uint32 value out of range");
    }
    copy_numeric_value(target, static_cast<std::uint32_t>(value));
    return {};
  }
  if (format == "l" || starts_with(format, "ts") || format == "ttu" ||
      format == "ttn") {
    copy_numeric_value(target, value);
    return {};
  }
  if (format == "L") {
    copy_numeric_value(target, static_cast<std::uint64_t>(value));
    return {};
  }
  return sanitize::Status::Invalid(
      "native Parquet reader: unsupported integer Arrow format");
}

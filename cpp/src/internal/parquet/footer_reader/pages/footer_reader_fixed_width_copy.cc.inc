sanitize::Status copy_fixed_width_physical_to_arrow(
    std::uint8_t *target, const char *source, const ColumnChunkInfo &column,
    std::int32_t physical_width, std::int32_t arrow_width) {
  if (!target || !source || physical_width <= 0 || arrow_width <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid fixed-width copy input");
  }
  if (column.physical_type == kPhysicalInt32) {
    if (physical_width != 4) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid INT32 physical width");
    }
    if (column.native_arrow_format == "I") {
      copy_numeric_value(target, read_plain_value<std::uint32_t>(
                                     std::string_view(source, 4), 0));
      return {};
    }
    return write_arrow_integer_value(
        target, column,
        static_cast<std::int64_t>(
            read_plain_value<std::int32_t>(std::string_view(source, 4), 0)));
  }
  if (column.physical_type == kPhysicalInt64) {
    if (physical_width != 8) {
      return sanitize::Status::Invalid(
          "native Parquet reader: invalid INT64 physical width");
    }
    return write_arrow_integer_value(
        target, column,
        read_plain_value<std::int64_t>(std::string_view(source, 8), 0));
  }
  if (is_decimal_arrow_format(column.native_arrow_format)) {
    if (physical_width != arrow_width) {
      return sanitize::Status::Invalid(
          "native Parquet reader: decimal physical width mismatch");
    }
    for (std::int32_t i = 0; i < arrow_width; ++i) {
      target[static_cast<std::size_t>(i)] =
          static_cast<std::uint8_t>(source[physical_width - 1 - i]);
    }
    return {};
  }
  if (physical_width != arrow_width) {
    return sanitize::Status::Invalid(
        "native Parquet reader: fixed-width Arrow width mismatch");
  }
  std::memcpy(target, source, static_cast<std::size_t>(arrow_width));
  return {};
}

std::vector<std::string> preview_plain_boolean_values(std::string_view values,
                                                      std::int32_t count) {
  std::vector<std::string> out;
  out.reserve(static_cast<std::size_t>(std::min<std::int32_t>(
      count, static_cast<std::int32_t>(kMaxValuePreviewItems))));
  for (std::int32_t i = 0; i < count && out.size() < kMaxValuePreviewItems;
       ++i) {
    const auto byte =
        static_cast<std::uint8_t>(values[static_cast<std::size_t>(i) / 8U]);
    const auto bit = static_cast<std::uint8_t>(i % 8);
    out.push_back((byte & (std::uint8_t{1} << bit)) != 0 ? "true" : "false");
  }
  return out;
}

std::vector<std::string>
preview_plain_fixed_values(std::string_view values,
                           const ColumnChunkInfo &column,
                           std::int32_t expected_values) {
  std::vector<std::string> out;
  out.reserve(static_cast<std::size_t>(std::min<std::int32_t>(
      expected_values, static_cast<std::int32_t>(kMaxValuePreviewItems))));
  const auto limit = std::min<std::int32_t>(
      expected_values, static_cast<std::int32_t>(kMaxValuePreviewItems));
  for (std::int32_t i = 0; i < limit; ++i) {
    const auto offset =
        static_cast<std::size_t>(i) *
        static_cast<std::size_t>(*fixed_width_for_plain_values(column));
    switch (column.physical_type) {
    case kPhysicalInt32:
      out.push_back(
          std::to_string(read_plain_value<std::int32_t>(values, offset)));
      break;
    case kPhysicalInt64:
      out.push_back(
          std::to_string(read_plain_value<std::int64_t>(values, offset)));
      break;
    case kPhysicalFloat:
      out.push_back(std::to_string(read_plain_value<float>(values, offset)));
      break;
    case kPhysicalDouble:
      out.push_back(std::to_string(read_plain_value<double>(values, offset)));
      break;
    case kPhysicalFixedLenByteArray:
      out.push_back(hex_bytes(values.substr(
          offset, static_cast<std::size_t>(column.fixed_type_length))));
      break;
    default:
      break;
    }
  }
  return out;
}

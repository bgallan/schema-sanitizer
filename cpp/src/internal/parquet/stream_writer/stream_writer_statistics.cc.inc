template <class T>
std::optional<std::pair<std::string, std::string>>
fixed_width_min_max(std::string_view values) {
  if (values.empty() || values.size() % sizeof(T) != 0) {
    return std::nullopt;
  }
  T min_value{};
  T max_value{};
  std::memcpy(&min_value, values.data(), sizeof(T));
  max_value = min_value;
  for (std::size_t offset = sizeof(T); offset < values.size();
       offset += sizeof(T)) {
    T value{};
    std::memcpy(&value, values.data() + offset, sizeof(T));
    min_value = std::min(min_value, value);
    max_value = std::max(max_value, value);
  }
  std::string min_bytes;
  std::string max_bytes;
  append_bytes(min_bytes, &min_value, sizeof(T));
  append_bytes(max_bytes, &max_value, sizeof(T));
  return std::pair<std::string, std::string>{std::move(min_bytes),
                                             std::move(max_bytes)};
}

template <class T> bool floating_less_for_stats(T left, T right) {
  if (left < right) {
    return true;
  }
  if (right < left) {
    return false;
  }
  return left == T{0} && right == T{0} && std::signbit(left) &&
         !std::signbit(right);
}

template <class T>
std::optional<std::pair<std::string, std::string>>
floating_min_max(std::string_view values) {
  if (values.empty() || values.size() % sizeof(T) != 0) {
    return std::nullopt;
  }
  std::optional<T> min_value;
  std::optional<T> max_value;
  for (std::size_t offset = 0; offset < values.size(); offset += sizeof(T)) {
    T value{};
    std::memcpy(&value, values.data() + offset, sizeof(T));
    if (std::isnan(value)) {
      continue;
    }
    if (!min_value) {
      min_value = value;
      max_value = value;
      continue;
    }
    if (floating_less_for_stats(value, *min_value)) {
      min_value = value;
    }
    if (floating_less_for_stats(*max_value, value)) {
      max_value = value;
    }
  }
  if (!min_value || !max_value) {
    return std::nullopt;
  }
  std::string min_bytes;
  std::string max_bytes;
  append_bytes(min_bytes, &*min_value, sizeof(T));
  append_bytes(max_bytes, &*max_value, sizeof(T));
  return std::pair<std::string, std::string>{std::move(min_bytes),
                                             std::move(max_bytes)};
}

std::optional<std::pair<std::string, std::string>>
boolean_min_max(const std::vector<bool> &values) {
  if (values.empty()) {
    return std::nullopt;
  }
  bool has_true = false;
  bool has_false = false;
  for (const bool value : values) {
    has_true = has_true || value;
    has_false = has_false || !value;
    if (has_true && has_false)
      break;
  }
  std::string min_bytes(1, has_false ? '\0' : '\1');
  std::string max_bytes(1, has_true ? '\1' : '\0');
  return std::pair<std::string, std::string>{std::move(min_bytes),
                                             std::move(max_bytes)};
}

bool signed_big_endian_bytes_less(std::string_view left,
                                  std::string_view right) {
  const bool left_negative =
      (static_cast<std::uint8_t>(left.front()) & 0x80U) != 0;
  const bool right_negative =
      (static_cast<std::uint8_t>(right.front()) & 0x80U) != 0;
  if (left_negative != right_negative) {
    return left_negative;
  }
  return std::lexicographical_compare(left.begin(), left.end(), right.begin(),
                                      right.end(), [](char lhs, char rhs) {
                                        return static_cast<std::uint8_t>(lhs) <
                                               static_cast<std::uint8_t>(rhs);
                                      });
}

std::optional<std::pair<std::string, std::string>>
fixed_len_byte_array_min_max(std::string_view values, std::int32_t byte_width) {
  if (byte_width <= 0 || values.empty() ||
      values.size() % static_cast<std::size_t>(byte_width) != 0) {
    return std::nullopt;
  }
  const auto width = static_cast<std::size_t>(byte_width);
  std::string_view min_value = values.substr(0, width);
  std::string_view max_value = min_value;
  for (std::size_t offset = width; offset < values.size(); offset += width) {
    const std::string_view current = values.substr(offset, width);
    if (signed_big_endian_bytes_less(current, min_value))
      min_value = current;
    if (signed_big_endian_bytes_less(max_value, current))
      max_value = current;
  }
  return std::pair<std::string, std::string>{std::string(min_value),
                                             std::string(max_value)};
}

std::uint32_t read_u32_le(std::string_view data, std::size_t offset) {
  static_assert(std::endian::native == std::endian::little ||
                std::endian::native == std::endian::big);
  std::uint32_t value = 0;
  std::memcpy(&value, data.data() + offset, sizeof(value));
  if constexpr (std::endian::native == std::endian::big) {
    value = std::byteswap(value);
  }
  return value;
}

bool byte_array_less(std::string_view left, std::string_view right) {
  return std::lexicographical_compare(left.begin(), left.end(), right.begin(),
                                      right.end(), [](char lhs, char rhs) {
                                        return static_cast<std::uint8_t>(lhs) <
                                               static_cast<std::uint8_t>(rhs);
                                      });
}

std::optional<std::pair<std::string, std::string>>
fixed_size_binary_min_max(std::string_view values, std::int32_t byte_width) {
  if (byte_width <= 0 || values.empty() ||
      values.size() % static_cast<std::size_t>(byte_width) != 0) {
    return std::nullopt;
  }
  const auto width = static_cast<std::size_t>(byte_width);
  std::string_view min_value = values.substr(0, width);
  std::string_view max_value = min_value;
  for (std::size_t offset = width; offset < values.size(); offset += width) {
    const std::string_view current = values.substr(offset, width);
    if (byte_array_less(current, min_value))
      min_value = current;
    if (byte_array_less(max_value, current))
      max_value = current;
  }
  return std::pair<std::string, std::string>{std::string(min_value),
                                             std::string(max_value)};
}

bool is_byte_array_kind(jsonl::JsonlKind kind) {
  return kind == jsonl::JsonlKind::kString ||
         kind == jsonl::JsonlKind::kLargeString ||
         kind == jsonl::JsonlKind::kBinary ||
         kind == jsonl::JsonlKind::kLargeBinary;
}

std::optional<std::size_t> fixed_dictionary_value_width(const ParquetNode &node,
                                                        jsonl::JsonlKind kind) {
  switch (kind) {
  case jsonl::JsonlKind::kInt8:
  case jsonl::JsonlKind::kUInt8:
  case jsonl::JsonlKind::kInt16:
  case jsonl::JsonlKind::kUInt16:
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kUInt32:
  case jsonl::JsonlKind::kDate32:
  case jsonl::JsonlKind::kTime32s:
  case jsonl::JsonlKind::kFloat32:
    return sizeof(std::int32_t);
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kUInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
  case jsonl::JsonlKind::kFloat64:
    return sizeof(std::int64_t);
  case jsonl::JsonlKind::kDecimal:
    if (node.decimal_byte_width <= 0) {
      return std::nullopt;
    }
    return static_cast<std::size_t>(node.decimal_byte_width);
  case jsonl::JsonlKind::kFixedSizeBinary:
    if (node.fixed_binary_byte_width <= 0) {
      return std::nullopt;
    }
    return static_cast<std::size_t>(node.fixed_binary_byte_width);
  default:
    return std::nullopt;
  }
}

std::optional<std::pair<std::string, std::string>>
byte_array_min_max(std::string_view values) {
  std::optional<std::string_view> min_value;
  std::optional<std::string_view> max_value;
  std::size_t offset = 0;
  while (offset < values.size()) {
    if (values.size() - offset < sizeof(std::uint32_t))
      return std::nullopt;
    const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
    offset += sizeof(std::uint32_t);
    if (values.size() - offset < size)
      return std::nullopt;
    const std::string_view current = values.substr(offset, size);
    offset += size;
    if (!min_value || byte_array_less(current, *min_value))
      min_value = current;
    if (!max_value || byte_array_less(*max_value, current))
      max_value = current;
  }
  if (!min_value || !max_value)
    return std::nullopt;
  return std::pair<std::string, std::string>{std::string(*min_value),
                                             std::string(*max_value)};
}
std::optional<std::pair<std::string, std::string>>
encoded_min_max_for_column(const LeafColumn &column,
                           const ColumnPageData &page_data) {
  const auto kind = column.node->arrow_kind == jsonl::JsonlKind::kDictionary
                        ? column.node->dictionary_value_kind
                        : column.node->arrow_kind;
  switch (kind) {
  case jsonl::JsonlKind::kBool:
    return boolean_min_max(page_data.bool_values);
  case jsonl::JsonlKind::kInt8:
  case jsonl::JsonlKind::kInt16:
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kDate32:
  case jsonl::JsonlKind::kTime32s:
    return fixed_width_min_max<std::int32_t>(page_data.values);
  case jsonl::JsonlKind::kUInt8:
  case jsonl::JsonlKind::kUInt16:
  case jsonl::JsonlKind::kUInt32:
    return fixed_width_min_max<std::uint32_t>(page_data.values);
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
    return fixed_width_min_max<std::int64_t>(page_data.values);
  case jsonl::JsonlKind::kUInt64:
    return fixed_width_min_max<std::uint64_t>(page_data.values);
  case jsonl::JsonlKind::kFloat32:
    return floating_min_max<float>(page_data.values);
  case jsonl::JsonlKind::kFloat64:
    return floating_min_max<double>(page_data.values);
  case jsonl::JsonlKind::kString:
  case jsonl::JsonlKind::kLargeString:
  case jsonl::JsonlKind::kBinary:
  case jsonl::JsonlKind::kLargeBinary:
    return byte_array_min_max(page_data.values);
  case jsonl::JsonlKind::kDecimal:
    return fixed_len_byte_array_min_max(page_data.values,
                                        column.node->decimal_byte_width);
  case jsonl::JsonlKind::kFixedSizeBinary:
    return fixed_size_binary_min_max(page_data.values,
                                     column.node->fixed_binary_byte_width);
  default:
    return std::nullopt;
  }
}

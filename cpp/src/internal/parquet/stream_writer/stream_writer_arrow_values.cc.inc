// Appends one little-endian 32-bit integer.
void append_u32_le(std::string &out, std::uint32_t value) {
  static_assert(std::endian::native == std::endian::little ||
                std::endian::native == std::endian::big);
  if constexpr (std::endian::native == std::endian::big) {
    value = std::byteswap(value);
  }
  out.append(reinterpret_cast<const char *>(&value), sizeof(value));
}

// Appends raw bytes to a string buffer.
void append_bytes(std::string &out, const void *data, std::size_t size) {
  out.append(static_cast<const char *>(data), size);
}

// Returns whether an Arrow value is valid at a logical row index.
bool bitmap_is_valid(const ArrowArray &array, std::int64_t logical_index) {
  if (array.n_buffers <= 0 || !array.buffers || !array.buffers[0]) {
    return true;
  }
  const auto *bits = static_cast<const std::uint8_t *>(array.buffers[0]);
  const auto index = array.offset + logical_index;
  return (bits[index >> 3] & (std::uint8_t{1} << (index & 7))) != 0;
}

// Reads one boolean value from an Arrow bitmap array.
bool bool_value(const ArrowArray &array, std::int64_t logical_index) {
  if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1]) {
    return false;
  }
  const auto *bits = static_cast<const std::uint8_t *>(array.buffers[1]);
  const auto index = array.offset + logical_index;
  return (bits[index >> 3] & (std::uint8_t{1} << (index & 7))) != 0;
}

// Returns a typed pointer to a fixed-width Arrow value buffer.
template <class T> const T *data_buffer(const ArrowArray &array) {
  if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1]) {
    return nullptr;
  }
  return static_cast<const T *>(array.buffers[1]);
}

// Reads one supported Arrow dictionary index.
template <class T>
std::optional<std::int64_t> dictionary_index_value(const ArrowArray &array,
                                                   std::int64_t row) {
  const T *values = data_buffer<T>(array);
  if (!values) {
    return std::nullopt;
  }
  return static_cast<std::int64_t>(values[array.offset + row]);
}

std::optional<std::int64_t> dictionary_index_at(const ArrowArray &array,
                                                jsonl::JsonlKind index_kind,
                                                std::int64_t row) {
  switch (index_kind) {
  case jsonl::JsonlKind::kInt8:
    return dictionary_index_value<std::int8_t>(array, row);
  case jsonl::JsonlKind::kUInt8:
    return dictionary_index_value<std::uint8_t>(array, row);
  case jsonl::JsonlKind::kInt16:
    return dictionary_index_value<std::int16_t>(array, row);
  case jsonl::JsonlKind::kUInt16:
    return dictionary_index_value<std::uint16_t>(array, row);
  case jsonl::JsonlKind::kInt32:
    return dictionary_index_value<std::int32_t>(array, row);
  case jsonl::JsonlKind::kUInt32:
    return dictionary_index_value<std::uint32_t>(array, row);
  case jsonl::JsonlKind::kInt64:
    return dictionary_index_value<std::int64_t>(array, row);
  default:
    return std::nullopt;
  }
}

// Encodes supported Arrow primitive and dictionary values.

// Appends non-null fixed-width values from one Arrow array.
template <class T>
sanitize::Status append_fixed_value(std::string &out, const ArrowArray &array,
                                    std::int64_t row) {
  const T *values = data_buffer<T>(array);
  if (!values) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing fixed-width value buffer");
  }
  const T value = values[array.offset + row];
  append_bytes(out, &value, sizeof(T));
  return sanitize::Status::OK();
}

// Appends an Arrow integer using the physical Parquet storage width.
template <class Source, class Physical>
sanitize::Status append_integer_value(std::string &out, const ArrowArray &array,
                                      std::int64_t row) {
  const Source *values = data_buffer<Source>(array);
  if (!values) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing fixed-width value buffer");
  }
  const Physical value = static_cast<Physical>(values[array.offset + row]);
  append_bytes(out, &value, sizeof(Physical));
  return sanitize::Status::OK();
}

// Appends an Arrow time32[s] value as Parquet TIME(MILLIS).
sanitize::Status append_time32s_as_millis(std::string &out,
                                          const ArrowArray &array,
                                          std::int64_t row) {
  const std::int32_t *values = data_buffer<std::int32_t>(array);
  if (!values) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing fixed-width value buffer");
  }
  const auto seconds = static_cast<std::int64_t>(values[array.offset + row]);
  if (seconds > std::numeric_limits<std::int32_t>::max() / 1000LL ||
      seconds < std::numeric_limits<std::int32_t>::min() / 1000LL) {
    return sanitize::Status::Invalid(
        "native Parquet writer: time32[s] value overflows TIME(MILLIS)");
  }
  const auto millis = static_cast<std::int32_t>(seconds * 1000LL);
  append_bytes(out, &millis, sizeof(millis));
  return sanitize::Status::OK();
}

// Appends one non-null boolean value to a staging byte vector.
void append_bool_value(std::vector<bool> *values, const ArrowArray &array,
                       std::int64_t row) {
  values->push_back(bool_value(array, row));
}

// Appends one non-null binary or string value as a Parquet byte array.
template <class Offset>
sanitize::Status append_binary_value(std::string &out, const ArrowArray &array,
                                     std::int64_t row) {
  if (array.n_buffers <= 2 || !array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing binary value buffers");
  }
  const auto *offsets = static_cast<const Offset *>(array.buffers[1]);
  const auto *data = static_cast<const char *>(array.buffers[2]);
  const auto index = array.offset + row;
  const auto begin = offsets[index];
  const auto end = offsets[index + 1];
  if (end < begin || static_cast<std::uint64_t>(end - begin) >
                         std::numeric_limits<std::uint32_t>::max()) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid binary value length");
  }
  const auto value_size = static_cast<std::size_t>(end - begin);
  if (value_size > 0 && !data) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing binary value buffers");
  }
  append_u32_le(out, static_cast<std::uint32_t>(value_size));
  if (value_size > 0) {
    out.append(data + begin, value_size);
  }
  return sanitize::Status::OK();
}

// Appends one Arrow decimal128/decimal256 value as Parquet fixed bytes.
sanitize::Status append_decimal_value(std::string &out, const ParquetNode &node,
                                      const ArrowArray &array,
                                      std::int64_t row) {
  if (node.decimal_byte_width <= 0 || array.n_buffers <= 1 || !array.buffers ||
      !array.buffers[1]) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing decimal value buffer");
  }
  const auto *data = static_cast<const std::uint8_t *>(array.buffers[1]);
  const auto byte_width = static_cast<std::size_t>(node.decimal_byte_width);
  const auto offset = static_cast<std::size_t>(array.offset + row) * byte_width;
  for (std::size_t i = 0; i < byte_width; ++i) {
    out.push_back(static_cast<char>(data[offset + byte_width - 1U - i]));
  }
  return sanitize::Status::OK();
}

// Appends one Arrow fixed-size binary value as Parquet fixed bytes.
sanitize::Status append_fixed_size_binary_value(std::string &out,
                                                const ParquetNode &node,
                                                const ArrowArray &array,
                                                std::int64_t row) {
  if (node.fixed_binary_byte_width <= 0 || array.n_buffers <= 1 ||
      !array.buffers || !array.buffers[1]) {
    return sanitize::Status::Invalid(
        "native Parquet writer: missing fixed-size binary value buffer");
  }
  const auto *data = static_cast<const char *>(array.buffers[1]);
  const auto byte_width =
      static_cast<std::size_t>(node.fixed_binary_byte_width);
  const auto offset = static_cast<std::size_t>(array.offset + row) * byte_width;
  out.append(data + offset, byte_width);
  return sanitize::Status::OK();
}

// Appends one non-null primitive value.
sanitize::Status append_plain_primitive_value(
    std::string &out, std::vector<bool> *bool_values, const ParquetNode &node,
    jsonl::JsonlKind value_kind, const ArrowArray &array, std::int64_t row) {
  switch (value_kind) {
  case jsonl::JsonlKind::kNull:
    return sanitize::Status::OK();
  case jsonl::JsonlKind::kBool:
    if (!bool_values) {
      return sanitize::Status::Invalid(
          "native Parquet writer: missing boolean staging buffer");
    }
    append_bool_value(bool_values, array, row);
    return sanitize::Status::OK();
  case jsonl::JsonlKind::kInt8:
    return append_integer_value<std::int8_t, std::int32_t>(out, array, row);
  case jsonl::JsonlKind::kUInt8:
    return append_integer_value<std::uint8_t, std::uint32_t>(out, array, row);
  case jsonl::JsonlKind::kInt16:
    return append_integer_value<std::int16_t, std::int32_t>(out, array, row);
  case jsonl::JsonlKind::kUInt16:
    return append_integer_value<std::uint16_t, std::uint32_t>(out, array, row);
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kDate32:
    return append_fixed_value<std::int32_t>(out, array, row);
  case jsonl::JsonlKind::kTime32s:
    return append_time32s_as_millis(out, array, row);
  case jsonl::JsonlKind::kUInt32:
    return append_fixed_value<std::uint32_t>(out, array, row);
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
    return append_fixed_value<std::int64_t>(out, array, row);
  case jsonl::JsonlKind::kUInt64:
    return append_fixed_value<std::uint64_t>(out, array, row);
  case jsonl::JsonlKind::kFloat32:
    return append_fixed_value<float>(out, array, row);
  case jsonl::JsonlKind::kFloat64:
    return append_fixed_value<double>(out, array, row);
  case jsonl::JsonlKind::kString:
  case jsonl::JsonlKind::kBinary:
    return append_binary_value<std::int32_t>(out, array, row);
  case jsonl::JsonlKind::kLargeString:
  case jsonl::JsonlKind::kLargeBinary:
    return append_binary_value<std::int64_t>(out, array, row);
  case jsonl::JsonlKind::kFixedSizeBinary:
    return append_fixed_size_binary_value(out, node, array, row);
  case jsonl::JsonlKind::kDecimal:
    return append_decimal_value(out, node, array, row);
  default:
    return sanitize::Status::NotImplemented(kUnsupportedPrefix,
                                            " unsupported column value kind");
  }
}

// Appends one non-null dictionary value as its plain value representation.
sanitize::Status append_dictionary_value(std::string &out,
                                         std::vector<bool> *bool_values,
                                         const ParquetNode &node,
                                         const ArrowArray &array,
                                         std::int64_t row) {
  if (!array.dictionary) {
    return sanitize::Status::Invalid(
        "native Parquet writer: dictionary column has no dictionary values");
  }
  auto index = dictionary_index_at(array, node.dictionary_index_kind, row);
  if (!index || *index < 0 || *index >= array.dictionary->length) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid dictionary index");
  }
  return append_plain_primitive_value(out, bool_values, node,
                                      node.dictionary_value_kind,
                                      *array.dictionary, *index);
}

// Returns whether a non-null dictionary index points at a null dictionary
// value.
sanitize::Result<bool> dictionary_value_is_null(const ParquetNode &node,
                                                const ArrowArray &array,
                                                std::int64_t row) {
  if (node.arrow_kind != jsonl::JsonlKind::kDictionary) {
    return false;
  }
  if (!array.dictionary) {
    return sanitize::Status::Invalid(
        "native Parquet writer: dictionary column has no dictionary values");
  }
  auto index = dictionary_index_at(array, node.dictionary_index_kind, row);
  if (!index || *index < 0 || *index >= array.dictionary->length) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid dictionary index");
  }
  const bool is_null = !bitmap_is_valid(*array.dictionary, *index);
  if (is_null && node.required) {
    return sanitize::Status::Invalid(
        "native Parquet writer: required dictionary value is null");
  }
  return is_null;
}

// Appends one non-null primitive value.
sanitize::Status append_primitive_value(std::string &out,
                                        std::vector<bool> *bool_values,
                                        const ParquetNode &node,
                                        const ArrowArray &array,
                                        std::int64_t row) {
  if (node.arrow_kind == jsonl::JsonlKind::kDictionary) {
    return append_dictionary_value(out, bool_values, node, array, row);
  }
  return append_plain_primitive_value(out, bool_values, node, node.arrow_kind,
                                      array, row);
}

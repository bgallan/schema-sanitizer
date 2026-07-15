// Implements primitive reads and bounded skipping for the Thrift compact
// reader.

sanitize::Result<std::uint8_t> CompactReader::read_byte() {
  if (pos_ >= data_.size()) {
    return sanitize::Status::Invalid("Parquet footer: unexpected end of data");
  }
  return static_cast<std::uint8_t>(data_[pos_++]);
}

sanitize::Result<std::uint64_t> CompactReader::read_varint() {
  std::uint64_t out = 0;
  int shift = 0;
  for (int index = 0; index < 10; ++index) {
    std::uint8_t byte = 0;
    SAN_ASSIGN_OR_RAISE(byte, read_byte());
    if (index == 9 && (byte & 0xFEU) != 0) {
      return sanitize::Status::Invalid("Parquet footer: varint overflow");
    }
    out |= static_cast<std::uint64_t>(byte & 0x7FU) << shift;
    if ((byte & 0x80U) == 0)
      return out;
    shift += 7;
  }
  return sanitize::Status::Invalid("Parquet footer: invalid varint");
}

sanitize::Result<std::int32_t> CompactReader::read_i32() {
  std::uint64_t raw = 0;
  SAN_ASSIGN_OR_RAISE(raw, read_varint());
  const auto value = unzigzag(raw);
  if (value < std::numeric_limits<std::int32_t>::min() ||
      value > std::numeric_limits<std::int32_t>::max()) {
    return sanitize::Status::Invalid("Parquet footer: i32 out of range");
  }
  return static_cast<std::int32_t>(value);
}

sanitize::Result<std::int16_t> CompactReader::read_i16() {
  std::int32_t value = 0;
  SAN_ASSIGN_OR_RAISE(value, read_i32());
  if (value < std::numeric_limits<std::int16_t>::min() ||
      value > std::numeric_limits<std::int16_t>::max()) {
    return sanitize::Status::Invalid("Parquet footer: i16 out of range");
  }
  return static_cast<std::int16_t>(value);
}

sanitize::Result<std::int64_t> CompactReader::read_i64() {
  std::uint64_t raw = 0;
  SAN_ASSIGN_OR_RAISE(raw, read_varint());
  return unzigzag(raw);
}

sanitize::Status CompactReader::charge_materialized_bytes(
    std::uint64_t bytes, std::string_view label) {
  if (bytes > static_cast<std::uint64_t>(
                  std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::Invalid("Parquet metadata: ", label,
                                     " allocation size out of range");
  }
  const auto amount = static_cast<std::size_t>(bytes);
  if (amount > materialized_limit_ -
                   std::min(materialized_bytes_, materialized_limit_)) {
    return sanitize::Status::Invalid(
        "Parquet metadata: materialized-memory budget exceeded while reading ",
        label);
  }
  materialized_bytes_ += amount;
  return {};
}

sanitize::Status validate_metadata_list(CompactReader &reader,
                                        std::uint64_t count,
                                        std::uint64_t max_count,
                                        std::size_t element_bytes,
                                        std::string_view label) {
  if (count > max_count) {
    return sanitize::Status::Invalid("Parquet metadata: ", label,
                                     " count exceeds security limit");
  }
  // Every compact-protocol list element consumes at least one byte. Reject the
  // declaration before reserving memory when the payload cannot possibly fit.
  if (count > reader.remaining_bytes()) {
    return sanitize::Status::Invalid("Parquet metadata: ", label,
                                     " count exceeds remaining input bytes");
  }
  if (element_bytes != 0 &&
      count > std::numeric_limits<std::uint64_t>::max() / element_bytes) {
    return sanitize::Status::Invalid("Parquet metadata: ", label,
                                     " allocation size overflow");
  }
  return reader.charge_materialized_bytes(count * element_bytes, label);
}

sanitize::Result<std::string> CompactReader::read_binary() {
  std::uint64_t size = 0;
  SAN_ASSIGN_OR_RAISE(size, read_varint());
  if (size > data_.size() - pos_) {
    return sanitize::Status::Invalid(
        "Parquet footer: binary value exceeds footer");
  }
  if (size > kMaxMetadataBinaryBytes) {
    return sanitize::Status::Invalid(
        "Parquet footer: binary metadata value exceeds security limit");
  }
  SAN_RETURN_NOT_OK(charge_materialized_bytes(size, "binary value"));
  std::string out(data_.data() + pos_, static_cast<std::size_t>(size));
  pos_ += static_cast<std::size_t>(size);
  return out;
}

sanitize::Status CompactReader::skip_binary() {
  std::uint64_t size = 0;
  SAN_ASSIGN_OR_RAISE(size, read_varint());
  if (size > data_.size() - pos_) {
    return sanitize::Status::Invalid(
        "Parquet footer: binary value exceeds footer");
  }
  if (size > kMaxMetadataBinaryBytes) {
    return sanitize::Status::Invalid(
        "Parquet footer: binary metadata value exceeds security limit");
  }
  pos_ += static_cast<std::size_t>(size);
  return {};
}

sanitize::Status CompactReader::read_list_header(std::uint8_t *element_type,
                                                 std::uint64_t *size) {
  if (!element_type || !size) {
    return sanitize::Status::Invalid("Parquet footer: internal list error");
  }
  std::uint8_t header = 0;
  SAN_ASSIGN_OR_RAISE(header, read_byte());
  *size = static_cast<std::uint64_t>(header >> 4U);
  *element_type = static_cast<std::uint8_t>(header & 0x0FU);
  if (*size == 15) {
    SAN_ASSIGN_OR_RAISE(*size, read_varint());
  }
  if (*size > kMaxContainerElements) {
    return sanitize::Status::Invalid("Parquet footer: container too large");
  }
  if (*size > 0 && *element_type == kTypeStop) {
    return sanitize::Status::Invalid(
        "Parquet footer: non-empty container has STOP element type");
  }
  // Every compact-protocol collection element consumes at least one byte.
  // Reject impossible declarations before entering a potentially huge loop.
  if (*size > remaining_bytes()) {
    return sanitize::Status::Invalid(
        "Parquet footer: container count exceeds remaining input bytes");
  }
  return {};
}

sanitize::Status CompactReader::skip_type(std::uint8_t type, int depth) {
  if (depth > kMaxSkipDepth) {
    return sanitize::Status::Invalid(
        "Parquet footer: nested metadata too deep");
  }
  switch (type) {
  case kTypeStop:
  case kTypeBoolTrue:
  case kTypeBoolFalse:
    return {};
  case kTypeByte: {
    std::uint8_t ignored = 0;
    SAN_ASSIGN_OR_RAISE(ignored, read_byte());
    (void)ignored;
    return {};
  }
  case kTypeI16: {
    std::int16_t ignored = 0;
    SAN_ASSIGN_OR_RAISE(ignored, read_i16());
    (void)ignored;
    return {};
  }
  case kTypeI32: {
    std::int32_t ignored = 0;
    SAN_ASSIGN_OR_RAISE(ignored, read_i32());
    (void)ignored;
    return {};
  }
  case kTypeI64: {
    std::int64_t ignored = 0;
    SAN_ASSIGN_OR_RAISE(ignored, read_i64());
    (void)ignored;
    return {};
  }
  case kTypeDouble:
    if (data_.size() - pos_ < 8)
      return sanitize::Status::Invalid("Parquet footer: truncated double");
    pos_ += 8;
    return {};
  case kTypeBinary:
    return skip_binary();
  case kTypeList:
  case kTypeSet:
    return skip_list(depth + 1);
  case kTypeMap:
    return skip_map(depth + 1);
  case kTypeStruct:
    return skip_struct(depth + 1);
  case kTypeUuid:
    if (data_.size() - pos_ < 16)
      return sanitize::Status::Invalid("Parquet footer: truncated uuid");
    pos_ += 16;
    return {};
  default:
    return sanitize::Status::Invalid(
        "Parquet footer: unsupported compact type ", static_cast<int>(type));
  }
}

sanitize::Status CompactReader::skip_container_value(std::uint8_t type,
                                                     int depth) {
  if (type == kTypeBoolTrue || type == kTypeBoolFalse) {
    std::uint8_t ignored = 0;
    SAN_ASSIGN_OR_RAISE(ignored, read_byte());
    (void)ignored;
    return {};
  }
  return skip_type(type, depth);
}

sanitize::Status CompactReader::skip_struct(int depth) {
  std::int16_t last_field_id = 0;
  while (true) {
    std::uint8_t header = 0;
    SAN_ASSIGN_OR_RAISE(header, read_byte());
    const auto type = static_cast<std::uint8_t>(header & 0x0FU);
    if (type == kTypeStop)
      return {};
    const auto delta = static_cast<std::uint8_t>(header >> 4U);
    if (delta == 0) {
      SAN_ASSIGN_OR_RAISE(last_field_id, read_i16());
    } else {
      last_field_id = static_cast<std::int16_t>(last_field_id + delta);
    }
    SAN_RETURN_NOT_OK(skip_type(type, depth));
  }
}

sanitize::Status CompactReader::skip_list(int depth) {
  std::uint8_t element_type = 0;
  std::uint64_t size = 0;
  SAN_RETURN_NOT_OK(read_list_header(&element_type, &size));
  for (std::uint64_t index = 0; index < size; ++index)
    SAN_RETURN_NOT_OK(skip_container_value(element_type, depth));
  return {};
}

sanitize::Status CompactReader::skip_map(int depth) {
  std::uint64_t size = 0;
  SAN_ASSIGN_OR_RAISE(size, read_varint());
  if (size == 0)
    return {};
  if (size > kMaxContainerElements)
    return sanitize::Status::Invalid("Parquet footer: map too large");
  std::uint8_t types = 0;
  SAN_ASSIGN_OR_RAISE(types, read_byte());
  const auto key_type = static_cast<std::uint8_t>((types >> 4U) & 0x0FU);
  const auto value_type = static_cast<std::uint8_t>(types & 0x0FU);
  if (key_type == kTypeStop || value_type == kTypeStop) {
    return sanitize::Status::Invalid(
        "Parquet footer: non-empty map has STOP key/value type");
  }
  // Each map entry contains at least one byte for its key and one for its
  // value, even when either side is an empty nested container.
  if (size > remaining_bytes() / 2U) {
    return sanitize::Status::Invalid(
        "Parquet footer: map count exceeds remaining input bytes");
  }
  for (std::uint64_t index = 0; index < size; ++index) {
    SAN_RETURN_NOT_OK(skip_container_value(key_type, depth));
    SAN_RETURN_NOT_OK(skip_container_value(value_type, depth));
  }
  return {};
}

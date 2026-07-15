// Appends an unsigned varint using Thrift compact encoding.
void append_varint(std::string &out, std::uint64_t value) {
  while ((value & ~std::uint64_t{0x7F}) != 0) {
    out.push_back(static_cast<char>((value & 0x7FU) | 0x80U));
    value >>= 7U;
  }
  out.push_back(static_cast<char>(value));
}

// Zig-zag encodes a signed integer for Thrift compact fields.
std::uint64_t zigzag(std::int64_t value) {
  return (static_cast<std::uint64_t>(value) << 1U) ^
         static_cast<std::uint64_t>(value >> 63U);
}

class CompactWriter {
public:
  // Creates a Thrift compact writer over an existing byte buffer.
  explicit CompactWriter(std::string &out) : out_(out) {}

  // Writes an i32 field.
  void FieldI32(std::int16_t id, std::int32_t value) {
    field(id, 5);
    append_varint(out_, zigzag(value));
  }

  // Writes an i8 field.
  void FieldByte(std::int16_t id, std::int8_t value) {
    field(id, 3);
    out_.push_back(static_cast<char>(value));
  }

  // Writes an i64 field.
  void FieldI64(std::int16_t id, std::int64_t value) {
    field(id, 6);
    append_varint(out_, zigzag(value));
  }

  // Writes a binary/string field.
  void FieldString(std::int16_t id, std::string_view value) {
    field(id, 8);
    append_varint(out_, value.size());
    out_.append(value.data(), value.size());
  }

  // Writes a boolean field.
  void FieldBool(std::int16_t id, bool value) { field(id, value ? 1 : 2); }

  // Writes a nested struct field.
  void FieldStruct(std::int16_t id, auto write_struct) {
    field(id, 12);
    CompactWriter child(out_);
    write_struct(child);
    child.Stop();
  }

  // Writes a list of i32 values.
  void FieldListI32(std::int16_t id, const std::vector<std::int32_t> &values) {
    field(id, 9);
    list_header(values.size(), 5);
    for (const auto value : values) {
      append_varint(out_, zigzag(value));
    }
  }

  // Writes a list of i64 values.
  void FieldListI64(std::int16_t id, const std::vector<std::int64_t> &values) {
    FieldListI64(id, values.size(),
                 [&](std::size_t index) { return values[index]; });
  }

  // Writes generated i64 values without materializing an auxiliary vector.
  void FieldListI64(std::int16_t id, std::size_t size, auto value_at) {
    field(id, 9);
    list_header(size, 6);
    for (std::size_t index = 0; index < size; ++index) {
      append_varint(out_, zigzag(value_at(index)));
    }
  }

  // Writes a list of boolean values.
  void FieldListBool(std::int16_t id, const std::vector<bool> &values) {
    FieldListBool(id, values.size(),
                  [&](std::size_t index) { return values[index]; });
  }

  // Writes generated booleans without materializing an auxiliary vector.
  void FieldListBool(std::int16_t id, std::size_t size, auto value_at) {
    field(id, 9);
    list_header(size, 2);
    for (std::size_t index = 0; index < size; ++index) {
      out_.push_back(static_cast<char>(value_at(index) ? 1 : 2));
    }
  }

  // Writes a list of strings.
  void FieldListString(std::int16_t id,
                       const std::vector<std::string> &values) {
    FieldListString(
        id, values.size(),
        [&](std::size_t index) -> std::string_view { return values[index]; });
  }

  // Writes generated strings without copying them into an auxiliary vector.
  void FieldListString(std::int16_t id, std::size_t size, auto value_at) {
    field(id, 9);
    list_header(size, 8);
    for (std::size_t index = 0; index < size; ++index) {
      const std::string_view value = value_at(index);
      append_varint(out_, value.size());
      out_.append(value.data(), value.size());
    }
  }

  // Writes a list of structs.
  void FieldListStruct(std::int16_t id, std::size_t size, auto write_items) {
    field(id, 9);
    list_header(size, 12);
    write_items(*this);
  }

  // Closes the current struct.
  void Stop() { out_.push_back('\0'); }

private:
  // Writes one compact field header.
  void field(std::int16_t id, std::uint8_t type) {
    const auto delta = id - last_field_id_;
    if (delta > 0 && delta <= 15) {
      out_.push_back(static_cast<char>((delta << 4) | type));
    } else {
      out_.push_back(static_cast<char>(type));
      append_varint(out_, zigzag(id));
    }
    last_field_id_ = id;
  }

  // Writes one compact list header.
  void list_header(std::size_t size, std::uint8_t element_type) {
    if (size <= 14) {
      out_.push_back(static_cast<char>((size << 4U) | element_type));
      return;
    }
    out_.push_back(static_cast<char>(0xF0U | element_type));
    append_varint(out_, size);
  }

  std::string &out_;
  std::int16_t last_field_id_ = 0;
};

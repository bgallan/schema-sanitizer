// Encodes Parquet data and dictionary page headers with definition-level
// metadata. This fragment is included by the consolidated writer to isolate
// format-specific implementation details.

/// Returns the minimum bit width needed to represent a repetition or definition
/// level.
std::uint8_t level_bit_width(std::int16_t max_level) {
  std::uint8_t width = 0;
  while ((std::int16_t{1} << width) <= max_level) {
    ++width;
  }
  return width;
}

/// Appends one RLE run for repetition or definition levels.
void append_rle_run(std::string &out, std::int64_t run_length,
                    std::uint16_t value, std::uint8_t bit_width) {
  append_varint(out, static_cast<std::uint64_t>(run_length) << 1U);
  const auto byte_width = static_cast<std::uint8_t>((bit_width + 7U) / 8U);
  for (std::uint8_t i = 0; i < byte_width; ++i) {
    out.push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
  }
}

/// Encodes Parquet RLE levels with a 4-byte length prefix.
std::string encode_levels(const std::vector<std::int16_t> &levels,
                          std::int16_t max_level) {
  const auto bit_width = level_bit_width(max_level);
  std::string encoded;
  if (!levels.empty() && bit_width > 0) {
    auto current = static_cast<std::uint16_t>(levels.front());
    std::int64_t run_length = 1;
    for (std::size_t i = 1; i < levels.size(); ++i) {
      const auto value = static_cast<std::uint16_t>(levels[i]);
      if (value == current) {
        ++run_length;
        continue;
      }
      append_rle_run(encoded, run_length, current, bit_width);
      current = value;
      run_length = 1;
    }
    append_rle_run(encoded, run_length, current, bit_width);
  }

  std::string out;
  append_u32_le(out, static_cast<std::uint32_t>(encoded.size()));
  out.append(encoded);
  return out;
}

/// Encodes a Parquet data page header.
std::string encode_data_page_header(std::int32_t num_values,
                                    std::int32_t uncompressed_size,
                                    std::int32_t compressed_size,
                                    std::int32_t value_encoding) {
  std::string out;
  CompactWriter writer(out);
  writer.FieldI32(1, 0);                 // DATA_PAGE
  writer.FieldI32(2, uncompressed_size); // uncompressed size
  writer.FieldI32(3, compressed_size);   // compressed size
  writer.FieldStruct(5, [&](CompactWriter &page) {
    page.FieldI32(1, num_values);
    page.FieldI32(2, value_encoding);
    page.FieldI32(3, 3); // RLE
    page.FieldI32(4, 3); // RLE
  });
  writer.Stop();
  return out;
}

/// Encodes a dictionary page header with its payload sizes and dictionary entry
/// count.
std::string encode_dictionary_page_header(std::int32_t num_values,
                                          std::int32_t uncompressed_size,
                                          std::int32_t compressed_size) {
  std::string out;
  CompactWriter writer(out);
  writer.FieldI32(1, 2);                 // DICTIONARY_PAGE
  writer.FieldI32(2, uncompressed_size); // uncompressed size
  writer.FieldI32(3, compressed_size);   // compressed size
  writer.FieldStruct(7, [&](CompactWriter &page) {
    page.FieldI32(1, num_values);
    page.FieldI32(2, 0); // PLAIN
    page.FieldBool(3, false);
  });
  writer.Stop();
  return out;
}

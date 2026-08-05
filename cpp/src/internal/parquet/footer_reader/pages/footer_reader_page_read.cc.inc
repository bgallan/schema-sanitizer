sanitize::Result<PageHeaderInfo> read_page_header_at(std::ifstream &file,
                                                     std::uint64_t offset,
                                                     std::uint64_t limit,
                                                     std::string &bytes) {
  if (offset >= limit) {
    return sanitize::Status::Invalid("Parquet page header: invalid offset");
  }
  constexpr std::size_t kInitialPageHeaderBytes = 256;
  const auto available = limit - offset;
  const auto maximum_window = static_cast<std::size_t>(std::min<std::uint64_t>(
      available, static_cast<std::uint64_t>(kMaxPageHeaderBytes)));
  clear_or_release_page_scratch(&bytes, kRetainedPageHeaderScratchBytes);
  std::size_t window_size = std::min(maximum_window, kInitialPageHeaderBytes);
  bytes.resize(window_size);
  file.clear();
  file.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  file.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  if (!file) {
    return sanitize::Status::IOError("Parquet page header: failed reading");
  }

  auto parsed = read_page_header(bytes);
  while (!parsed.ok() && window_size < maximum_window) {
    const auto previous_size = window_size;
    window_size = std::min(maximum_window, window_size * 2);
    bytes.resize(window_size);
    file.read(bytes.data() + previous_size,
              static_cast<std::streamsize>(window_size - previous_size));
    if (!file) {
      return sanitize::Status::IOError("Parquet page header: failed reading");
    }
    parsed = read_page_header(bytes);
  }
  if (!parsed.ok()) {
    return parsed.status();
  }

  auto page = std::move(parsed).ValueOrDie();
  page.header_offset = static_cast<std::int64_t>(offset);
  page.compressed_payload_offset = static_cast<std::int64_t>(
      offset + static_cast<std::uint64_t>(page.header_size));
  if (page.header_size < 0 || page.compressed_page_size < 0) {
    return sanitize::Status::Invalid(
        "Parquet page header: negative header or payload size");
  }
  const auto header_size = static_cast<std::uint64_t>(page.header_size);
  const auto payload_size =
      static_cast<std::uint64_t>(page.compressed_page_size);
  if (header_size > std::numeric_limits<std::uint64_t>::max() - offset ||
      payload_size >
          std::numeric_limits<std::uint64_t>::max() - offset - header_size) {
    return sanitize::Status::Invalid(
        "Parquet page header: page range overflow");
  }
  const auto page_end = offset + header_size + payload_size;
  if (page_end > limit) {
    return sanitize::Status::Invalid("Parquet page header: page exceeds chunk");
  }
  return page;
}

std::uint32_t parquet_page_crc32(std::string_view payload) noexcept {
  std::uint32_t crc = 0xFFFFFFFFU;
  for (const auto byte : payload) {
    crc ^= static_cast<std::uint8_t>(byte);
    for (int bit = 0; bit < 8; ++bit) {
      const auto mask =
          static_cast<std::uint32_t>(-static_cast<std::int32_t>(crc & 1U));
      crc = (crc >> 1U) ^ (0xEDB88320U & mask);
    }
  }
  return ~crc;
}

sanitize::Status
validate_parquet_page_checksum(const PageHeaderInfo &page,
                               std::string_view compressed_payload) {
  if (!page.has_crc) {
    return {};
  }
  const auto actual = parquet_page_crc32(compressed_payload);
  if (actual != page.crc) {
    return sanitize::Status::Invalid(
        "Parquet page payload: CRC32 checksum mismatch at byte ",
        page.compressed_payload_offset);
  }
  return {};
}

sanitize::Status read_exact_payload_into(std::ifstream &file,
                                         std::int64_t offset, std::int32_t size,
                                         std::string *out) {
  if (!out) {
    return sanitize::Status::Invalid("Parquet page payload: output is null");
  }
  if (offset < 0 || size < 0) {
    return sanitize::Status::Invalid("Parquet page payload: invalid range");
  }
  if (static_cast<std::size_t>(size) > kMaxPayloadVerificationBytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: payload exceeds verification limit");
  }
  if (secure_memory_cleanup_enabled() && !out->empty()) {
    secure_zero_memory(out->data(), out->size());
  }
  out->clear();
  file.clear();
  file.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  bool read_ok = false;
  const auto payload_size = static_cast<std::size_t>(size);
  out->resize_and_overwrite(
      payload_size, [&](char *data, std::size_t capacity) {
        if (capacity < payload_size) {
          return std::size_t{0};
        }
        file.read(data, static_cast<std::streamsize>(payload_size));
        read_ok = static_cast<bool>(file);
        return payload_size;
      });
  if (!read_ok) {
    return sanitize::Status::IOError("Parquet page payload: failed reading");
  }
  return {};
}

sanitize::Result<std::string> read_exact_payload(std::ifstream &file,
                                                 std::int64_t offset,
                                                 std::int32_t size) {
  std::string payload;
  SAN_RETURN_NOT_OK(read_exact_payload_into(file, offset, size, &payload));
  return payload;
}

std::uint64_t
remaining_page_derived_budget(const PageVerificationScratch &scratch) noexcept {
  return scratch.derived_bytes >= scratch.max_derived_bytes
             ? 0
             : scratch.max_derived_bytes - scratch.derived_bytes;
}

sanitize::Status verify_page_payload(std::ifstream &file,
                                     const ColumnChunkInfo &column,
                                     PageHeaderInfo *page,
                                     DictionaryPageState *dictionary,
                                     PageVerificationScratch *scratch) {
  if (!page || !scratch || !column.has_codec ||
      !page->has_compressed_page_size || !page->has_uncompressed_page_size) {
    return {};
  }
  if (page->compressed_page_size < 0 || page->uncompressed_page_size < 0) {
    return sanitize::Status::Invalid(
        "Parquet page payload: negative page size");
  }
  const auto compressed_size =
      static_cast<std::size_t>(page->compressed_page_size);
  const auto uncompressed_size =
      static_cast<std::size_t>(page->uncompressed_page_size);
  if (compressed_size > scratch->max_page_bytes ||
      uncompressed_size > scratch->max_page_bytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: page exceeds effective operation limit ",
        scratch->max_page_bytes);
  }
  if (column.codec != kCompressionUncompressed &&
      compressed_size > scratch->max_page_bytes - uncompressed_size) {
    return sanitize::Status::Invalid(
        "Parquet page payload: compressed and decompressed scratch exceed "
        "effective operation limit ",
        scratch->max_page_bytes);
  }
  SAN_RETURN_NOT_OK(charge_page_verification_budget(scratch, compressed_size,
                                                    uncompressed_size));
  if (compressed_size > kMaxPayloadVerificationBytes ||
      uncompressed_size > kMaxPayloadVerificationBytes) {
    page->payload_verification_skipped = true;
    return {};
  }
  if (column.codec != kCompressionUncompressed &&
      column.codec != kCompressionSnappy && column.codec != kCompressionGzip) {
    return {};
  }
#if !defined(SCHEMA_SANITIZER_HAS_ZLIB)
  if (column.codec == kCompressionGzip) {
    page->payload_verification_skipped = true;
    return {};
  }
#endif
  SAN_RETURN_NOT_OK(read_exact_payload_into(
      file, page->compressed_payload_offset, page->compressed_page_size,
      &scratch->compressed_payload));
  SAN_RETURN_NOT_OK(
      validate_parquet_page_checksum(*page, scratch->compressed_payload));
  if (column.codec == kCompressionUncompressed) {
    page->decompressed_page_size = page->compressed_page_size;
    page->has_decompressed_page_size = true;
    page->payload_verified =
        page->compressed_page_size == page->uncompressed_page_size;
    if (!page->payload_verified) {
      scratch->retire_compressed_payload();
      return sanitize::Status::Invalid(
          "Parquet page payload: uncompressed size mismatch");
    }
    const auto decode_status = decode_verified_page_payload(
        scratch->compressed_payload, column, page, dictionary,
        remaining_page_derived_budget(*scratch));
    scratch->retire_compressed_payload();
    SAN_RETURN_NOT_OK(decode_status);
    return charge_page_derived_budget(scratch, *page);
  }
  if (column.codec == kCompressionSnappy) {
    const auto decompress_status = snappy_decompress_payload_into(
        scratch->compressed_payload, page->uncompressed_page_size,
        &scratch->decompressed_payload);
    scratch->retire_compressed_payload();
    if (!decompress_status.ok()) {
      scratch->retire_decompressed_payload();
      return decompress_status;
    }
    page->decompressed_page_size =
        static_cast<std::int32_t>(scratch->decompressed_payload.size());
    page->has_decompressed_page_size = true;
    page->payload_verified = true;
    const auto decode_status = decode_verified_page_payload(
        scratch->decompressed_payload, column, page, dictionary,
        remaining_page_derived_budget(*scratch));
    scratch->retire_decompressed_payload();
    SAN_RETURN_NOT_OK(decode_status);
    return charge_page_derived_budget(scratch, *page);
  }
  if (column.codec == kCompressionGzip) {
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
    const auto decompress_status = gzip_decompress_payload_into(
        scratch->compressed_payload, page->uncompressed_page_size,
        &scratch->decompressed_payload);
    scratch->retire_compressed_payload();
    if (!decompress_status.ok()) {
      scratch->retire_decompressed_payload();
      return decompress_status;
    }
    page->decompressed_page_size =
        static_cast<std::int32_t>(scratch->decompressed_payload.size());
    page->has_decompressed_page_size = true;
    page->payload_verified = true;
    const auto decode_status = decode_verified_page_payload(
        scratch->decompressed_payload, column, page, dictionary,
        remaining_page_derived_budget(*scratch));
    scratch->retire_decompressed_payload();
    SAN_RETURN_NOT_OK(decode_status);
    return charge_page_derived_budget(scratch, *page);
#else
    scratch->retire_compressed_payload();
    page->payload_verification_skipped = true;
    return {};
#endif
  }
  return {};
}

sanitize::Status read_page_headers_for_column(std::ifstream &file,
                                              ColumnChunkInfo *column,
                                              PageVerificationScratch *scratch,
                                              std::uint64_t data_end) {
  if (!column || !scratch || !column->has_total_compressed_size ||
      column->total_compressed_size <= 0) {
    return {};
  }
  if (column->has_num_values && column->num_values == 0) {
    column->pages.clear();
    column->decoded_dictionary_values.clear();
    column->decoded_dictionary_fixed_width_values.clear();
    return {};
  }
  std::string &page_header_bytes = scratch->page_header_bytes;
  const bool has_dictionary = column->has_dictionary_page_offset;
  if (!has_dictionary && !column->has_data_page_offset) {
    return {};
  }
  const auto selected_offset = has_dictionary ? column->dictionary_page_offset
                                              : column->data_page_offset;
  if (selected_offset < 0 ||
      (column->has_data_page_offset && column->data_page_offset < 0) ||
      (has_dictionary && column->dictionary_page_offset < 0)) {
    return sanitize::Status::Invalid(
        "Parquet page header: negative page offset");
  }
  auto start = static_cast<std::uint64_t>(selected_offset);
  const auto data_offset =
      column->has_data_page_offset
          ? static_cast<std::uint64_t>(column->data_page_offset)
          : start;
  if (has_dictionary && data_offset < start) {
    return sanitize::Status::Invalid(
        "Parquet page header: dictionary page follows data page");
  }
  const auto compressed_size =
      static_cast<std::uint64_t>(column->total_compressed_size);
  if (start > data_end || compressed_size > data_end - start) {
    return sanitize::Status::Invalid(
        "Parquet page header: column chunk exceeds data section");
  }
  const auto limit = start + compressed_size;
  if (data_offset < start || data_offset >= limit) {
    return sanitize::Status::Invalid(
        "Parquet page header: data page offset is outside column chunk");
  }
  column->pages.clear();
  std::uint64_t offset = start;
  std::int64_t data_values = 0;
  DictionaryPageState dictionary;
  while (offset < limit) {
    if (column->pages.size() >= kMaxPagesPerColumn) {
      return sanitize::Status::Invalid(
          "Parquet page header: page count exceeds security limit");
    }
    PageHeaderInfo page;
    SAN_ASSIGN_OR_RAISE(
        page, read_page_header_at(file, offset, limit, page_header_bytes));
    SAN_RETURN_NOT_OK(
        verify_page_payload(file, *column, &page, &dictionary, scratch));
    const auto next_offset =
        offset + static_cast<std::uint64_t>(page.header_size) +
        static_cast<std::uint64_t>(page.compressed_page_size);
    if (next_offset <= offset) {
      return sanitize::Status::Invalid(
          "Parquet page header: parser did not advance");
    }
    if (!page.is_dictionary_page && page.has_num_values) {
      data_values += page.num_values;
    }
    column->pages.push_back(std::move(page));
    offset = next_offset;
    if (column->has_num_values && data_values >= column->num_values) {
      break;
    }
  }
  column->decoded_dictionary_values = std::move(dictionary.byte_array_values);
  column->decoded_dictionary_fixed_width_values =
      std::move(dictionary.fixed_width_values);
  return {};
}

sanitize::Status read_page_headers(
    std::ifstream &file, FooterInfo *info,
    std::size_t max_page_bytes = kMaxPayloadVerificationBytes,
    std::uint64_t max_row_group_bytes = kMaxDecompressedBytesPerRowGroup) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet page header: internal error");
  }
  file.clear();
  file.seekg(0, std::ios::end);
  const auto end_pos = file.tellg();
  if (end_pos < static_cast<std::streampos>(8)) {
    return sanitize::Status::Invalid("Parquet page header: file is too small");
  }
  const auto file_size = static_cast<std::uint64_t>(end_pos);
  std::array<char, 8> trailer{};
  file.seekg(-8, std::ios::end);
  file.read(trailer.data(), static_cast<std::streamsize>(trailer.size()));
  if (!file || std::string_view(trailer.data() + 4, 4) != kParquetMagic) {
    return sanitize::Status::Invalid(
        "Parquet page header: invalid trailing magic");
  }
  const auto footer_len =
      static_cast<std::uint64_t>(read_u32_le(trailer.data()));
  if (footer_len > file_size - 8) {
    return sanitize::Status::Invalid(
        "Parquet page header: invalid footer range");
  }
  const auto data_end = file_size - 8 - footer_len;

  PageVerificationScratch scratch;
  scratch.max_page_bytes =
      std::min(max_page_bytes, kMaxPayloadVerificationBytes);
  scratch.max_decompressed_bytes = std::min<std::uint64_t>(
      max_row_group_bytes, kMaxDecompressedBytesPerRowGroup);
  scratch.max_derived_bytes =
      std::min<std::uint64_t>(max_row_group_bytes, kMaxDerivedBytesPerRowGroup);
  scratch.page_header_bytes.reserve(256);
  for (auto &row_group : info->row_groups) {
    scratch.reset_budget();
    for (auto &column : row_group.columns) {
      SAN_RETURN_NOT_OK(
          read_page_headers_for_column(file, &column, &scratch, data_end));
    }
  }
  return {};
}

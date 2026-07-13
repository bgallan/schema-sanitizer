struct PageVerificationScratch {
  std::string compressed_payload;
  std::string decompressed_payload;
};

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
  const auto page_end = offset + static_cast<std::uint64_t>(page.header_size) +
                        static_cast<std::uint64_t>(page.compressed_page_size);
  if (page_end > limit) {
    return sanitize::Status::Invalid("Parquet page header: page exceeds chunk");
  }
  return page;
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
  if (column.codec == kCompressionUncompressed) {
    page->decompressed_page_size = page->compressed_page_size;
    page->has_decompressed_page_size = true;
    page->payload_verified =
        page->compressed_page_size == page->uncompressed_page_size;
    if (!page->payload_verified) {
      return sanitize::Status::Invalid(
          "Parquet page payload: uncompressed size mismatch");
    }
    SAN_RETURN_NOT_OK(decode_verified_page_payload(scratch->compressed_payload,
                                                   column, page, dictionary));
    return {};
  }
  if (column.codec == kCompressionSnappy) {
    SAN_RETURN_NOT_OK(snappy_decompress_payload_into(
        scratch->compressed_payload, page->uncompressed_page_size,
        &scratch->decompressed_payload));
    page->decompressed_page_size =
        static_cast<std::int32_t>(scratch->decompressed_payload.size());
    page->has_decompressed_page_size = true;
    page->payload_verified = true;
    SAN_RETURN_NOT_OK(decode_verified_page_payload(
        scratch->decompressed_payload, column, page, dictionary));
    return {};
  }
  if (column.codec == kCompressionGzip) {
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
    SAN_RETURN_NOT_OK(gzip_decompress_payload_into(
        scratch->compressed_payload, page->uncompressed_page_size,
        &scratch->decompressed_payload));
    page->decompressed_page_size =
        static_cast<std::int32_t>(scratch->decompressed_payload.size());
    page->has_decompressed_page_size = true;
    page->payload_verified = true;
    SAN_RETURN_NOT_OK(decode_verified_page_payload(
        scratch->decompressed_payload, column, page, dictionary));
#else
    page->payload_verification_skipped = true;
#endif
    return {};
  }
  return {};
}

sanitize::Status
read_page_headers_for_column(std::ifstream &file, ColumnChunkInfo *column,
                             std::string &page_header_bytes,
                             PageVerificationScratch *scratch) {
  if (!column || !column->has_total_compressed_size ||
      column->total_compressed_size <= 0) {
    return {};
  }
  if (column->has_num_values && column->num_values == 0) {
    column->pages.clear();
    column->decoded_dictionary_values.clear();
    column->decoded_dictionary_fixed_width_values.clear();
    return {};
  }
  const bool has_dictionary = column->has_dictionary_page_offset;
  if (!has_dictionary && !column->has_data_page_offset) {
    return {};
  }
  auto start =
      static_cast<std::uint64_t>(has_dictionary ? column->dictionary_page_offset
                                                : column->data_page_offset);
  const auto data_offset =
      column->has_data_page_offset
          ? static_cast<std::uint64_t>(column->data_page_offset)
          : start;
  if (has_dictionary) {
    start = std::min(start, data_offset);
  }
  const auto limit =
      start + static_cast<std::uint64_t>(column->total_compressed_size);
  column->pages.clear();
  std::uint64_t offset = start;
  std::int64_t data_values = 0;
  DictionaryPageState dictionary;
  while (offset < limit) {
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

sanitize::Status read_page_headers(std::ifstream &file, FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet page header: internal error");
  }
  std::string page_header_bytes;
  page_header_bytes.reserve(256);
  PageVerificationScratch scratch;
  for (auto &row_group : info->row_groups) {
    for (auto &column : row_group.columns) {
      SAN_RETURN_NOT_OK(read_page_headers_for_column(
          file, &column, page_header_bytes, &scratch));
    }
  }
  return {};
}

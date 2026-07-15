constexpr std::size_t kRetainedPagePayloadScratchBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::size_t kRetainedPageHeaderScratchBytes = 64ULL * 1024ULL;

void clear_or_release_page_scratch(std::string *value,
                                   std::size_t retained_capacity) noexcept {
  if (!value) {
    return;
  }
  if (secure_memory_cleanup_enabled() && !value->empty()) {
    secure_zero_memory(value->data(), value->size());
  }
  if (value->capacity() > retained_capacity) {
    std::string empty;
    value->swap(empty);
    return;
  }
  value->clear();
}

struct PageVerificationScratch {
  std::string page_header_bytes;
  std::string compressed_payload;
  std::string decompressed_payload;
  std::uint64_t compressed_bytes = 0;
  std::uint64_t decompressed_bytes = 0;
  std::uint64_t derived_bytes = 0;

  ~PageVerificationScratch() {
    clear_or_release_page_scratch(&page_header_bytes, 0);
    clear_or_release_page_scratch(&compressed_payload, 0);
    clear_or_release_page_scratch(&decompressed_payload, 0);
  }

  void reset_budget() noexcept {
    clear_or_release_page_scratch(&page_header_bytes,
                                  kRetainedPageHeaderScratchBytes);
    clear_or_release_page_scratch(&compressed_payload,
                                  kRetainedPagePayloadScratchBytes);
    clear_or_release_page_scratch(&decompressed_payload,
                                  kRetainedPagePayloadScratchBytes);
    compressed_bytes = 0;
    decompressed_bytes = 0;
    derived_bytes = 0;
  }

  void retire_compressed_payload() noexcept {
    clear_or_release_page_scratch(&compressed_payload,
                                  kRetainedPagePayloadScratchBytes);
  }

  void retire_decompressed_payload() noexcept {
    clear_or_release_page_scratch(&decompressed_payload,
                                  kRetainedPagePayloadScratchBytes);
  }
};

sanitize::Status charge_page_verification_budget(
    PageVerificationScratch *scratch, std::size_t compressed,
    std::size_t decompressed) {
  if (!scratch) {
    return sanitize::Status::Invalid(
        "Parquet page payload: verification scratch is null");
  }
  if (compressed > std::numeric_limits<std::uint64_t>::max() -
                       scratch->compressed_bytes ||
      decompressed > std::numeric_limits<std::uint64_t>::max() -
                         scratch->decompressed_bytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: decompression budget overflow");
  }
  const auto next_compressed =
      scratch->compressed_bytes + static_cast<std::uint64_t>(compressed);
  const auto next_decompressed =
      scratch->decompressed_bytes + static_cast<std::uint64_t>(decompressed);
  if (next_decompressed > kMaxDecompressedBytesPerRowGroup) {
    return sanitize::Status::Invalid(
        "Parquet page payload: row-group decompression budget exceeded");
  }
  constexpr std::uint64_t kSmallPayloadAllowance = 64ULL * 1024ULL * 1024ULL;
  const auto ratio_limit =
      next_compressed >
              std::numeric_limits<std::uint64_t>::max() /
                  kMaxDecompressionExpansionRatio
          ? std::numeric_limits<std::uint64_t>::max()
          : next_compressed * kMaxDecompressionExpansionRatio;
  if (next_decompressed > std::max(kSmallPayloadAllowance, ratio_limit)) {
    return sanitize::Status::Invalid(
        "Parquet page payload: decompression expansion ratio exceeded");
  }
  scratch->compressed_bytes = next_compressed;
  scratch->decompressed_bytes = next_decompressed;
  return {};
}

sanitize::Status charge_page_derived_budget(PageVerificationScratch *scratch,
                                             const PageHeaderInfo &page) {
  if (!scratch) {
    return sanitize::Status::Invalid(
        "Parquet page payload: verification scratch is null");
  }
  std::uint64_t bytes = 0;
  const auto charge = [&](std::int32_t value) -> bool {
    if (value <= 0) {
      return true;
    }
    const auto amount = static_cast<std::uint64_t>(value);
    if (amount > std::numeric_limits<std::uint64_t>::max() - bytes) {
      return false;
    }
    bytes += amount;
    return true;
  };
  if (!charge(page.decoded_validity_bytes) ||
      !charge(page.materialized_offset_bytes) ||
      !charge(page.materialized_value_bytes)) {
    return sanitize::Status::Invalid(
        "Parquet page payload: derived buffer budget overflow");
  }
  const auto definition_bytes =
      static_cast<std::uint64_t>(page.decoded_definition_level_values.size()) *
      sizeof(std::int16_t);
  const auto repetition_bytes =
      static_cast<std::uint64_t>(page.decoded_repetition_level_values.size()) *
      sizeof(std::int16_t);
  if (definition_bytes > std::numeric_limits<std::uint64_t>::max() - bytes ||
      repetition_bytes > std::numeric_limits<std::uint64_t>::max() -
                             bytes - definition_bytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: derived level budget overflow");
  }
  bytes += definition_bytes + repetition_bytes;
  if (bytes > std::numeric_limits<std::uint64_t>::max() -
                  scratch->derived_bytes ||
      scratch->derived_bytes + bytes > kMaxDerivedBytesPerRowGroup) {
    return sanitize::Status::Invalid(
        "Parquet page payload: row-group derived buffer budget exceeded");
  }
  scratch->derived_bytes += bytes;
  return {};
}

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
  SAN_RETURN_NOT_OK(charge_page_verification_budget(
      scratch, compressed_size, uncompressed_size));
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
      scratch->retire_compressed_payload();
      return sanitize::Status::Invalid(
          "Parquet page payload: uncompressed size mismatch");
    }
    const auto decode_status = decode_verified_page_payload(
        scratch->compressed_payload, column, page, dictionary);
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
        scratch->decompressed_payload, column, page, dictionary);
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
        scratch->decompressed_payload, column, page, dictionary);
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

sanitize::Status
read_page_headers_for_column(std::ifstream &file, ColumnChunkInfo *column,
                             PageVerificationScratch *scratch) {
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

sanitize::Status read_page_headers(std::ifstream &file, FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet page header: internal error");
  }
  PageVerificationScratch scratch;
  scratch.page_header_bytes.reserve(256);
  for (auto &row_group : info->row_groups) {
    scratch.reset_budget();
    for (auto &column : row_group.columns) {
      SAN_RETURN_NOT_OK(
          read_page_headers_for_column(file, &column, &scratch));
    }
  }
  return {};
}

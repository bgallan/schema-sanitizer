constexpr std::size_t kRetainedPagePayloadScratchBytes =
    8ULL * 1024ULL * 1024ULL;
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
  std::size_t max_page_bytes = kMaxPayloadVerificationBytes;
  std::uint64_t max_decompressed_bytes = kMaxDecompressedBytesPerRowGroup;
  std::uint64_t max_derived_bytes = kMaxDerivedBytesPerRowGroup;

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

sanitize::Status
charge_page_verification_budget(PageVerificationScratch *scratch,
                                std::size_t compressed,
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
  if (next_decompressed > scratch->max_decompressed_bytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: row-group decompression budget exceeded ",
        scratch->max_decompressed_bytes);
  }
  constexpr std::uint64_t kSmallPayloadAllowance = 64ULL * 1024ULL * 1024ULL;
  const auto ratio_limit =
      next_compressed > std::numeric_limits<std::uint64_t>::max() /
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
      repetition_bytes > std::numeric_limits<std::uint64_t>::max() - bytes -
                             definition_bytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: derived level budget overflow");
  }
  bytes += definition_bytes + repetition_bytes;
  if (bytes >
          std::numeric_limits<std::uint64_t>::max() - scratch->derived_bytes ||
      scratch->derived_bytes + bytes > scratch->max_derived_bytes) {
    return sanitize::Status::Invalid(
        "Parquet page payload: row-group derived buffer budget exceeded ",
        scratch->max_derived_bytes);
  }
  scratch->derived_bytes += bytes;
  return {};
}

// Normalizes writer options, applies safety caps, and rejects unavailable
// compression codecs. This fragment is included by the consolidated writer to
// isolate format-specific details.

/// Applies native Parquet defaults and hard caps to caller-provided writer
/// options.
WriterOptions normalized_writer_options(const WriterOptions &input) {
  WriterOptions out = input;
  if (out.compression.empty()) {
    out.compression = "gzip";
  }
  return out;
}

/// Reports whether any writer leaf requires definition or repetition-level
/// encoding.
bool parquet_columns_have_nested_levels(
    const std::vector<LeafColumn> &columns) noexcept {
  for (const auto &column : columns) {
    if (column.max_repetition_level > 0 || column.max_definition_level > 1) {
      return true;
    }
  }
  return false;
}

/// Derives a row-group byte limit that adapts to nesting while staying below
/// the hard writer cap.
std::int64_t
adaptive_max_bytes_per_row_group(const std::vector<LeafColumn> &columns,
                                 std::int64_t configured_value) {
  bool has_repeated = false;
  for (const auto &column : columns) {
    has_repeated = has_repeated || column.max_repetition_level > 0;
  }
  std::int64_t adaptive = configured_value;
  if (columns.size() >= 512) {
    adaptive = std::min<std::int64_t>(adaptive, 16LL * 1024LL * 1024LL);
  } else if (columns.size() >= 128 || has_repeated) {
    adaptive = std::min<std::int64_t>(adaptive, 32LL * 1024LL * 1024LL);
  }
  return std::max<std::int64_t>(adaptive, 1024LL * 1024LL);
}

/// Reduces row-group staging capacity so concurrent writer tasks remain within
/// memory limits.
std::int64_t parallel_safe_max_bytes_per_row_group(
    const WriterOptions &options,
    const sanitize::internal::MemoryBudget &budget,
    const std::vector<LeafColumn> &columns) {
  auto target =
      adaptive_max_bytes_per_row_group(columns, budget.parquet_row_group_bytes);
  const auto nested = parquet_columns_have_nested_levels(columns);
  if (options.threading_mode == sanitize::ThreadingMode::kSingle ||
      columns.size() < 2 || budget.total_bytes < 96LL * 1024LL * 1024LL ||
      (!nested && columns.size() <= 4)) {
    return target;
  }

  // Parallel column preparation retains three classes of memory at once:
  // collected Arrow values, encoded/compressed artifacts waiting for ordinal
  // commit, and one page-sized candidate workspace per active worker. Keep the
  // complete logical row group to at most one quarter of the operation budget;
  // this leaves a second quarter for prepared artifacts and the remaining half
  // for page candidates, the source batch, footer metadata, and coordinator
  // state. The row-group boundary is a physical-layout detail, so narrowing it
  // in multi mode does not alter logical Parquet contents.
  const auto parallel_cap =
      std::max<std::int64_t>(1024LL * 1024LL, budget.total_bytes / 4);
  return std::min(target, parallel_cap);
}

/// Maps normalized writer options to the supported physical Parquet compression
/// codec.
sanitize::Result<CompressionCodec>
compression_codec_from_options(const WriterOptions &options) {
  const std::string_view value(options.compression);
  if (value == "uncompressed") {
    return CompressionCodec::kUncompressed;
  }
  if (value == "snappy") {
    return CompressionCodec::kSnappy;
  }
  if (value == "gzip") {
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
    return CompressionCodec::kGzip;
#else
    return sanitize::Status::Invalid(
        "native Parquet writer: gzip compression requested but zlib is not "
        "available");
#endif
  }
  return sanitize::Status::Invalid(
      "native Parquet writer: unsupported compression '", value,
      "'; expected gzip, snappy, or uncompressed");
}

/// Returns the normalized GZIP level used by native Parquet compression.
sanitize::Result<int> gzip_level_from_options(const WriterOptions &options) {
  if (options.gzip_level < -1 || options.gzip_level > 9) {
    return sanitize::Status::Invalid(
        "native Parquet writer: gzip level must be -1 or from 0 to 9");
  }
  return options.gzip_level;
}

std::optional<std::string> env_value(const char *name) {
  if (!name || *name == '\0') {
    return std::nullopt;
  }
#if defined(_MSC_VER)
  char *raw = nullptr;
  std::size_t size = 0;
  if (_dupenv_s(&raw, &size, name) != 0 || !raw) {
    return std::nullopt;
  }
  std::string value(raw, size > 0 ? size - 1 : 0);
  std::free(raw);
  return value;
#else
  const char *raw = std::getenv(name);
  if (!raw) {
    return std::nullopt;
  }
  return std::string(raw);
#endif
}

std::int64_t configured_max_rows_per_row_group() {
  const auto raw = env_value("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_ROWS");
  if (!raw || raw->empty()) {
    return kDefaultMaxRowsPerRowGroup;
  }
  char *end = nullptr;
  const char *begin = raw->c_str();
  const auto parsed = std::strtoll(begin, &end, 10);
  if (end == begin || (end && *end != '\0') || parsed <= 0) {
    return kDefaultMaxRowsPerRowGroup;
  }
  return parsed;
}

std::int64_t configured_positive_i64_env(const char *name,
                                         std::int64_t default_value) {
  const auto raw = env_value(name);
  if (!raw || raw->empty()) {
    return default_value;
  }
  char *end = nullptr;
  const char *begin = raw->c_str();
  const auto parsed = std::strtoll(begin, &end, 10);
  if (end == begin || (end && *end != '\0') || parsed <= 0) {
    return default_value;
  }
  return parsed;
}

std::int64_t configured_max_bytes_per_row_group() {
  return configured_positive_i64_env(
      "SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES",
      kDefaultMaxBytesPerRowGroup);
}

bool has_configured_max_bytes_per_row_group() {
  const auto raw = env_value("SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES");
  return raw && !raw->empty();
}

std::int64_t
adaptive_max_bytes_per_row_group(const std::vector<LeafColumn> &columns,
                                 std::int64_t configured_value) {
  if (has_configured_max_bytes_per_row_group()) {
    return configured_value;
  }
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

std::int64_t configured_target_page_bytes() {
  return configured_positive_i64_env(
      "SCHEMA_SANITIZER_NATIVE_PARQUET_PAGE_BYTES", kDefaultTargetPageBytes);
}

sanitize::Result<CompressionCodec> configured_compression_codec() {
  const auto raw = env_value("SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION");
  if (!raw || raw->empty()) {
#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
    return CompressionCodec::kGzip;
#else
    return sanitize::Status::Invalid(
        "native Parquet writer: gzip compression is the default but zlib is "
        "not available; set SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION="
        "uncompressed to write uncompressed output");
#endif
  }
  const std::string_view value(*raw);
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

sanitize::Result<int> configured_gzip_level() {
  const auto raw = env_value("SCHEMA_SANITIZER_NATIVE_PARQUET_GZIP_LEVEL");
  if (!raw || raw->empty()) {
    return -1; // zlib default compression level.
  }
  char *end = nullptr;
  const char *begin = raw->c_str();
  const auto parsed = std::strtol(begin, &end, 10);
  if (end == begin || (end && *end != '\0') || parsed < 0 || parsed > 9) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid gzip level '", *raw,
        "'; expected an integer from 0 to 9");
  }
  return static_cast<int>(parsed);
}

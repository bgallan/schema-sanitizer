WriterOptions normalized_writer_options(const WriterOptions &input) {
  WriterOptions out = input;
  if (out.compression.empty()) {
    out.compression = "gzip";
  }
  return out;
}

std::int64_t adaptive_max_bytes_per_row_group(
    const std::vector<LeafColumn> &columns,
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

sanitize::Result<int> gzip_level_from_options(const WriterOptions &options) {
  if (options.gzip_level < -1 || options.gzip_level > 9) {
    return sanitize::Status::Invalid(
        "native Parquet writer: gzip level must be -1 or from 0 to 9");
  }
  return options.gzip_level;
}

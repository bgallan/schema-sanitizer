// Internal construction helpers for ingestion chunk sources.

#pragma once

#include "sanitize/ingest/chunk_source.hh"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace sanitize::internal {

inline constexpr std::int64_t kMaxChunkRequestBytes =
    std::int64_t{256} * 1024 * 1024;

// Rejects chunk requests that could turn one caller mistake into a single
// enormous allocation.
sanitize::Status validate_chunk_request(std::int64_t max_bytes,
                                        std::string_view operation);

// Validates current + additional without overflowing before a full input view
// grows.
sanitize::Status validate_materialized_input_growth(
    std::string_view operation, std::string_view source, std::uint64_t current, std::uint64_t additional,
    std::uint64_t limit);

enum class TextEncoding : std::uint8_t {
  kUtf8 = 0,
  kLatin1,
  kUtf16,
  kUtf16LE,
  kUtf16BE,
  kUnsupported,
};

TextEncoding parse_text_encoding(std::string_view encoding);
std::string_view text_encoding_name(TextEncoding encoding);

sanitize::Status ensure_uncompressed_file(const std::string &path,
                                          std::string_view operation);

ChunkSourcePtr make_memory_chunk_source(std::string bytes);
ChunkSourcePtr make_file_chunk_source(std::string path,
                                      std::int64_t memory_limit_bytes);
ChunkSourcePtr make_transcoding_file_chunk_source(
    std::string path, TextEncoding encoding, std::int64_t memory_limit_bytes);
ChunkSourcePtr
make_multi_path_chunk_source(std::vector<std::string> paths,
                             std::vector<std::string> source_names,
                             std::string separator, TextEncoding encoding,
                             std::int64_t memory_limit_bytes);

} // namespace sanitize::internal

// Internal construction helpers for ingestion chunk sources.

#pragma once

#include "sanitize/ingest/chunk_source.hh"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace sanitize::internal {

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

sanitize::Result<std::string> read_file_bytes(const std::string &path);
sanitize::Status ensure_uncompressed_file(const std::string &path,
                                          std::string_view operation);

ChunkSourcePtr make_memory_chunk_source(std::string bytes);
ChunkSourcePtr make_file_chunk_source(std::string path);
ChunkSourcePtr make_transcoding_file_chunk_source(std::string path,
                                                  TextEncoding encoding);
ChunkSourcePtr
make_multi_path_chunk_source(std::vector<std::string> paths,
                             std::vector<std::string> source_names,
                             std::string separator, TextEncoding encoding);

} // namespace sanitize::internal

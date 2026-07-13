// Implements public chunk-source factories and argument validation.

#include "ingest/chunk_source_detail.hh"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize {
namespace {

sanitize::Result<internal::TextEncoding>
validated_text_encoding(std::string_view encoding) {
  const auto parsed = internal::parse_text_encoding(encoding);
  if (parsed == internal::TextEncoding::kUnsupported) {
    return sanitize::Status::NotImplemented("unsupported input_text_encoding: ",
                                            std::string(encoding));
  }
  return parsed;
}

sanitize::Status validate_paths(const std::vector<std::string> &paths,
                                std::string_view operation) {
  if (paths.empty()) {
    return sanitize::Status::Invalid(operation, ": no paths");
  }
  return {};
}

sanitize::Status
validate_source_names(const std::vector<std::string> &paths,
                      const std::vector<std::string> &source_names,
                      std::string_view operation) {
  SAN_RETURN_NOT_OK(validate_paths(paths, operation));
  if (paths.size() != source_names.size()) {
    return sanitize::Status::Invalid(operation,
                                     ": paths/source_names size mismatch");
  }
  return {};
}

} // namespace

sanitize::Result<ChunkSourcePtr>
chunk_source_from_path(const std::string &path) {
  SAN_RETURN_NOT_OK(
      internal::ensure_uncompressed_file(path, "chunk_source_from_path"));
  return internal::make_file_chunk_source(path);
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_path_with_encoding(const std::string &path,
                                     std::string_view encoding) {
  SAN_ASSIGN_OR_RAISE(const auto parsed, validated_text_encoding(encoding));
  if (parsed == internal::TextEncoding::kUtf8) {
    return chunk_source_from_path(path);
  }
  SAN_RETURN_NOT_OK(internal::ensure_uncompressed_file(
      path, "chunk_source_from_path_with_encoding"));
  return internal::make_transcoding_file_chunk_source(path, parsed);
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths(std::vector<std::string> paths, std::string separator) {
  SAN_RETURN_NOT_OK(validate_paths(paths, "chunk_source_from_paths"));
  return internal::make_multi_path_chunk_source(std::move(paths), {},
                                                std::move(separator),
                                                internal::TextEncoding::kUtf8);
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_encoding(std::vector<std::string> paths,
                                      std::string separator,
                                      std::string_view encoding) {
  SAN_RETURN_NOT_OK(
      validate_paths(paths, "chunk_source_from_paths_with_encoding"));
  SAN_ASSIGN_OR_RAISE(const auto parsed, validated_text_encoding(encoding));
  return internal::make_multi_path_chunk_source(std::move(paths), {},
                                                std::move(separator), parsed);
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_source_names(std::vector<std::string> paths,
                                          std::vector<std::string> source_names,
                                          std::string separator) {
  SAN_RETURN_NOT_OK(validate_source_names(
      paths, source_names, "chunk_source_from_paths_with_source_names"));
  return internal::make_multi_path_chunk_source(
      std::move(paths), std::move(source_names), std::move(separator),
      internal::TextEncoding::kUtf8);
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_source_names_encoding(
    std::vector<std::string> paths, std::vector<std::string> source_names,
    std::string separator, std::string_view encoding) {
  SAN_RETURN_NOT_OK(validate_source_names(
      paths, source_names,
      "chunk_source_from_paths_with_source_names_encoding"));
  SAN_ASSIGN_OR_RAISE(const auto parsed, validated_text_encoding(encoding));
  return internal::make_multi_path_chunk_source(
      std::move(paths), std::move(source_names), std::move(separator), parsed);
}

} // namespace sanitize

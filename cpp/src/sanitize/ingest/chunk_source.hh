// Declares byte chunk sources consumed by ingestion frontends.
// Memory, file, and multi-path implementations share bounded reset, read, size,
// and source-identity contracts without exposing their storage mechanism.

#pragma once

#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace sanitize {

// One contiguous chunk of bytes.
//
// The chunk may alias memory owned by `owner`.
struct Chunk {
  // Keep chunk memory alive.
  std::shared_ptr<const void> owner;

  // View of bytes.
  std::string_view data;

  // Best-effort byte offset of `data.data()` within the underlying source.
  std::size_t base_offset = 0;

  // Optional display name of the source file backing this chunk.
  std::shared_ptr<const std::string> source_name_owner;
  std::string_view source_name;
  std::size_t source_index = 0;
  bool has_source_index = false;
};

// Replayable chunked byte source.
//
// Design goals:
// - Support contiguous views (fast path for mmap'd files and in-memory
// buffers).
// - Support chunked iteration for streaming sources.
// - Support Reset() so inference + materialization can replay the same input.
class ChunkSource {
public:
  /// Allows implementations to release owned input resources polymorphically.
  virtual ~ChunkSource() = default;

  /// Resets the source to the beginning.
  virtual sanitize::Status Reset() = 0;

  /// Returns the next bounded byte chunk, or an empty chunk at end of input.
  virtual sanitize::Result<Chunk> NextChunk(int64_t max_bytes) = 0;

  /// Returns a stable contiguous view, materializing storage when necessary.
  virtual sanitize::Result<Chunk> View() = 0;
};

using ChunkSourcePtr = std::shared_ptr<ChunkSource>;

/// Creates a source that owns its bytes.
ChunkSourcePtr chunk_source_from_bytes(std::string bytes);

/// Creates a source that reads bytes from one local file.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_path(const std::string &path,
                       std::int64_t memory_limit_bytes = -1);

/// Creates a file source that transcodes supported text encodings to UTF-8.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_path_with_encoding(const std::string &path,
                                     std::string_view encoding,
                                     std::int64_t memory_limit_bytes = -1);

/// Creates one logical byte stream from files separated by the supplied bytes.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths(std::vector<std::string> paths, std::string separator,
                        std::int64_t memory_limit_bytes = -1);

/// Creates one logical UTF-8 stream by transcoding and separating local files.
sanitize::Result<ChunkSourcePtr> chunk_source_from_paths_with_encoding(
    std::vector<std::string> paths, std::string separator,
    std::string_view encoding, std::int64_t memory_limit_bytes = -1);

/// Creates one logical file stream whose chunks carry caller-provided names.
sanitize::Result<ChunkSourcePtr> chunk_source_from_paths_with_source_names(
    std::vector<std::string> paths, std::vector<std::string> source_names,
    std::string separator, std::int64_t memory_limit_bytes = -1);

/// Creates one named logical UTF-8 stream by transcoding and separating files.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_source_names_encoding(
    std::vector<std::string> paths, std::vector<std::string> source_names,
    std::string separator, std::string_view encoding,
    std::int64_t memory_limit_bytes = -1);

} // namespace sanitize

// Declares byte chunk sources consumed by ingestion frontends.

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
  // Destroys the ChunkSource.
  virtual ~ChunkSource() = default;

  // Reset the source to the beginning.
  virtual sanitize::Status Reset() = 0;

  // Return the next chunk of bytes (up to max_bytes). An empty chunk indicates
  // EOF.
  virtual sanitize::Result<Chunk> NextChunk(int64_t max_bytes) = 0;

  // Return a stable contiguous view of the entire source.
  //
  // Implementations may memory-map, zero-copy slice, or materialize by reading.
  virtual sanitize::Result<Chunk> View() = 0;
};

using ChunkSourcePtr = std::shared_ptr<ChunkSource>;

// Creates a source that owns its bytes.
ChunkSourcePtr chunk_source_from_bytes(std::string bytes);

// Creates a source from file bytes.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_path(const std::string &path);

// Creates a source from file bytes, transcoding supported text encodings to
// UTF-8 before frontend parsing.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_path_with_encoding(const std::string &path,
                                     std::string_view encoding);

// Creates a source that reads multiple local files as one logical byte stream.
// A separator is emitted between adjacent files.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths(std::vector<std::string> paths, std::string separator);

// Creates a source that reads multiple local files as one logical UTF-8 byte
// stream, transcoding supported per-file text encodings.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_encoding(std::vector<std::string> paths,
                                      std::string separator,
                                      std::string_view encoding);

// Creates a source that reads multiple local files as one logical byte stream
// and annotates chunks with caller-provided display names.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_source_names(std::vector<std::string> paths,
                                          std::vector<std::string> source_names,
                                          std::string separator);

// Creates a source that reads multiple local files as one logical UTF-8 byte
// stream, transcoding supported text encodings and annotating source names.
sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_source_names_encoding(
    std::vector<std::string> paths, std::vector<std::string> source_names,
    std::string separator, std::string_view encoding);

} // namespace sanitize

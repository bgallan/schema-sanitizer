// Declares the incremental CSV record scanner used by streaming frontends.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string_view>
#include <vector>

#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/row_scanner.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/chunk_source.hh"

namespace sanitize::internal {

class CsvRecordSpanScanner;

inline constexpr int64_t kDefaultCsvChunkBytes = 1024LL * 1024LL;
inline constexpr std::size_t kMaxCsvRecordBytes =
    static_cast<std::size_t>(256) * 1024u * 1024u;
inline constexpr std::size_t kMaxCsvRecordSegments = 65'536;

class CsvStreamingScanner {
public:
  CsvStreamingScanner(ChunkSourcePtr source, int64_t chunk_bytes,
                      std::size_t max_record_bytes = kMaxCsvRecordBytes,
                      std::size_t max_record_segments = kMaxCsvRecordSegments,
                      void *pool_handle = nullptr, char escape_char = '\0');

  sanitize::Status Reset();
  sanitize::Result<TextSlice> next_record(BumpArena *arena);
  [[nodiscard]] bool done() const noexcept;

private:
  struct Segment {
    std::shared_ptr<const void> owner;
    std::string_view view;
  };

  /// Releases retained record fragments and trims excessive vector capacity.
  void clear_segments() noexcept;
  /// Ensures that the current chunk has unread bytes unless input is exhausted.
  sanitize::Status ensure_chunk();
  /// Fetches the next bounded chunk and records its end-of-input offset.
  sanitize::Status refill();
  /// Consumes an LF deferred from a CRLF pair split across chunks.
  void consume_pending_lf() noexcept;
  /// Advances to the first byte of the next record, reporting clean end of
  /// input.
  sanitize::Result<bool> prepare_record_start();

  friend class CsvRecordSpanScanner;

  PoolResource segment_resource_;
  std::pmr::vector<Segment> segments_;
  ChunkSourcePtr source_;
  int64_t chunk_bytes_ = kDefaultCsvChunkBytes;
  Chunk chunk_;
  bool have_chunk_ = false;
  std::size_t pos_ = 0;
  bool eof_ = false;
  bool pending_consume_lf_ = false;
  bool prefer_vector_scan_ = false;
  std::size_t eof_offset_ = 0;
  std::size_t max_record_bytes_ = kMaxCsvRecordBytes;
  std::size_t max_record_segments_ = kMaxCsvRecordSegments;
  char escape_char_ = '\0';
};

sanitize::Result<TextSlice> scan_csv_record_span(CsvStreamingScanner &scanner,
                                                 BumpArena *arena);

} // namespace sanitize::internal

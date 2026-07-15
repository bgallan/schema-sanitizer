// Declares the incremental CSV record scanner used by streaming frontends.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "internal/memory/arena.hh"
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
  CsvStreamingScanner(ChunkSourcePtr source, int64_t chunk_bytes);

  sanitize::Status Reset();
  sanitize::Result<TextSlice> next_record(BumpArena *arena);
  [[nodiscard]] bool done() const noexcept;

private:
  struct Segment {
    std::shared_ptr<const void> owner;
    std::string_view view;
  };

  void clear_segments() noexcept;
  sanitize::Status ensure_chunk();
  sanitize::Status refill();
  void consume_pending_lf() noexcept;
  sanitize::Result<bool> prepare_record_start();

  friend class CsvRecordSpanScanner;

  std::vector<Segment> segments_;
  ChunkSourcePtr source_;
  int64_t chunk_bytes_ = kDefaultCsvChunkBytes;
  Chunk chunk_;
  bool have_chunk_ = false;
  std::size_t pos_ = 0;
  bool eof_ = false;
  bool pending_consume_lf_ = false;
  std::size_t eof_offset_ = 0;
};

sanitize::Result<TextSlice> scan_csv_record_span(CsvStreamingScanner &scanner,
                                                 BumpArena *arena);

} // namespace sanitize::internal

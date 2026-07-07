// Declares the incremental CSV record scanner used by streaming frontends.
//
// Reads incrementally from a ChunkSource and yields complete CSV records.
#pragma once

#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "internal/memory/arena.hh"
#include "internal/parsing/row_scanner.hh"
#include "sanitize/ingest/chunk_source.hh"

namespace sanitize::internal {

class CsvRecordSpanScanner;

inline constexpr int64_t kDefaultCsvChunkBytes = 1024LL * 1024LL;
inline constexpr std::size_t kMaxCsvRecordBytes =
    static_cast<std::size_t>(256) * 1024u * 1024u;

// Streaming CSV record scanner.
//
// Reads from a ChunkSource incrementally and yields complete RFC-4180-ish
// records.
class CsvStreamingScanner {
public:
  // Creates a CsvStreamingScanner.
  CsvStreamingScanner(ChunkSourcePtr src, int64_t chunk_bytes);

  // Rewinds the scanner and its chunk source.
  sanitize::Status Reset();
  // Returns the next record.
  sanitize::Result<TextSlice> next_record(BumpArena *arena);
  // Returns whether input processing is complete.
  [[nodiscard]] bool done() const noexcept;

private:
  struct Seg {
    std::shared_ptr<const void> owner;
    std::string_view view;
  };

  std::vector<Seg> segs_;

  // Ensures a current chunk is available or EOF has been reached.
  sanitize::Status ensure_chunk();
  // Refills the input buffer.
  sanitize::Status refill();
  // Consumes a pending LF after a CR split across chunks.
  void consume_pending_lf() noexcept;
  // Advances to the start of the next record or reports EOF.
  sanitize::Result<bool> prepare_record_start();

  friend class CsvRecordSpanScanner;

  ChunkSourcePtr src_;
  int64_t chunk_bytes_ = kDefaultCsvChunkBytes;

  Chunk chunk_;
  bool have_chunk_ = false;
  std::size_t pos_ = 0; // scan position within chunk_.data
  bool eof_ = false;
  bool pending_consume_lf_ = false;
  std::size_t eof_offset_ = 0;
};

// Scans one CSV record span from the scanner's current input position.
sanitize::Result<TextSlice> scan_csv_record_span(CsvStreamingScanner &scanner,
                                                 BumpArena *arena);

} // namespace sanitize::internal

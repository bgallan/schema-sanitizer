// Declares incremental JSON value scanning over chunk sources.
//
// Supports a single JSON value, an array of values, or whitespace-delimited
// value streams.

#pragma once

#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>

#include "internal/memory/arena.hh"
#include "internal/parsing/row_scanner.hh"
#include "sanitize/ingest/chunk_source.hh"

namespace sanitize::internal {

class JsonValueSpanScanner;

class JsonStreamingScanner {
public:
  // Creates a JsonStreamingScanner.
  JsonStreamingScanner(ChunkSourcePtr src, int64_t chunk_bytes,
                       bool require_top_level_array = false);

  // Rewinds the scanner and its chunk source.
  sanitize::Status Reset();
  // Returns the next value.
  sanitize::Result<TextSlice> next_value(BumpArena *arena);
  // Returns whether input processing is complete.
  [[nodiscard]] bool done() const noexcept;

private:
  enum class State : uint8_t { kInit = 0, kArray = 1, kStream = 2, kDone = 3 };

  // Returns the best known absolute end-of-input offset.
  [[nodiscard]] std::size_t eof_offset() const noexcept;
  // Ensures a current chunk is available or EOF has been reached.
  sanitize::Status ensure_chunk();
  // Refills the input buffer.
  sanitize::Status refill();
  // Skips JSON whitespace and reports whether a value byte remains.
  sanitize::Result<bool> skip_ws();
  // Skips whitespace before a value and updates EOF state.
  sanitize::Result<bool> skip_ws_before_value();
  // Initializes scanner mode after seeing the first non-whitespace byte.
  sanitize::Status enter_initial_mode();
  // Returns the next whitespace-delimited stream value.
  sanitize::Result<TextSlice> next_stream_value(BumpArena *arena);
  // Returns the next input byte without consuming it.
  [[nodiscard]] char peek() const noexcept;
  // Consumes the next input byte.
  void consume() noexcept;
  // Scans one complete JSON value from the current position.
  sanitize::Result<TextSlice> scan_value(BumpArena *arena);
  // Returns the next array value.
  sanitize::Result<TextSlice> next_array_value(BumpArena *arena);
  // Consumes and validates the tail after a top-level array closes.
  sanitize::Status finish_array();

  // Allows the chunk-crossing value scanner to operate on private stream state.
  friend class JsonValueSpanScanner;

  ChunkSourcePtr src_;
  int64_t chunk_bytes_ = int64_t{1} << 20;

  Chunk chunk_;
  bool have_chunk_ = false;
  std::size_t pos_ = 0;
  bool eof_ = false;
  std::size_t eof_offset_ = 0;
  std::size_t last_end_offset_ = 0;

  State state_ = State::kInit;
  bool require_top_level_array_ = false;
};

} // namespace sanitize::internal

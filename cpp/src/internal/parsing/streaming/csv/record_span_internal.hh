// Declares record-level CSV scanning across chunk boundaries.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#pragma once

#include <cstddef>
#include <memory>
#include <string_view>

#include "internal/parsing/streaming/csv/scanner.hh"

namespace sanitize::internal {

class CsvRecordSpanScanner {
public:
  CsvRecordSpanScanner(CsvStreamingScanner &scanner, BumpArena *arena);
  sanitize::Result<TextSlice> scan();

private:
  /// Retains the current chunk fragment while enforcing record and segment
  /// limits.
  sanitize::Status push_segment(std::size_t end_pos);
  /// Saves the current fragment and refills the scanner for a continued record.
  sanitize::Status span_to_next_chunk();
  /// Removes a trailing carriage return from a segmented record.
  void trim_trailing_cr();
  /// Copies retained record fragments into one arena-owned text slice.
  sanitize::Result<TextSlice> materialize_segments();
  /// Advances past an LF or CRLF boundary, including a split CRLF pair.
  void consume_newline(char current);
  /// Completes a newline-terminated record as a view or arena-owned slice.
  sanitize::Result<TextSlice> finish_newline_record(char current);
  /// Completes the final record or reports an unterminated quoted field.
  sanitize::Result<TextSlice> finish_eof_record();
  /// Refills at a chunk boundary or completes the record when input ends.
  sanitize::Status handle_chunk_end(TextSlice *out, bool *finished);
  /// Updates quoted-field state, including escaped and chunk-split quotes.
  sanitize::Status handle_quote();

  CsvStreamingScanner &scanner_;
  BumpArena *arena_ = nullptr;
  std::size_t record_start_pos_ = 0;
  std::size_t record_start_abs_ = 0;
  std::size_t opening_quote_abs_ = std::string_view::npos;
  std::shared_ptr<const void> record_owner_;
  std::shared_ptr<const std::string> record_source_file_owner_;
  std::string_view record_source_file_;
  std::size_t record_source_index_ = 0;
  bool record_has_source_index_ = false;
  bool in_quotes_ = false;
  bool escape_pending_ = false;
  bool multi_ = false;
  std::size_t segment_start_pos_ = 0;
  std::shared_ptr<const void> segment_owner_;
  std::size_t total_bytes_ = 0;
};

} // namespace sanitize::internal

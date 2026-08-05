// Declares record-level CSV scanning across chunk boundaries.

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
  sanitize::Status push_segment(std::size_t end_pos);
  sanitize::Status span_to_next_chunk();
  void trim_trailing_cr();
  sanitize::Result<TextSlice> materialize_segments();
  void consume_newline(char current);
  sanitize::Result<TextSlice> finish_newline_record(char current);
  sanitize::Result<TextSlice> finish_eof_record();
  sanitize::Status handle_chunk_end(TextSlice *out, bool *finished);
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

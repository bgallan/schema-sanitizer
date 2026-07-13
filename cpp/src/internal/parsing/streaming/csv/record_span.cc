// Implements record-level CSV quote and newline scanning.

#include "internal/parsing/streaming/csv/record_span_internal.hh"

namespace sanitize::internal {

CsvRecordSpanScanner::CsvRecordSpanScanner(CsvStreamingScanner &scanner,
                                           BumpArena *arena)
    : scanner_(scanner), arena_(arena), record_start_pos_(scanner.pos_),
      record_start_abs_(scanner.chunk_.base_offset + scanner.pos_),
      record_owner_(scanner.chunk_.owner),
      record_source_file_owner_(scanner.chunk_.source_name_owner),
      record_source_file_(scanner.chunk_.source_name),
      record_source_index_(scanner.chunk_.source_index),
      record_has_source_index_(scanner.chunk_.has_source_index),
      segment_start_pos_(scanner.pos_), segment_owner_(scanner.chunk_.owner) {
  scanner_.segments_.clear();
}

sanitize::Result<TextSlice> CsvRecordSpanScanner::scan() {
  for (;;) {
    TextSlice eof_record;
    bool finished = false;
    SAN_RETURN_NOT_OK(handle_chunk_end(&eof_record, &finished));
    if (finished) {
      return eof_record;
    }

    const char current = scanner_.chunk_.data[scanner_.pos_];
    if (current == '"') {
      SAN_RETURN_NOT_OK(handle_quote());
      continue;
    }
    if (!in_quotes_ && (current == '\n' || current == '\r')) {
      return finish_newline_record(current);
    }
    ++scanner_.pos_;
  }
}

void CsvRecordSpanScanner::consume_newline(char current) {
  if (current == '\r') {
    ++scanner_.pos_;
    if (scanner_.pos_ < scanner_.chunk_.data.size() &&
        scanner_.chunk_.data[scanner_.pos_] == '\n') {
      ++scanner_.pos_;
    } else if (scanner_.pos_ >= scanner_.chunk_.data.size() && !scanner_.eof_) {
      scanner_.pending_consume_lf_ = true;
    }
    return;
  }
  ++scanner_.pos_;
}

sanitize::Result<TextSlice>
CsvRecordSpanScanner::finish_newline_record(char current) {
  const std::size_t end_pos = scanner_.pos_;
  if (!multi_) {
    std::string_view record = scanner_.chunk_.data.substr(
        record_start_pos_, end_pos - record_start_pos_);
    if (!record.empty() && record.back() == '\r') {
      record.remove_suffix(1);
    }
    consume_newline(current);
    return make_text_slice(record, record_start_abs_, record_owner_,
                           record_source_file_owner_, record_source_file_,
                           record_source_index_, record_has_source_index_);
  }

  SAN_RETURN_NOT_OK(push_segment(end_pos));
  trim_trailing_cr();
  SAN_ASSIGN_OR_RAISE(TextSlice out, materialize_segments());
  consume_newline(current);
  return out;
}

sanitize::Result<TextSlice> CsvRecordSpanScanner::finish_eof_record() {
  if (!multi_) {
    std::string_view record = scanner_.chunk_.data.substr(record_start_pos_);
    if (!record.empty() && record.back() == '\r') {
      record.remove_suffix(1);
    }
    scanner_.pos_ = scanner_.chunk_.data.size();
    return make_text_slice(record, record_start_abs_, record_owner_,
                           record_source_file_owner_, record_source_file_,
                           record_source_index_, record_has_source_index_);
  }

  SAN_RETURN_NOT_OK(push_segment(scanner_.chunk_.data.size()));
  trim_trailing_cr();
  SAN_ASSIGN_OR_RAISE(TextSlice out, materialize_segments());
  scanner_.pos_ = scanner_.chunk_.data.size();
  return out;
}

sanitize::Status CsvRecordSpanScanner::handle_chunk_end(TextSlice *out,
                                                        bool *finished) {
  *finished = false;
  if (scanner_.pos_ < scanner_.chunk_.data.size()) {
    return sanitize::Status::OK();
  }
  if (scanner_.eof_) {
    SAN_ASSIGN_OR_RAISE(*out, finish_eof_record());
    *finished = true;
    return sanitize::Status::OK();
  }
  return span_to_next_chunk();
}

sanitize::Status CsvRecordSpanScanner::handle_quote() {
  if (!in_quotes_) {
    in_quotes_ = true;
    ++scanner_.pos_;
    return sanitize::Status::OK();
  }
  if (scanner_.pos_ + 1 < scanner_.chunk_.data.size() &&
      scanner_.chunk_.data[scanner_.pos_ + 1] == '"') {
    scanner_.pos_ += 2;
    return sanitize::Status::OK();
  }
  if (scanner_.pos_ + 1 >= scanner_.chunk_.data.size() && !scanner_.eof_) {
    ++scanner_.pos_;
    SAN_RETURN_NOT_OK(push_segment(scanner_.pos_));
    multi_ = true;
    in_quotes_ = false;
    SAN_RETURN_NOT_OK(scanner_.refill());
    segment_start_pos_ = 0;
    segment_owner_ = scanner_.chunk_.owner;
    if (!scanner_.chunk_.data.empty() && scanner_.chunk_.data[0] == '"') {
      in_quotes_ = true;
      scanner_.pos_ = 1;
    }
    return sanitize::Status::OK();
  }
  in_quotes_ = false;
  ++scanner_.pos_;
  return sanitize::Status::OK();
}

sanitize::Result<TextSlice> scan_csv_record_span(CsvStreamingScanner &scanner,
                                                 BumpArena *arena) {
  CsvRecordSpanScanner record_scanner(scanner, arena);
  return record_scanner.scan();
}

} // namespace sanitize::internal

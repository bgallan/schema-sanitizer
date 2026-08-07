// Implements record-level CSV quote and newline scanning.

#include "internal/parsing/streaming/csv/record_span_internal.hh"

#include <algorithm>
#include <cstring>
#include <string_view>

namespace sanitize::internal {

namespace {

constexpr std::size_t kCsvVectorScanMinimum = 1024;
constexpr std::size_t kCsvDenseQuoteGap = 16;
constexpr unsigned kCsvDenseQuoteRun = 4;

[[nodiscard]] std::size_t find_byte(std::string_view input, std::size_t begin,
                                    char needle) noexcept {
  if (begin >= input.size()) {
    return std::string_view::npos;
  }
  const auto *found = static_cast<const char *>(
      std::memchr(input.data() + begin, static_cast<unsigned char>(needle),
                  input.size() - begin));
  return found ? static_cast<std::size_t>(found - input.data())
               : std::string_view::npos;
}

[[nodiscard]] std::size_t find_line_break(std::string_view input,
                                          std::size_t begin) noexcept {
  const auto lf = find_byte(input, begin, '\n');
  const auto cr = find_byte(input, begin, '\r');
  return std::min(lf, cr);
}

} // namespace

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
  const char *line_break_data = nullptr;
  std::size_t next_line_break = std::string_view::npos;
  // Escaped quotes require state across chunk boundaries. Keep that opt-in
  // dialect on the scalar path; the strict default retains vector scanning.
  bool vector_scan =
      scanner_.escape_char_ == '\0' && scanner_.prefer_vector_scan_;
  unsigned short_quote_run = 0;
  for (;;) {
    TextSlice eof_record;
    bool finished = false;
    SAN_RETURN_NOT_OK(handle_chunk_end(&eof_record, &finished));
    if (finished) {
      return eof_record;
    }
    if (scanner_.pos_ >= scanner_.chunk_.data.size()) {
      continue;
    }

    if (!vector_scan) {
      const char *const data_ptr = scanner_.chunk_.data.data();
      while (scanner_.pos_ < scanner_.chunk_.data.size()) {
        const char current = scanner_.chunk_.data[scanner_.pos_];
        if (in_quotes_ && scanner_.escape_char_ != '\0') {
          if (escape_pending_) {
            escape_pending_ = false;
            ++scanner_.pos_;
            continue;
          }
          if (current == scanner_.escape_char_) {
            escape_pending_ = true;
            ++scanner_.pos_;
            continue;
          }
        }
        if (current == '"') {
          SAN_RETURN_NOT_OK(handle_quote());
          if (scanner_.chunk_.data.data() != data_ptr) {
            break;
          }
          continue;
        }
        if (!in_quotes_ && (current == '\n' || current == '\r')) {
          return finish_newline_record(current);
        }
        ++scanner_.pos_;
      }
      continue;
    }

    const auto data = scanner_.chunk_.data;
    if (line_break_data != data.data()) {
      line_break_data = data.data();
      next_line_break = find_line_break(data, scanner_.pos_);
    } else if (!in_quotes_ && next_line_break < scanner_.pos_) {
      next_line_break = find_line_break(data, scanner_.pos_);
    }

    const auto begin = scanner_.pos_;
    const auto special =
        in_quotes_ ? find_byte(data, begin, '"')
                   : std::min(find_byte(data, begin, '"'), next_line_break);
    if (special == std::string_view::npos) {
      scanner_.pos_ = data.size();
      continue;
    }
    scanner_.pos_ = special;
    const char current = data[special];
    if (current == '"') {
      const auto gap = special - begin;
      short_quote_run = gap <= kCsvDenseQuoteGap ? short_quote_run + 1U : 0U;
      SAN_RETURN_NOT_OK(handle_quote());
      if (short_quote_run >= kCsvDenseQuoteRun) {
        vector_scan = false;
      }
      continue;
    }
    return finish_newline_record(current);
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
    if (record.size() > scanner_.max_record_bytes_) {
      return sanitize::Status::OutOfMemory(
          "CSV raw record size exceeds effective operation limit: ",
          record.size(), " > ", scanner_.max_record_bytes_);
    }
    scanner_.prefer_vector_scan_ = record.size() >= kCsvVectorScanMinimum;
    consume_newline(current);
    return make_text_slice(record, record_start_abs_, record_owner_,
                           record_source_file_owner_, record_source_file_,
                           record_source_index_, record_has_source_index_);
  }

  SAN_RETURN_NOT_OK(push_segment(end_pos));
  trim_trailing_cr();
  SAN_ASSIGN_OR_RAISE(TextSlice out, materialize_segments());
  scanner_.prefer_vector_scan_ = out.view.size() >= kCsvVectorScanMinimum;
  consume_newline(current);
  return out;
}

sanitize::Result<TextSlice> CsvRecordSpanScanner::finish_eof_record() {
  if (in_quotes_) {
    return sanitize::Status::Invalid(
        "CSV parse error at byte ",
        opening_quote_abs_ == std::string_view::npos ? record_start_abs_
                                                     : opening_quote_abs_,
        ": unterminated quoted field at end of file");
  }
  if (!multi_) {
    std::string_view record = scanner_.chunk_.data.substr(record_start_pos_);
    if (!record.empty() && record.back() == '\r') {
      record.remove_suffix(1);
    }
    if (record.size() > scanner_.max_record_bytes_) {
      return sanitize::Status::OutOfMemory(
          "CSV raw record size exceeds effective operation limit: ",
          record.size(), " > ", scanner_.max_record_bytes_);
    }
    scanner_.prefer_vector_scan_ = record.size() >= kCsvVectorScanMinimum;
    scanner_.pos_ = scanner_.chunk_.data.size();
    return make_text_slice(record, record_start_abs_, record_owner_,
                           record_source_file_owner_, record_source_file_,
                           record_source_index_, record_has_source_index_);
  }

  SAN_RETURN_NOT_OK(push_segment(scanner_.chunk_.data.size()));
  trim_trailing_cr();
  SAN_ASSIGN_OR_RAISE(TextSlice out, materialize_segments());
  scanner_.prefer_vector_scan_ = out.view.size() >= kCsvVectorScanMinimum;
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
    opening_quote_abs_ = scanner_.chunk_.base_offset + scanner_.pos_;
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
    } else {
      opening_quote_abs_ = std::string_view::npos;
    }
    return sanitize::Status::OK();
  }
  in_quotes_ = false;
  opening_quote_abs_ = std::string_view::npos;
  ++scanner_.pos_;
  return sanitize::Status::OK();
}

sanitize::Result<TextSlice> scan_csv_record_span(CsvStreamingScanner &scanner,
                                                 BumpArena *arena) {
  CsvRecordSpanScanner record_scanner(scanner, arena);
  return record_scanner.scan();
}

} // namespace sanitize::internal

// Implements CSV record scanning for spans that cross chunk boundaries.
//
// The main CSV streaming scanner owns source state; this helper owns the
// record-level quote tracking and buffered materialization path.

#include "internal/parsing/streaming/csv_streaming_scanner.hh"

#include <cstring>
#include <memory>
#include <string_view>

namespace sanitize::internal {

class CsvRecordSpanScanner {
public:
  // Creates a scanner for one CSV record, starting at the current chunk byte.
  CsvRecordSpanScanner(CsvStreamingScanner &scanner, BumpArena *arena)
      : scanner_(scanner), arena_(arena), record_start_pos_(scanner.pos_),
        record_start_abs_(scanner.chunk_.base_offset + scanner.pos_),
        record_owner_(scanner.chunk_.owner),
        record_source_file_owner_(scanner.chunk_.source_name_owner),
        record_source_file_(scanner.chunk_.source_name),
        record_source_index_(scanner.chunk_.source_index),
        record_has_source_index_(scanner.chunk_.has_source_index),
        seg_start_pos_(scanner.pos_), seg_owner_(scanner.chunk_.owner) {
    scanner_.segs_.clear();
  }

  // Scans and returns one complete CSV record.
  sanitize::Result<TextSlice> scan() {
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

private:
  // Buffers the current chunk segment.
  sanitize::Status push_segment(std::size_t end_pos) {
    if (end_pos <= seg_start_pos_) {
      return sanitize::Status::OK();
    }
    std::string_view part =
        scanner_.chunk_.data.substr(seg_start_pos_, end_pos - seg_start_pos_);
    if (part.size() > kMaxCsvRecordBytes ||
        total_bytes_ > kMaxCsvRecordBytes - part.size()) {
      return sanitize::Status::Invalid("CSV record exceeds max buffered size");
    }
    total_bytes_ += part.size();
    scanner_.segs_.push_back(CsvStreamingScanner::Seg{
        .owner = seg_owner_,
        .view = part,
    });
    return sanitize::Status::OK();
  }

  // Buffers the current chunk tail and refills the scanner.
  sanitize::Status span_to_next_chunk() {
    SAN_RETURN_NOT_OK(push_segment(scanner_.chunk_.data.size()));
    multi_ = true;
    SAN_RETURN_NOT_OK(scanner_.refill());
    seg_start_pos_ = 0;
    seg_owner_ = scanner_.chunk_.owner;
    return sanitize::Status::OK();
  }

  // Removes a trailing carriage return from buffered segment data.
  void trim_trailing_cr() {
    if (!scanner_.segs_.empty() && !scanner_.segs_.back().view.empty() &&
        scanner_.segs_.back().view.back() == '\r') {
      auto view = scanner_.segs_.back().view;
      view.remove_suffix(1);
      total_bytes_ -= 1;
      scanner_.segs_.back().view = view;
    }
  }

  // Copies buffered segments into arena-owned memory.
  sanitize::Result<TextSlice> materialize_segments() {
    char *dst = static_cast<char *>(arena_->alloc(total_bytes_, alignof(char)));
    if (!dst && total_bytes_) {
      return sanitize::Status::Invalid("CSV scanner: arena alloc failed");
    }
    std::size_t written = 0;
    for (const auto &segment : scanner_.segs_) {
      std::memcpy(dst + written, segment.view.data(), segment.view.size());
      written += segment.view.size();
    }
    return make_text_slice(std::string_view(dst, total_bytes_),
                           record_start_abs_, {}, record_source_file_owner_,
                           record_source_file_, record_source_index_,
                           record_has_source_index_);
  }

  // Consumes a record-ending newline sequence.
  void consume_newline(char current) {
    if (current == '\r') {
      ++scanner_.pos_;
      if (scanner_.pos_ < scanner_.chunk_.data.size() &&
          scanner_.chunk_.data[scanner_.pos_] == '\n') {
        ++scanner_.pos_;
      } else if (scanner_.pos_ >= scanner_.chunk_.data.size() &&
                 !scanner_.eof_) {
        scanner_.pending_consume_lf_ = true;
      }
      return;
    }
    ++scanner_.pos_;
  }

  // Finishes a record ended by newline in the current chunk.
  sanitize::Result<TextSlice> finish_newline_record(char current) {
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

  // Finishes a record that ends at EOF.
  sanitize::Result<TextSlice> finish_eof_record() {
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

  // Handles chunk exhaustion during record scanning.
  sanitize::Status handle_chunk_end(TextSlice *out, bool *finished) {
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

  // Handles quotes, including escaped quotes across chunk boundaries.
  sanitize::Status handle_quote() {
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
      seg_start_pos_ = 0;
      seg_owner_ = scanner_.chunk_.owner;
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

  CsvStreamingScanner &scanner_;
  BumpArena *arena_ = nullptr;
  std::size_t record_start_pos_ = 0;
  std::size_t record_start_abs_ = 0;
  std::shared_ptr<const void> record_owner_;
  std::shared_ptr<const std::string> record_source_file_owner_;
  std::string_view record_source_file_;
  std::size_t record_source_index_ = 0;
  bool record_has_source_index_ = false;
  bool in_quotes_ = false;
  bool multi_ = false;
  std::size_t seg_start_pos_ = 0;
  std::shared_ptr<const void> seg_owner_;
  std::size_t total_bytes_ = 0;
};

sanitize::Result<TextSlice> scan_csv_record_span(CsvStreamingScanner &scanner,
                                                 BumpArena *arena) {
  CsvRecordSpanScanner record_scanner(scanner, arena);
  return record_scanner.scan();
}

} // namespace sanitize::internal

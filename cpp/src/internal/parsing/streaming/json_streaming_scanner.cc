// Scans chunked JSON input into complete value spans.

#include "internal/parsing/streaming/json_streaming_scanner.hh"

#include "internal/parsing/json_ondemand.hh"

#include <string_view>
#include <utility>

namespace sanitize::internal {

namespace {

// Returns whether a byte is JSON whitespace.
bool json_stream_is_ws(unsigned char c) {
  return c == ' ' || c == '\n' || c == '\r' || c == '\t';
}

} // namespace

JsonStreamingScanner::JsonStreamingScanner(ChunkSourcePtr src,
                                           int64_t chunk_bytes,
                                           bool require_top_level_array)
    : src_(std::move(src)),
      chunk_bytes_(chunk_bytes > 0 ? chunk_bytes : (int64_t{1} << 20)),
      require_top_level_array_(require_top_level_array) {}

sanitize::Status JsonStreamingScanner::Reset() {
  eof_ = false;
  have_chunk_ = false;
  pos_ = 0;
  eof_offset_ = 0;
  last_end_offset_ = 0;
  chunk_ = Chunk{};
  state_ = State::kInit;
  if (!src_) {
    return sanitize::Status::Invalid("JSON scanner: source is null");
  }
  return src_->Reset();
}

sanitize::Result<TextSlice> JsonStreamingScanner::next_value(BumpArena *arena) {
  if (!arena) {
    return sanitize::Status::Invalid("JSON scanner: arena is null");
  }

  for (;;) {
    if (state_ == State::kDone) {
      return make_text_slice(std::string_view{}, eof_offset());
    }

    SAN_ASSIGN_OR_RAISE(bool has, skip_ws_before_value());
    if (!has) {
      return make_text_slice(std::string_view{}, eof_offset());
    }

    SAN_RETURN_NOT_OK(enter_initial_mode());

    if (state_ == State::kStream) {
      return next_stream_value(arena);
    }

    SAN_ASSIGN_OR_RAISE(TextSlice value, next_array_value(arena));
    if (value.view.empty() && state_ != State::kDone) {
      continue;
    }
    return value;
  }
}

bool JsonStreamingScanner::done() const noexcept {
  return state_ == State::kDone;
}

std::size_t JsonStreamingScanner::eof_offset() const noexcept {
  return eof_ ? eof_offset_ : last_end_offset_;
}

sanitize::Status JsonStreamingScanner::ensure_chunk() {
  if (have_chunk_ && pos_ < chunk_.data.size()) {
    return sanitize::Status::OK();
  }
  if (have_chunk_ && pos_ >= chunk_.data.size() && eof_) {
    return sanitize::Status::OK();
  }
  return refill();
}

sanitize::Status JsonStreamingScanner::refill() {
  if (!src_) {
    return sanitize::Status::Invalid("JSON scanner: source is null");
  }
  SAN_ASSIGN_OR_RAISE(Chunk c, src_->NextChunk(chunk_bytes_));
  have_chunk_ = true;
  chunk_ = std::move(c);
  pos_ = 0;
  if (chunk_.data.empty()) {
    eof_ = true;
    eof_offset_ = chunk_.base_offset;
    last_end_offset_ = eof_offset_;
  } else {
    last_end_offset_ = chunk_.base_offset + chunk_.data.size();
  }
  return sanitize::Status::OK();
}

sanitize::Result<bool> JsonStreamingScanner::skip_ws() {
  for (;;) {
    SAN_RETURN_NOT_OK(ensure_chunk());
    while (pos_ < chunk_.data.size() &&
           json_stream_is_ws(static_cast<unsigned char>(chunk_.data[pos_]))) {
      ++pos_;
    }
    if (pos_ < chunk_.data.size()) {
      return true;
    }
    if (eof_) {
      return false;
    }
    SAN_RETURN_NOT_OK(refill());
  }
}

sanitize::Result<bool> JsonStreamingScanner::skip_ws_before_value() {
  SAN_ASSIGN_OR_RAISE(bool has, skip_ws());
  if (has) {
    return true;
  }
  if (state_ == State::kArray) {
    return sanitize::Status::Invalid("JSON parse error: unterminated array");
  }
  state_ = State::kDone;
  return false;
}

sanitize::Status JsonStreamingScanner::enter_initial_mode() {
  if (state_ != State::kInit) {
    return sanitize::Status::OK();
  }
  if (peek() == '[') {
    consume();
    state_ = State::kArray;
    return sanitize::Status::OK();
  }
  if (require_top_level_array_) {
    return sanitize::Status::Invalid("json_array input must be a JSON array");
  }
  state_ = State::kStream;
  return sanitize::Status::OK();
}

sanitize::Result<TextSlice>
JsonStreamingScanner::next_stream_value(BumpArena *arena) {
  SAN_ASSIGN_OR_RAISE(TextSlice value, scan_value(arena));
  if (value.view.empty() && eof_) {
    state_ = State::kDone;
  }
  return value;
}

char JsonStreamingScanner::peek() const noexcept {
  if (!have_chunk_ || pos_ >= chunk_.data.size()) {
    return '\0';
  }
  return chunk_.data[pos_];
}

void JsonStreamingScanner::consume() noexcept {
  if (have_chunk_ && pos_ < chunk_.data.size()) {
    ++pos_;
  }
}

sanitize::Result<TextSlice> JsonStreamingScanner::scan_value(BumpArena *arena) {
  SAN_RETURN_NOT_OK(ensure_chunk());
  if (chunk_.data.empty()) {
    return make_text_slice(std::string_view{}, eof_offset());
  }

  // Fast path: value fully contained in this chunk.
  {
    const std::string_view s = chunk_.data;
    const std::size_t start = pos_;
    auto r = json_skip_value(s, start, chunk_.base_offset);
    if (r.ok() && *r <= s.size()) {
      const std::size_t end = *r;
      pos_ = end;
      return make_text_slice(s.substr(start, end - start),
                             chunk_.base_offset + start, chunk_.owner,
                             chunk_.source_name_owner, chunk_.source_name,
                             chunk_.source_index, chunk_.has_source_index);
    }
  }

  return scan_json_value_span(*this, arena);
}

sanitize::Result<TextSlice>
JsonStreamingScanner::next_array_value(BumpArena *arena) {
  // Caller already performed skip_ws() for state init; do it again here for
  // element scanning.
  SAN_ASSIGN_OR_RAISE(bool has, skip_ws());
  if (!has) {
    return sanitize::Status::Invalid("JSON parse error: unterminated array");
  }

  if (peek() == ']') {
    const std::size_t end_offset = chunk_.base_offset + pos_;
    consume();
    SAN_RETURN_NOT_OK(finish_array());
    return make_text_slice(std::string_view{}, end_offset);
  }

  SAN_ASSIGN_OR_RAISE(TextSlice v, scan_value(arena));

  // Separator / end.
  SAN_ASSIGN_OR_RAISE(bool has2, skip_ws());
  if (!has2) {
    return sanitize::Status::Invalid("JSON parse error: unterminated array");
  }

  const char sep = peek();
  if (sep == ',') {
    consume();
    return v;
  }
  if (sep == ']') {
    consume();
    SAN_RETURN_NOT_OK(finish_array());
    return v;
  }
  return sanitize::Status::Invalid(
      "JSON parse error: expected ',' or ']' in array");
}

sanitize::Status JsonStreamingScanner::finish_array() {
  SAN_ASSIGN_OR_RAISE(bool has_trailing, skip_ws());
  if (has_trailing) {
    return sanitize::Status::Invalid(
        "JSON parse error: trailing characters after top-level array");
  }
  state_ = State::kDone;
  return sanitize::Status::OK();
}

} // namespace sanitize::internal

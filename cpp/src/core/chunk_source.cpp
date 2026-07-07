// Provides file, memory, and full-view byte chunk sources for ingestion.

#include "sanitize/ingest/chunk_source.hh"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "sanitize/core/status.hh"

namespace sanitize {

namespace {

// Reads an entire file into a std::string for full-view ingestion.
static sanitize::Result<std::string>
read_file_bytes_std(const std::string &path);

enum class TextEncoding : uint8_t {
  kUtf8 = 0,
  kLatin1,
  kUtf16,
  kUtf16LE,
  kUtf16BE,
  kUnsupported,
};

std::string normalize_encoding(std::string_view encoding) {
  std::string out;
  out.reserve(encoding.size());
  for (const unsigned char ch : encoding) {
    if (ch == '_' || ch == ' ') {
      out.push_back('-');
    } else {
      out.push_back(static_cast<char>(std::tolower(ch)));
    }
  }
  return out;
}

TextEncoding parse_text_encoding(std::string_view encoding) {
  const std::string enc = normalize_encoding(encoding);
  if (enc.empty() || enc == "utf-8" || enc == "utf8" || enc == "utf-8-sig") {
    return TextEncoding::kUtf8;
  }
  if (enc == "latin-1" || enc == "latin1" || enc == "iso-8859-1" ||
      enc == "iso8859-1" || enc == "iso88591" || enc == "cp819") {
    return TextEncoding::kLatin1;
  }
  if (enc == "utf-16" || enc == "utf16") {
    return TextEncoding::kUtf16;
  }
  if (enc == "utf-16-le" || enc == "utf16-le" || enc == "utf-16le" ||
      enc == "utf16le") {
    return TextEncoding::kUtf16LE;
  }
  if (enc == "utf-16-be" || enc == "utf16-be" || enc == "utf-16be" ||
      enc == "utf16be") {
    return TextEncoding::kUtf16BE;
  }
  return TextEncoding::kUnsupported;
}

std::string_view encoding_name(TextEncoding encoding) {
  switch (encoding) {
  case TextEncoding::kLatin1:
    return "iso8859-1";
  case TextEncoding::kUtf16:
    return "utf-16";
  case TextEncoding::kUtf16LE:
    return "utf-16-le";
  case TextEncoding::kUtf16BE:
    return "utf-16-be";
  case TextEncoding::kUtf8:
  case TextEncoding::kUnsupported:
    break;
  }
  return "utf-8";
}

void append_utf8_codepoint(uint32_t cp, std::string *out) {
  if (cp <= 0x7f) {
    out->push_back(static_cast<char>(cp));
  } else if (cp <= 0x7ff) {
    out->push_back(static_cast<char>(0xc0 | (cp >> 6)));
    out->push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  } else if (cp <= 0xffff) {
    out->push_back(static_cast<char>(0xe0 | (cp >> 12)));
    out->push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  } else {
    out->push_back(static_cast<char>(0xf0 | (cp >> 18)));
    out->push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  }
}

bool is_high_surrogate(uint16_t value) {
  return value >= 0xd800 && value <= 0xdbff;
}

bool is_low_surrogate(uint16_t value) {
  return value >= 0xdc00 && value <= 0xdfff;
}

class OwnedChunkSource final : public ChunkSource {
public:
  // Creates an owned in-memory chunk source.
  explicit OwnedChunkSource(std::string bytes)
      : data_(std::make_shared<std::string>(std::move(bytes))) {}

  // Rewinds the source to the first byte.
  sanitize::Status Reset() override {
    pos_ = 0;
    return sanitize::Status::OK();
  }

  // Returns the next chunk.
  sanitize::Result<Chunk> NextChunk(int64_t max_bytes) override {
    if (max_bytes <= 0) {
      return sanitize::Status::Invalid("NextChunk: max_bytes must be > 0");
    }

    const std::string_view s(*data_);
    if (pos_ >= s.size()) {
      return Chunk{.owner = data_,
                   .data = std::string_view{},
                   .base_offset = pos_,
                   .source_name_owner = {},
                   .source_name = {},
                   .source_index = 0,
                   .has_source_index = false};
    }

    const std::size_t n = std::min<std::size_t>(
        static_cast<std::size_t>(max_bytes), s.size() - pos_);

    Chunk c;
    c.owner = data_;
    c.data = s.substr(pos_, n);
    c.base_offset = pos_;
    pos_ += n;
    return c;
  }

  // Returns a zero-copy view of the full in-memory payload.
  sanitize::Result<Chunk> View() override {
    Chunk c;
    c.owner = data_;
    c.data = std::string_view(*data_);
    c.base_offset = 0;
    return c;
  }

private:
  std::shared_ptr<std::string> data_;
  std::size_t pos_ = 0;
};

class FileChunkSource final : public ChunkSource {
public:
  // Creates a chunk source backed by a local filesystem path.
  explicit FileChunkSource(std::string path) : path_(std::move(path)) {}

  // Closes and rewinds the file source.
  sanitize::Status Reset() override {
    in_.close();
    in_.clear();
    pos_ = 0;
    eof_ = false;
    full_view_.reset();
    return sanitize::Status::OK();
  }

  // Reads the next byte chunk from the file stream.
  sanitize::Result<Chunk> NextChunk(int64_t max_bytes) override {
    if (max_bytes <= 0) {
      return sanitize::Status::Invalid("NextChunk: max_bytes must be > 0");
    }
    if (eof_) {
      return Chunk{.owner = nullptr,
                   .data = std::string_view{},
                   .base_offset = pos_,
                   .source_name_owner = {},
                   .source_name = {},
                   .source_index = 0,
                   .has_source_index = false};
    }
    SAN_RETURN_NOT_OK(open_if_needed());

    const auto max_stream =
        static_cast<int64_t>(std::numeric_limits<std::streamsize>::max());
    const auto want =
        static_cast<std::streamsize>(std::min<int64_t>(max_bytes, max_stream));

    auto bytes = std::make_shared<std::string>();
    bytes->resize(static_cast<std::size_t>(want));
    in_.read(bytes->data(), want);
    const std::streamsize got = in_.gcount();
    if (got <= 0) {
      eof_ = true;
      return Chunk{.owner = nullptr,
                   .data = std::string_view{},
                   .base_offset = pos_,
                   .source_name_owner = {},
                   .source_name = {},
                   .source_index = 0,
                   .has_source_index = false};
    }

    bytes->resize(static_cast<std::size_t>(got));
    Chunk c;
    c.owner = bytes;
    c.data = std::string_view(*bytes);
    c.base_offset = pos_;
    pos_ += static_cast<std::size_t>(got);

    if (got < want || in_.eof()) {
      eof_ = true;
    }
    return c;
  }

  // Returns an owned view of the complete file contents.
  sanitize::Result<Chunk> View() override {
    if (!full_view_) {
      SAN_ASSIGN_OR_RAISE(auto bytes, read_file_bytes_std(path_));
      full_view_ = std::make_shared<std::string>(std::move(bytes));
    }
    Chunk c;
    c.owner = full_view_;
    c.data = std::string_view(*full_view_);
    c.base_offset = 0;
    return c;
  }

private:
  // Opens if needed.
  sanitize::Status open_if_needed() {
    if (in_.is_open())
      return sanitize::Status::OK();
    in_.open(path_, std::ios::binary);
    if (!in_.good()) {
      return sanitize::Status::Invalid("FileChunkSource: failed to open '",
                                       path_, "'");
    }
    return sanitize::Status::OK();
  }

  std::string path_;
  std::ifstream in_;
  std::size_t pos_ = 0;
  bool eof_ = false;
  std::shared_ptr<std::string> full_view_;
};

class TranscodingFileChunkSource final : public ChunkSource {
public:
  TranscodingFileChunkSource(std::string path, TextEncoding encoding)
      : path_(std::move(path)), encoding_(encoding) {}

  sanitize::Status Reset() override {
    in_.close();
    in_.clear();
    utf8_pos_ = 0;
    eof_ = false;
    bom_checked_ = false;
    utf16_little_endian_ = encoding_ != TextEncoding::kUtf16BE;
    pending_byte_.reset();
    pending_high_surrogate_.reset();
    full_view_.reset();
    return sanitize::Status::OK();
  }

  sanitize::Result<Chunk> NextChunk(int64_t max_bytes) override {
    if (max_bytes <= 0) {
      return sanitize::Status::Invalid("NextChunk: max_bytes must be > 0");
    }
    if (eof_) {
      return empty_chunk();
    }
    SAN_RETURN_NOT_OK(open_if_needed());

    const std::size_t raw_want = raw_read_size(max_bytes);
    for (;;) {
      std::string raw;
      raw.resize(raw_want);
      in_.read(raw.data(), static_cast<std::streamsize>(raw_want));
      const std::streamsize got = in_.gcount();
      if (got > 0) {
        raw.resize(static_cast<std::size_t>(got));
      } else {
        raw.clear();
      }

      const bool final = got <= 0;
      SAN_ASSIGN_OR_RAISE(auto out_bytes, transcode_chunk(raw, final));
      if (final) {
        eof_ = true;
        in_.close();
      }
      if (!out_bytes.empty()) {
        auto bytes = std::make_shared<std::string>(std::move(out_bytes));
        Chunk c;
        c.owner = bytes;
        c.data = std::string_view(*bytes);
        c.base_offset = utf8_pos_;
        utf8_pos_ += bytes->size();
        return c;
      }
      if (eof_) {
        return empty_chunk();
      }
    }
  }

  sanitize::Result<Chunk> View() override {
    if (!full_view_) {
      SAN_RETURN_NOT_OK(Reset());
      auto bytes = std::make_shared<std::string>();
      for (;;) {
        SAN_ASSIGN_OR_RAISE(auto chunk, NextChunk(1LL << 20));
        if (chunk.data.empty()) {
          break;
        }
        bytes->append(chunk.data);
      }
      full_view_ = std::move(bytes);
    }
    Chunk c;
    c.owner = full_view_;
    c.data = std::string_view(*full_view_);
    c.base_offset = 0;
    return c;
  }

private:
  Chunk empty_chunk() const {
    return Chunk{.owner = nullptr,
                 .data = std::string_view{},
                 .base_offset = utf8_pos_,
                 .source_name_owner = {},
                 .source_name = {},
                 .source_index = 0,
                 .has_source_index = false};
  }

  sanitize::Status open_if_needed() {
    if (in_.is_open()) {
      return sanitize::Status::OK();
    }
    in_.open(path_, std::ios::binary);
    if (!in_.good()) {
      return sanitize::Status::Invalid(
          "TranscodingFileChunkSource: failed to open '", path_, "'");
    }
    return sanitize::Status::OK();
  }

  std::size_t raw_read_size(int64_t max_bytes) const {
    const auto max_stream =
        static_cast<int64_t>(std::numeric_limits<std::streamsize>::max());
    const int64_t bounded = std::min<int64_t>(max_bytes, max_stream);
    if (encoding_ == TextEncoding::kLatin1) {
      return static_cast<std::size_t>(std::max<int64_t>(1, bounded / 2));
    }
    return static_cast<std::size_t>(std::max<int64_t>(1, bounded));
  }

  sanitize::Result<std::string> transcode_chunk(std::string_view raw,
                                                bool final) {
    switch (encoding_) {
    case TextEncoding::kLatin1:
      return transcode_latin1(raw);
    case TextEncoding::kUtf16:
    case TextEncoding::kUtf16LE:
    case TextEncoding::kUtf16BE:
      return transcode_utf16(raw, final);
    case TextEncoding::kUtf8:
    case TextEncoding::kUnsupported:
      break;
    }
    return sanitize::Status::Invalid("unsupported text encoding");
  }

  std::string transcode_latin1(std::string_view raw) const {
    std::string out;
    out.reserve(raw.size());
    for (const unsigned char ch : raw) {
      if (ch < 0x80) {
        out.push_back(static_cast<char>(ch));
      } else {
        out.push_back(static_cast<char>(0xc0 | (ch >> 6)));
        out.push_back(static_cast<char>(0x80 | (ch & 0x3f)));
      }
    }
    return out;
  }

  sanitize::Result<std::string> transcode_utf16(std::string_view raw,
                                                bool final) {
    std::string bytes;
    bytes.reserve(raw.size() + (pending_byte_ ? 1 : 0));
    if (pending_byte_) {
      bytes.push_back(static_cast<char>(*pending_byte_));
      pending_byte_.reset();
    }
    bytes.append(raw);

    std::size_t pos = 0;
    if (!bom_checked_) {
      bom_checked_ = true;
      if (bytes.size() >= 2) {
        const auto b0 = static_cast<unsigned char>(bytes[0]);
        const auto b1 = static_cast<unsigned char>(bytes[1]);
        if (b0 == 0xff && b1 == 0xfe) {
          utf16_little_endian_ = true;
          pos = 2;
        } else if (b0 == 0xfe && b1 == 0xff) {
          utf16_little_endian_ = false;
          pos = 2;
        }
      }
    }

    std::string out;
    out.reserve(bytes.size());
    while (pos + 1 < bytes.size()) {
      const auto b0 = static_cast<unsigned char>(bytes[pos]);
      const auto b1 = static_cast<unsigned char>(bytes[pos + 1]);
      const uint16_t unit = utf16_little_endian_
                                ? static_cast<uint16_t>(b0 | (b1 << 8))
                                : static_cast<uint16_t>((b0 << 8) | b1);
      pos += 2;

      if (pending_high_surrogate_) {
        const uint16_t high = *pending_high_surrogate_;
        pending_high_surrogate_.reset();
        if (!is_low_surrogate(unit)) {
          return sanitize::Status::Invalid(
              "UTF-16 decode error: high surrogate is not followed by a low "
              "surrogate");
        }
        const uint32_t cp =
            0x10000u + (((high - 0xd800u) << 10) | (unit - 0xdc00u));
        append_utf8_codepoint(cp, &out);
        continue;
      }

      if (is_high_surrogate(unit)) {
        pending_high_surrogate_ = unit;
        continue;
      }
      if (is_low_surrogate(unit)) {
        return sanitize::Status::Invalid(
            "UTF-16 decode error: unexpected low surrogate");
      }
      append_utf8_codepoint(unit, &out);
    }

    if (pos < bytes.size()) {
      pending_byte_ = static_cast<unsigned char>(bytes[pos]);
    }
    if (final) {
      if (pending_byte_) {
        return sanitize::Status::Invalid(
            "UTF-16 decode error: truncated trailing byte");
      }
      if (pending_high_surrogate_) {
        return sanitize::Status::Invalid(
            "UTF-16 decode error: truncated trailing surrogate");
      }
    }
    return out;
  }

  std::string path_;
  TextEncoding encoding_;
  std::ifstream in_;
  std::size_t utf8_pos_ = 0;
  bool eof_ = false;
  bool bom_checked_ = false;
  bool utf16_little_endian_ = true;
  std::optional<unsigned char> pending_byte_;
  std::optional<uint16_t> pending_high_surrogate_;
  std::shared_ptr<std::string> full_view_;
};

class MultiPathChunkSource final : public ChunkSource {
public:
  MultiPathChunkSource(std::vector<std::string> paths, std::string separator)
      : paths_(std::move(paths)),
        separator_(std::make_shared<std::string>(std::move(separator))) {}

  MultiPathChunkSource(std::vector<std::string> paths, std::string separator,
                       TextEncoding encoding)
      : paths_(std::move(paths)),
        separator_(std::make_shared<std::string>(std::move(separator))),
        encoding_(encoding) {}

  MultiPathChunkSource(std::vector<std::string> paths,
                       std::vector<std::string> source_names,
                       std::string separator)
      : paths_(std::move(paths)),
        separator_(std::make_shared<std::string>(std::move(separator))) {
    source_names_.reserve(source_names.size());
    for (auto &name : source_names) {
      source_names_.push_back(
          std::make_shared<const std::string>(std::move(name)));
    }
  }

  MultiPathChunkSource(std::vector<std::string> paths,
                       std::vector<std::string> source_names,
                       std::string separator, TextEncoding encoding)
      : paths_(std::move(paths)),
        separator_(std::make_shared<std::string>(std::move(separator))),
        encoding_(encoding) {
    source_names_.reserve(source_names.size());
    for (auto &name : source_names) {
      source_names_.push_back(
          std::make_shared<const std::string>(std::move(name)));
    }
  }

  sanitize::Status Reset() override {
    current_.reset();
    current_source_name_.reset();
    current_source_index_ = 0;
    current_has_source_index_ = false;
    index_ = 0;
    source_base_offset_ = 0;
    separator_pending_ = false;
    full_view_.reset();
    return sanitize::Status::OK();
  }

  sanitize::Result<Chunk> NextChunk(int64_t max_bytes) override {
    if (max_bytes <= 0) {
      return sanitize::Status::Invalid("NextChunk: max_bytes must be > 0");
    }
    if (separator_pending_) {
      separator_pending_ = false;
      Chunk c;
      c.owner = separator_;
      c.data = std::string_view(*separator_);
      c.base_offset = source_base_offset_;
      source_base_offset_ += separator_->size();
      return c;
    }
    while (true) {
      if (!current_) {
        if (index_ >= paths_.size()) {
          return Chunk{.owner = nullptr,
                       .data = std::string_view{},
                       .base_offset = source_base_offset_,
                       .source_name_owner = {},
                       .source_name = {},
                       .source_index = 0,
                       .has_source_index = false};
        }
        current_source_name_.reset();
        if (index_ < source_names_.size()) {
          current_source_name_ = source_names_[index_];
          current_source_index_ = index_;
          current_has_source_index_ = true;
        }
        SAN_ASSIGN_OR_RAISE(current_, make_child_source(paths_[index_]));
        ++index_;
      }
      SAN_ASSIGN_OR_RAISE(auto chunk, current_->NextChunk(max_bytes));
      if (!chunk.data.empty()) {
        chunk.base_offset += source_base_offset_;
        if (current_source_name_) {
          chunk.source_name_owner = current_source_name_;
          chunk.source_name = std::string_view(*current_source_name_);
          chunk.source_index = current_source_index_;
          chunk.has_source_index = current_has_source_index_;
        }
        return chunk;
      }
      current_.reset();
      if (index_ < paths_.size() && !separator_->empty()) {
        separator_pending_ = true;
      }
    }
  }

  sanitize::Result<Chunk> View() override {
    if (!full_view_) {
      auto bytes = std::make_shared<std::string>();
      for (std::size_t i = 0; i < paths_.size(); ++i) {
        SAN_ASSIGN_OR_RAISE(auto source, make_child_source(paths_[i]));
        SAN_ASSIGN_OR_RAISE(auto chunk, source->View());
        if (i > 0) {
          bytes->append(*separator_);
        }
        bytes->append(chunk.data);
      }
      full_view_ = std::move(bytes);
    }
    Chunk c;
    c.owner = full_view_;
    c.data = std::string_view(*full_view_);
    c.base_offset = 0;
    return c;
  }

private:
  sanitize::Result<ChunkSourcePtr>
  make_child_source(const std::string &path) const {
    if (encoding_ == TextEncoding::kUtf8) {
      return chunk_source_from_path(path);
    }
    return chunk_source_from_path_with_encoding(path, encoding_name(encoding_));
  }

  std::vector<std::string> paths_;
  std::vector<std::shared_ptr<const std::string>> source_names_;
  std::shared_ptr<std::string> separator_;
  TextEncoding encoding_ = TextEncoding::kUtf8;
  ChunkSourcePtr current_;
  std::shared_ptr<const std::string> current_source_name_;
  std::size_t current_source_index_ = 0;
  bool current_has_source_index_ = false;
  std::size_t index_ = 0;
  std::size_t source_base_offset_ = 0;
  bool separator_pending_ = false;
  std::shared_ptr<std::string> full_view_;
};

enum class DetectedCompression : uint8_t { kNone = 0, kGzip };

// Detects compression from leading magic bytes.
static DetectedCompression sniff_compression(const uint8_t *data,
                                             std::size_t n) {
  if (!data || n < 2)
    return DetectedCompression::kNone;
  // gzip magic: 1F 8B
  if (data[0] == 0x1f && data[1] == 0x8b)
    return DetectedCompression::kGzip;
  return DetectedCompression::kNone;
}

// Detects file compression by reading the leading magic bytes.
static sanitize::Result<DetectedCompression>
sniff_file_compression(const std::string &path) {
  std::ifstream in(path, std::ios::binary);
  if (!in.good()) {
    return sanitize::Status::Invalid("sniff_file_compression: failed to open '",
                                     path, "'");
  }
  std::array<uint8_t, 2> magic{};
  in.read(reinterpret_cast<char *>(magic.data()), magic.size());
  const std::streamsize got = in.gcount();
  return sniff_compression(magic.data(), static_cast<std::size_t>(got));
}

static sanitize::Result<std::string>
read_file_bytes_std(const std::string &path) {
  std::ifstream in(path, std::ios::binary);
  if (!in.good()) {
    return sanitize::Status::Invalid("read_file_bytes_std: failed to open '",
                                     path, "'");
  }
  in.seekg(0, std::ios::end);
  const std::streamoff end = in.tellg();
  if (end < 0) {
    return sanitize::Status::Invalid("read_file_bytes_std: tellg failed for '",
                                     path, "'");
  }
  const auto max_size =
      static_cast<std::uintmax_t>(std::numeric_limits<std::size_t>::max());
  const auto max_stream =
      static_cast<std::uintmax_t>(std::numeric_limits<std::streamsize>::max());
  const auto file_size = static_cast<std::uintmax_t>(end);
  if (file_size > max_size || file_size > max_stream) {
    return sanitize::Status::OutOfMemory(
        "read_file_bytes_std: file too large to read into memory: '", path,
        "'");
  }
  in.seekg(0, std::ios::beg);
  std::string bytes;
  bytes.resize(static_cast<std::size_t>(file_size));
  if (end > 0) {
    in.read(bytes.data(), static_cast<std::streamsize>(file_size));
    if (!in) {
      return sanitize::Status::Invalid("read_file_bytes_std: short read for '",
                                       path, "'");
    }
  }
  return bytes;
}

} // namespace

ChunkSourcePtr chunk_source_from_bytes(std::string bytes) {
  return std::make_shared<OwnedChunkSource>(std::move(bytes));
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_path(const std::string &path) {
  SAN_ASSIGN_OR_RAISE(const auto kind, sniff_file_compression(path));
  if (kind != DetectedCompression::kNone) {
    return sanitize::Status::NotImplemented(
        "chunk_source_from_path: compressed input is not available in core "
        "runtime; provide decompressed input or use an adapter path");
  }
  return std::make_shared<FileChunkSource>(path);
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_path_with_encoding(const std::string &path,
                                     std::string_view encoding) {
  const TextEncoding text_encoding = parse_text_encoding(encoding);
  if (text_encoding == TextEncoding::kUnsupported) {
    return sanitize::Status::NotImplemented("unsupported input_text_encoding: ",
                                            std::string(encoding));
  }
  if (text_encoding == TextEncoding::kUtf8) {
    return chunk_source_from_path(path);
  }
  SAN_ASSIGN_OR_RAISE(const auto kind, sniff_file_compression(path));
  if (kind != DetectedCompression::kNone) {
    return sanitize::Status::NotImplemented(
        "chunk_source_from_path_with_encoding: compressed input is not "
        "available in core runtime; provide decompressed input or use an "
        "adapter path");
  }
  return std::make_shared<TranscodingFileChunkSource>(path, text_encoding);
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths(std::vector<std::string> paths, std::string separator) {
  if (paths.empty()) {
    return sanitize::Status::Invalid("chunk_source_from_paths: no paths");
  }
  return std::make_shared<MultiPathChunkSource>(std::move(paths),
                                                std::move(separator));
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_encoding(std::vector<std::string> paths,
                                      std::string separator,
                                      std::string_view encoding) {
  if (paths.empty()) {
    return sanitize::Status::Invalid(
        "chunk_source_from_paths_with_encoding: no paths");
  }
  const TextEncoding text_encoding = parse_text_encoding(encoding);
  if (text_encoding == TextEncoding::kUnsupported) {
    return sanitize::Status::NotImplemented("unsupported input_text_encoding: ",
                                            std::string(encoding));
  }
  return std::make_shared<MultiPathChunkSource>(
      std::move(paths), std::move(separator), text_encoding);
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_source_names(std::vector<std::string> paths,
                                          std::vector<std::string> source_names,
                                          std::string separator) {
  if (paths.empty()) {
    return sanitize::Status::Invalid(
        "chunk_source_from_paths_with_source_names: no paths");
  }
  if (paths.size() != source_names.size()) {
    return sanitize::Status::Invalid(
        "chunk_source_from_paths_with_source_names: paths/source_names size "
        "mismatch");
  }
  return std::make_shared<MultiPathChunkSource>(
      std::move(paths), std::move(source_names), std::move(separator));
}

sanitize::Result<ChunkSourcePtr>
chunk_source_from_paths_with_source_names_encoding(
    std::vector<std::string> paths, std::vector<std::string> source_names,
    std::string separator, std::string_view encoding) {
  if (paths.empty()) {
    return sanitize::Status::Invalid(
        "chunk_source_from_paths_with_source_names_encoding: no paths");
  }
  if (paths.size() != source_names.size()) {
    return sanitize::Status::Invalid(
        "chunk_source_from_paths_with_source_names_encoding: "
        "paths/source_names size mismatch");
  }
  const TextEncoding text_encoding = parse_text_encoding(encoding);
  if (text_encoding == TextEncoding::kUnsupported) {
    return sanitize::Status::NotImplemented("unsupported input_text_encoding: ",
                                            std::string(encoding));
  }
  return std::make_shared<MultiPathChunkSource>(
      std::move(paths), std::move(source_names), std::move(separator),
      text_encoding);
}

} // namespace sanitize

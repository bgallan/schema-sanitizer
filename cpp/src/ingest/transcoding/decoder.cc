// Implements incremental Latin-1 and UTF-16 decoding to UTF-8.

#include "ingest/transcoding/decoder.hh"

#include <algorithm>
#include <limits>

namespace sanitize::internal {
namespace {

void append_utf8_codepoint(std::uint32_t codepoint, std::string *out) {
  if (codepoint <= 0x7f) {
    out->push_back(static_cast<char>(codepoint));
  } else if (codepoint <= 0x7ff) {
    out->push_back(static_cast<char>(0xc0 | (codepoint >> 6)));
    out->push_back(static_cast<char>(0x80 | (codepoint & 0x3f)));
  } else if (codepoint <= 0xffff) {
    out->push_back(static_cast<char>(0xe0 | (codepoint >> 12)));
    out->push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | (codepoint & 0x3f)));
  } else {
    out->push_back(static_cast<char>(0xf0 | (codepoint >> 18)));
    out->push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3f)));
    out->push_back(static_cast<char>(0x80 | (codepoint & 0x3f)));
  }
}

bool is_high_surrogate(std::uint16_t value) {
  return value >= 0xd800 && value <= 0xdbff;
}

bool is_low_surrogate(std::uint16_t value) {
  return value >= 0xdc00 && value <= 0xdfff;
}

} // namespace

TranscodingDecoder::TranscodingDecoder(TextEncoding encoding)
    : encoding_(encoding) {
  Reset();
}

void TranscodingDecoder::Reset() {
  bom_checked_ = false;
  utf16_little_endian_ = encoding_ != TextEncoding::kUtf16BE;
  pending_byte_.reset();
  pending_high_surrogate_.reset();
}

std::size_t TranscodingDecoder::raw_read_size(std::int64_t max_bytes) const {
  const auto max_stream =
      static_cast<std::int64_t>(std::numeric_limits<std::streamsize>::max());
  const auto bounded = std::min<std::int64_t>(max_bytes, max_stream);
  if (encoding_ == TextEncoding::kLatin1) {
    return static_cast<std::size_t>(std::max<std::int64_t>(1, bounded / 2));
  }
  return static_cast<std::size_t>(std::max<std::int64_t>(1, bounded));
}

sanitize::Result<std::string> TranscodingDecoder::Decode(std::string_view raw,
                                                         bool final) {
  if (encoding_ == TextEncoding::kLatin1) {
    return transcode_latin1(raw);
  }
  if (encoding_ == TextEncoding::kUtf16 ||
      encoding_ == TextEncoding::kUtf16LE ||
      encoding_ == TextEncoding::kUtf16BE) {
    return transcode_utf16(raw, final);
  }
  return sanitize::Status::Invalid("unsupported text encoding");
}

std::string TranscodingDecoder::transcode_latin1(std::string_view raw) {
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

sanitize::Result<std::string>
TranscodingDecoder::transcode_utf16(std::string_view raw, bool final) {
  std::string bytes;
  bytes.reserve(raw.size() + (pending_byte_ ? 1 : 0));
  if (pending_byte_) {
    bytes.push_back(static_cast<char>(*pending_byte_));
    pending_byte_.reset();
  }
  bytes.append(raw);

  std::size_t pos = consume_bom(bytes);
  std::string out;
  out.reserve(bytes.size());
  while (pos + 1 < bytes.size()) {
    const auto b0 = static_cast<unsigned char>(bytes[pos]);
    const auto b1 = static_cast<unsigned char>(bytes[pos + 1]);
    const auto unit = utf16_little_endian_
                          ? static_cast<std::uint16_t>(b0 | (b1 << 8))
                          : static_cast<std::uint16_t>((b0 << 8) | b1);
    pos += 2;
    SAN_RETURN_NOT_OK(append_utf16_unit(unit, &out));
  }
  if (pos < bytes.size()) {
    pending_byte_ = static_cast<unsigned char>(bytes[pos]);
  }
  if (final && pending_byte_) {
    return sanitize::Status::Invalid(
        "UTF-16 decode error: truncated trailing byte");
  }
  if (final && pending_high_surrogate_) {
    return sanitize::Status::Invalid(
        "UTF-16 decode error: truncated trailing surrogate");
  }
  return out;
}

std::size_t TranscodingDecoder::consume_bom(std::string_view bytes) {
  if (bom_checked_) {
    return 0;
  }
  bom_checked_ = true;
  if (bytes.size() < 2) {
    return 0;
  }
  const auto b0 = static_cast<unsigned char>(bytes[0]);
  const auto b1 = static_cast<unsigned char>(bytes[1]);
  if (b0 == 0xff && b1 == 0xfe) {
    utf16_little_endian_ = true;
    return 2;
  }
  if (b0 == 0xfe && b1 == 0xff) {
    utf16_little_endian_ = false;
    return 2;
  }
  return 0;
}

sanitize::Status TranscodingDecoder::append_utf16_unit(std::uint16_t unit,
                                                       std::string *out) {
  if (pending_high_surrogate_) {
    const auto high = *pending_high_surrogate_;
    pending_high_surrogate_.reset();
    if (!is_low_surrogate(unit)) {
      return sanitize::Status::Invalid(
          "UTF-16 decode error: high surrogate is not followed by a low "
          "surrogate");
    }
    append_utf8_codepoint(
        0x10000u + (((high - 0xd800u) << 10) | (unit - 0xdc00u)), out);
    return {};
  }
  if (is_high_surrogate(unit)) {
    pending_high_surrogate_ = unit;
    return {};
  }
  if (is_low_surrogate(unit)) {
    return sanitize::Status::Invalid(
        "UTF-16 decode error: unexpected low surrogate");
  }
  append_utf8_codepoint(unit, out);
  return {};
}

} // namespace sanitize::internal

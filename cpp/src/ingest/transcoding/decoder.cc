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

sanitize::Result<std::size_t>
latin1_output_size(std::string_view raw) {
  std::size_t non_ascii = 0;
  for (const unsigned char ch : raw) {
    non_ascii += ch >= 0x80 ? 1U : 0U;
  }
  if (non_ascii > std::numeric_limits<std::size_t>::max() - raw.size()) {
    return sanitize::Status::OutOfMemory(
        "Latin-1 decode output size overflow");
  }
  return raw.size() + non_ascii;
}

sanitize::Result<std::size_t>
utf16_reserve_size(std::size_t raw_size, bool has_pending_byte,
                   bool has_pending_high_surrogate) {
  if (has_pending_byte && raw_size == std::numeric_limits<std::size_t>::max()) {
    return sanitize::Status::OutOfMemory("UTF-16 decode input size overflow");
  }
  const auto available = raw_size + (has_pending_byte ? 1U : 0U);
  const auto pairs = available / 2U;
  const auto extra = has_pending_high_surrogate ? 1U : 0U;
  if (pairs > (std::numeric_limits<std::size_t>::max() - extra) / 3U) {
    return sanitize::Status::OutOfMemory("UTF-16 decode output size overflow");
  }
  return pairs * 3U + extra;
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
  // Latin-1 and a UTF-16 low surrogate paired with a carried high surrogate
  // can both expand newly read input to roughly twice its raw size. Reading at
  // most half of the requested UTF-8 budget bounds that expansion. The source
  // slices a rare oversized scalar without copying it before returning it.
  return static_cast<std::size_t>(std::max<std::int64_t>(1, bounded / 2));
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

sanitize::Result<std::string>
TranscodingDecoder::transcode_latin1(std::string_view raw) {
  SAN_ASSIGN_OR_RAISE(const auto output_size, latin1_output_size(raw));
  std::string out(output_size, '\0');
  std::size_t write = 0;
  for (const unsigned char ch : raw) {
    if (ch < 0x80) {
      out[write++] = static_cast<char>(ch);
    } else {
      out[write++] = static_cast<char>(0xc0 | (ch >> 6));
      out[write++] = static_cast<char>(0x80 | (ch & 0x3f));
    }
  }
  return out;
}

sanitize::Result<std::string>
TranscodingDecoder::transcode_utf16(std::string_view raw, bool final) {
  SAN_ASSIGN_OR_RAISE(
      const auto reserve_size,
      utf16_reserve_size(raw.size(), pending_byte_.has_value(),
                         pending_high_surrogate_.has_value()));
  std::string out;
  out.reserve(reserve_size);

  std::size_t pos = 0;
  if (pending_byte_) {
    if (raw.empty()) {
      if (final) {
        return sanitize::Status::Invalid(
            "UTF-16 decode error: truncated trailing byte");
      }
      return out;
    }
    const auto first = *pending_byte_;
    pending_byte_.reset();
    SAN_RETURN_NOT_OK(append_utf16_pair(
        first, static_cast<unsigned char>(raw.front()), &out));
    pos = 1;
  }

  while (pos + 1 < raw.size()) {
    SAN_RETURN_NOT_OK(append_utf16_pair(
        static_cast<unsigned char>(raw[pos]),
        static_cast<unsigned char>(raw[pos + 1]), &out));
    pos += 2;
  }
  if (pos < raw.size()) {
    pending_byte_ = static_cast<unsigned char>(raw[pos]);
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

sanitize::Status TranscodingDecoder::append_utf16_pair(
    unsigned char b0, unsigned char b1, std::string *out) {
  if (!bom_checked_) {
    bom_checked_ = true;
    if (b0 == 0xff && b1 == 0xfe) {
      utf16_little_endian_ = true;
      return {};
    }
    if (b0 == 0xfe && b1 == 0xff) {
      utf16_little_endian_ = false;
      return {};
    }
  }
  const auto unit = utf16_little_endian_
                        ? static_cast<std::uint16_t>(b0 | (b1 << 8U))
                        : static_cast<std::uint16_t>((b0 << 8U) | b1);
  return append_utf16_unit(unit, out);
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

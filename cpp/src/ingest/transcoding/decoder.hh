// Incremental canonical text decoders used by transcoding chunk sources.

#pragma once

#include "ingest/chunk_source_detail.hh"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace sanitize::internal {

class TranscodingDecoder {
public:
  explicit TranscodingDecoder(TextEncoding encoding);

  void Reset();
  [[nodiscard]] std::size_t raw_read_size(std::int64_t max_bytes) const;
  sanitize::Result<std::string> Decode(std::string_view raw, bool final);

private:
  static sanitize::Result<std::string>
  transcode_latin1(std::string_view raw);
  sanitize::Result<std::string> transcode_utf16(std::string_view raw,
                                                bool final);
  sanitize::Status append_utf16_pair(unsigned char b0, unsigned char b1,
                                     std::string *out);
  sanitize::Status append_utf16_unit(std::uint16_t unit, std::string *out);

  TextEncoding encoding_;
  bool bom_checked_ = false;
  bool utf16_little_endian_ = true;
  std::optional<unsigned char> pending_byte_;
  std::optional<std::uint16_t> pending_high_surrogate_;
};

} // namespace sanitize::internal

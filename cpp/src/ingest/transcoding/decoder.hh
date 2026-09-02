// Declares incremental canonical text decoders used by transcoding chunk
// sources. The implementation preserves split code units and bounded buffers
// across incremental source reads.

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
  /// Converts a Latin-1 chunk to UTF-8 after computing its exact output size.
  static sanitize::Result<std::string> transcode_latin1(std::string_view raw);

  /// Decodes UTF-16 incrementally while retaining split bytes and surrogate
  /// state.
  sanitize::Result<std::string> transcode_utf16(std::string_view raw,
                                                bool final);

  /// Applies BOM-selected byte order and decodes one byte pair as a UTF-16
  /// unit.
  sanitize::Status append_utf16_pair(unsigned char b0, unsigned char b1,
                                     std::string *out);

  /// Validates surrogate sequencing and appends the resulting scalar as UTF-8.
  sanitize::Status append_utf16_unit(std::uint16_t unit, std::string *out);

  TextEncoding encoding_;
  bool bom_checked_ = false;
  bool utf16_little_endian_ = true;
  std::optional<unsigned char> pending_byte_;
  std::optional<std::uint16_t> pending_high_surrogate_;
};

} // namespace sanitize::internal

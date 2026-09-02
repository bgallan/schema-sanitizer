// Defines the canonical text encoding names accepted by ingestion. It
// translates user spellings and native enum values at the chunk-source
// boundary.

#include "ingest/chunk_source_detail.hh"

#include <string_view>

namespace sanitize::internal {

/// Parses a supported encoding spelling into the canonical ingestion enum.
TextEncoding parse_text_encoding(std::string_view encoding) {
  if (encoding == "utf-8") {
    return TextEncoding::kUtf8;
  }
  if (encoding == "iso8859-1") {
    return TextEncoding::kLatin1;
  }
  if (encoding == "utf-16") {
    return TextEncoding::kUtf16;
  }
  if (encoding == "utf-16-le") {
    return TextEncoding::kUtf16LE;
  }
  if (encoding == "utf-16-be") {
    return TextEncoding::kUtf16BE;
  }
  return TextEncoding::kUnsupported;
}

/// Returns the canonical option spelling for a native ingestion encoding.
std::string_view text_encoding_name(TextEncoding encoding) {
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
    return "utf-8";
  }
  return "utf-8";
}

} // namespace sanitize::internal

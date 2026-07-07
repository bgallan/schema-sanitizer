// Decodes escaped JSON string slices into arena-provided output buffers.

#include "internal/parsing/json_string_decode.hh"

#include "internal/parsing/json_ondemand_scan.hh"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "sanitize/core/status.hh"

namespace sanitize::internal::json_string_decode {

namespace {

struct DecodeContext {
  char *out = nullptr;
  std::size_t n = 0;
  const char *p = nullptr;
  const char *end = nullptr;
  std::string_view full_text;
  std::size_t base_offset = 0;
  const DecodeErrors *errors = nullptr;
};

// Builds a JSON string parse error with an absolute byte offset.
sanitize::Status parse_error(std::string_view msg, std::size_t offset) {
  return sanitize::Status::Invalid(std::string(msg), " at byte ",
                                   std::to_string(offset));
}

// Returns the input byte offset for the decoder cursor.
std::size_t byte_offset(const DecodeContext &ctx) {
  return ctx.base_offset +
         static_cast<std::size_t>(ctx.p - ctx.full_text.data());
}

// Builds a JSON string parse error at the current decoder cursor.
sanitize::Status parse_error_at_cursor(const DecodeContext &ctx,
                                       std::string_view msg) {
  return parse_error(msg, byte_offset(ctx));
}

// Appends one simple JSON escape sequence.
sanitize::Status append_simple_escape(char esc, DecodeContext *ctx) {
  switch (esc) {
  case '"':
    ctx->out[ctx->n++] = '"';
    return sanitize::Status::OK();
  case '\\':
    ctx->out[ctx->n++] = '\\';
    return sanitize::Status::OK();
  case '/':
    ctx->out[ctx->n++] = '/';
    return sanitize::Status::OK();
  case 'b':
    ctx->out[ctx->n++] = '\b';
    return sanitize::Status::OK();
  case 'f':
    ctx->out[ctx->n++] = '\f';
    return sanitize::Status::OK();
  case 'n':
    ctx->out[ctx->n++] = '\n';
    return sanitize::Status::OK();
  case 'r':
    ctx->out[ctx->n++] = '\r';
    return sanitize::Status::OK();
  case 't':
    ctx->out[ctx->n++] = '\t';
    return sanitize::Status::OK();
  default:
    return parse_error_at_cursor(*ctx, ctx->errors->invalid_escape);
  }
}

// Reads a four-hex-digit Unicode code unit and advances the cursor.
sanitize::Result<uint32_t> read_hex_quad(DecodeContext *ctx,
                                         std::string_view invalid_message) {
  int h0 = json_scan::hex_val(ctx->p[0]), h1 = json_scan::hex_val(ctx->p[1]);
  int h2 = json_scan::hex_val(ctx->p[2]), h3 = json_scan::hex_val(ctx->p[3]);
  if (h0 < 0 || h1 < 0 || h2 < 0 || h3 < 0) {
    return parse_error_at_cursor(*ctx, invalid_message);
  }
  auto value = static_cast<uint32_t>((h0 << 12) | (h1 << 8) | (h2 << 4) | h3);
  ctx->p += 4;
  return value;
}

// Reads and validates a low-surrogate escape after a high surrogate.
sanitize::Result<uint32_t> read_low_surrogate(DecodeContext *ctx) {
  if (ctx->p + 6 > ctx->end || ctx->p[0] != '\\' || ctx->p[1] != 'u') {
    return parse_error_at_cursor(*ctx, ctx->errors->missing_low_surrogate);
  }
  ctx->p += 2;
  SAN_ASSIGN_OR_RAISE(
      uint32_t low, read_hex_quad(ctx, ctx->errors->invalid_low_surrogate_hex));
  if (low < 0xDC00 || low > 0xDFFF) {
    return parse_error_at_cursor(*ctx,
                                 ctx->errors->invalid_low_surrogate_range);
  }
  return low;
}

// Decodes one \uXXXX escape and appends its UTF-8 bytes.
sanitize::Status append_unicode_escape(DecodeContext *ctx) {
  if (ctx->p + 4 > ctx->end) {
    return parse_error_at_cursor(*ctx, ctx->errors->incomplete_unicode_escape);
  }
  SAN_ASSIGN_OR_RAISE(uint32_t cp,
                      read_hex_quad(ctx, ctx->errors->invalid_unicode_hex));

  if (cp >= 0xD800 && cp <= 0xDBFF) {
    SAN_ASSIGN_OR_RAISE(uint32_t low, read_low_surrogate(ctx));
    cp = 0x10000 + (((cp - 0xD800) << 10) | (low - 0xDC00));
  } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
    return parse_error_at_cursor(*ctx, ctx->errors->unexpected_low_surrogate);
  }

  json_scan::append_utf8(cp, ctx->out, ctx->n);
  return sanitize::Status::OK();
}

// Decodes one JSON escape sequence and appends the resulting bytes.
sanitize::Status append_escape(DecodeContext *ctx) {
  if (ctx->p >= ctx->end) {
    return parse_error_at_cursor(*ctx, ctx->errors->truncated_escape);
  }
  const char esc = *ctx->p++;
  if (esc == 'u') {
    return append_unicode_escape(ctx);
  }
  return append_simple_escape(esc, ctx);
}

} // namespace

sanitize::Result<std::string_view>
decode_json_string_slice(char *out, const char *begin, const char *end,
                         std::string_view full_text, std::size_t base_offset,
                         const DecodeErrors &errors) {
  if (!out) {
    return sanitize::Status::Invalid("decode_json_string_slice: out is null");
  }

  DecodeContext ctx{.out = out,
                    .p = begin,
                    .end = end,
                    .full_text = full_text,
                    .base_offset = base_offset,
                    .errors = &errors};
  while (ctx.p < ctx.end) {
    char x = *ctx.p++;
    if (x != '\\') {
      ctx.out[ctx.n++] = x;
      continue;
    }
    SAN_RETURN_NOT_OK(append_escape(&ctx));
  }
  return std::string_view(out, ctx.n);
}

} // namespace sanitize::internal::json_string_decode

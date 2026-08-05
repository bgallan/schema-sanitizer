// Declares low-level JSON cursor scanning primitives.

#pragma once

#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>

namespace sanitize::internal::json_scan {

inline constexpr std::size_t kMaxJsonNestingDepth = 512;
inline constexpr std::size_t kMaxJsonObjectFields = 65'536;

struct Cursor {
  const char *p = nullptr;
  const char *end = nullptr;
  std::size_t base = 0;
  const char *text_begin = nullptr;
  bool saw_escape = false;

  // Returns the absolute input offset at the cursor.
  [[nodiscard]] std::size_t offset() const {
    return base + static_cast<std::size_t>(p - text_begin);
  }
};

// Returns whether the byte is JSON whitespace.
bool is_ws(char c);

// Advances past JSON whitespace.
void skip_ws(Cursor &c);

// Consumes the expected character or returns a parse error.
sanitize::Status expect(Cursor &c, char ch);

// Converts a hexadecimal digit to its numeric value.
int hex_val(char x);

// Appends a Unicode scalar value as UTF-8 bytes.
void append_utf8(uint32_t cp, char *out, std::size_t &n);

// Scans a JSON string and reports its raw content span.
sanitize::Status scan_string(Cursor &c, const char *&out_begin,
                             const char *&out_end, bool &has_esc);

// Advances over one JSON string.
sanitize::Status skip_string(Cursor &c);

// Advances over one JSON number.
sanitize::Status skip_number(Cursor &c);

// Advances over a fixed JSON literal.
sanitize::Status skip_literal(Cursor &c, const char *lit, std::size_t n);

// Advances over one JSON object, including all nested values.
sanitize::Status skip_object(Cursor &c);

// Advances over one JSON array, including all nested values.
sanitize::Status skip_array(Cursor &c);

// Advances over one complete JSON value.
sanitize::Status skip_value(Cursor &c);

} // namespace sanitize::internal::json_scan

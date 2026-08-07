// Implements lexical JSON cursor scanning primitives.

#include "internal/parsing/json/ondemand/scan.hh"

#include "sanitize/core/status.hh"

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

namespace sanitize::internal::json_scan {

namespace {

// Returns whether a character is an ASCII decimal digit.
bool is_digit(char ch) { return ch >= '0' && ch <= '9'; }

// Builds a JSON parse error at the cursor.
sanitize::Status parse_error_at(const Cursor &c, std::string_view message) {
  return sanitize::Status::Invalid(message, std::to_string(c.offset()));
}

// Consumes a contiguous ASCII digit run.
void consume_digits(Cursor &c) {
  while (c.p < c.end && is_digit(*c.p))
    ++c.p;
}

// Consumes the optional sign before a JSON number.
void consume_optional_minus(Cursor &c) {
  if (c.p < c.end && *c.p == '-')
    ++c.p;
}

// Consumes the required integer part of a JSON number.
sanitize::Status consume_integer_part(Cursor &c) {
  if (c.p >= c.end) {
    return parse_error_at(c, "JSON parse error: invalid number at byte ");
  }
  if (*c.p == '0') {
    ++c.p;
    return sanitize::Status::OK();
  }
  if (*c.p >= '1' && *c.p <= '9') {
    consume_digits(c);
    return sanitize::Status::OK();
  }
  return parse_error_at(c, "JSON parse error: invalid number at byte ");
}

// Consumes an optional fractional part of a JSON number.
sanitize::Status consume_fraction_part(Cursor &c) {
  if (c.p >= c.end || *c.p != '.') {
    return sanitize::Status::OK();
  }
  ++c.p;
  if (c.p >= c.end || !is_digit(*c.p)) {
    return parse_error_at(c, "JSON parse error: invalid fraction at byte ");
  }
  consume_digits(c);
  return sanitize::Status::OK();
}

// Consumes an optional exponent part of a JSON number.
sanitize::Status consume_exponent_part(Cursor &c) {
  if (c.p >= c.end || (*c.p != 'e' && *c.p != 'E')) {
    return sanitize::Status::OK();
  }
  ++c.p;
  if (c.p < c.end && (*c.p == '+' || *c.p == '-'))
    ++c.p;
  if (c.p >= c.end || !is_digit(*c.p)) {
    return parse_error_at(c, "JSON parse error: invalid exponent at byte ");
  }
  consume_digits(c);
  return sanitize::Status::OK();
}

} // namespace

bool is_ws(char c) { return c == ' ' || c == '\n' || c == '\r' || c == '\t'; }

void skip_ws(Cursor &c) {
  while (c.p < c.end && is_ws(*c.p))
    ++c.p;
}

sanitize::Status expect(Cursor &c, char ch) {
  if (c.p >= c.end || *c.p != ch) {
    return sanitize::Status::Invalid("JSON parse error: expected '",
                                     std::string(1, ch), "' at byte ",
                                     std::to_string(c.offset()));
  }
  ++c.p;
  return sanitize::Status::OK();
}

int hex_val(char x) {
  if (x >= '0' && x <= '9')
    return x - '0';
  if (x >= 'a' && x <= 'f')
    return 10 + (x - 'a');
  if (x >= 'A' && x <= 'F')
    return 10 + (x - 'A');
  return -1;
}

void append_utf8(uint32_t cp, char *out, std::size_t &n) {
  if (cp <= 0x7F) {
    out[n++] = static_cast<char>(cp);
  } else if (cp <= 0x7FF) {
    out[n++] = static_cast<char>(0xC0 | (cp >> 6));
    out[n++] = static_cast<char>(0x80 | (cp & 0x3F));
  } else if (cp <= 0xFFFF) {
    out[n++] = static_cast<char>(0xE0 | (cp >> 12));
    out[n++] = static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
    out[n++] = static_cast<char>(0x80 | (cp & 0x3F));
  } else {
    out[n++] = static_cast<char>(0xF0 | (cp >> 18));
    out[n++] = static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
    out[n++] = static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
    out[n++] = static_cast<char>(0x80 | (cp & 0x3F));
  }
}

sanitize::Status scan_string(Cursor &c, const char *&out_begin,
                             const char *&out_end, bool &has_esc) {
  SAN_RETURN_NOT_OK(expect(c, '"'));
  const char *start = c.p;
  const char *q = c.p;
  has_esc = false;

  const auto error_at = [&](const char *position,
                            std::string_view message) -> sanitize::Status {
    return sanitize::Status::Invalid(
        message, std::to_string(c.base + static_cast<std::size_t>(
                                             position - c.text_begin)));
  };
  const auto is_continuation = [](unsigned char byte) noexcept {
    return byte >= 0x80U && byte <= 0xBFU;
  };
  const auto read_hex_quad = [&](const char *position,
                                 std::uint32_t *value) -> sanitize::Status {
    if (position > c.end || c.end - position < 4) {
      return error_at(position,
                      "JSON parse error: incomplete \\uXXXX escape at byte ");
    }
    std::uint32_t decoded = 0;
    for (unsigned index = 0; index < 4U; ++index) {
      const int digit = hex_val(position[index]);
      if (digit < 0) {
        return error_at(
            position + index,
            "JSON parse error: invalid hex in \\uXXXX escape at byte ");
      }
      decoded = (decoded << 4U) | static_cast<std::uint32_t>(digit);
    }
    *value = decoded;
    return sanitize::Status::OK();
  };

  while (q < c.end) {
    const char *character_position = q;
    const auto ch = static_cast<unsigned char>(*q++);
    if (ch == static_cast<unsigned char>('"')) {
      out_begin = start;
      out_end = q - 1;
      c.p = q;
      return sanitize::Status::OK();
    }
    if (ch == static_cast<unsigned char>('\\')) {
      has_esc = true;
      c.saw_escape = true;
      if (q >= c.end) {
        return error_at(character_position,
                        "JSON parse error: unterminated escape at byte ");
      }
      const char escape = *q++;
      if (escape == '"' || escape == '\\' || escape == '/' || escape == 'b' ||
          escape == 'f' || escape == 'n' || escape == 'r' || escape == 't') {
        continue;
      }
      if (escape != 'u') {
        return error_at(q - 1, "JSON parse error: invalid escape at byte ");
      }

      std::uint32_t code_unit = 0;
      SAN_RETURN_NOT_OK(read_hex_quad(q, &code_unit));
      q += 4;
      if (code_unit >= 0xD800U && code_unit <= 0xDBFFU) {
        if (q > c.end || c.end - q < 6 || q[0] != '\\' || q[1] != 'u') {
          return error_at(q,
                          "JSON parse error: missing low surrogate at byte ");
        }
        std::uint32_t low = 0;
        SAN_RETURN_NOT_OK(read_hex_quad(q + 2, &low));
        if (low < 0xDC00U || low > 0xDFFFU) {
          return error_at(q + 2,
                          "JSON parse error: invalid low surrogate at byte ");
        }
        q += 6;
      } else if (code_unit >= 0xDC00U && code_unit <= 0xDFFFU) {
        return error_at(q - 4,
                        "JSON parse error: unexpected low surrogate at byte ");
      }
      continue;
    }
    if (ch < 0x20U) {
      return error_at(character_position,
                      "JSON parse error: control char in string at byte ");
    }
    if (ch < 0x80U) {
      continue;
    }

    std::size_t continuation_count = 0;
    unsigned char second_min = 0x80U;
    unsigned char second_max = 0xBFU;
    if (ch >= 0xC2U && ch <= 0xDFU) {
      continuation_count = 1;
    } else if (ch >= 0xE0U && ch <= 0xEFU) {
      continuation_count = 2;
      if (ch == 0xE0U) {
        second_min = 0xA0U;
      } else if (ch == 0xEDU) {
        second_max = 0x9FU;
      }
    } else if (ch >= 0xF0U && ch <= 0xF4U) {
      continuation_count = 3;
      if (ch == 0xF0U) {
        second_min = 0x90U;
      } else if (ch == 0xF4U) {
        second_max = 0x8FU;
      }
    } else {
      return error_at(character_position,
                      "JSON parse error: invalid UTF-8 lead byte at byte ");
    }

    if (static_cast<std::size_t>(c.end - q) < continuation_count) {
      return error_at(character_position,
                      "JSON parse error: truncated UTF-8 sequence at byte ");
    }
    const auto second = static_cast<unsigned char>(q[0]);
    if (second < second_min || second > second_max) {
      return error_at(q,
                      "JSON parse error: invalid UTF-8 continuation at byte ");
    }
    for (std::size_t index = 1; index < continuation_count; ++index) {
      if (!is_continuation(static_cast<unsigned char>(q[index]))) {
        return error_at(
            q + index, "JSON parse error: invalid UTF-8 continuation at byte ");
      }
    }
    q += continuation_count;
  }
  return error_at(start, "JSON parse error: unterminated string at byte ");
}

sanitize::Status skip_string(Cursor &c) {
  const char *begin = nullptr;
  const char *end = nullptr;
  bool has_escape = false;
  return scan_string(c, begin, end, has_escape);
}

sanitize::Status skip_number(Cursor &c) {
  const char *start = c.p;
  consume_optional_minus(c);
  SAN_RETURN_NOT_OK(consume_integer_part(c));
  SAN_RETURN_NOT_OK(consume_fraction_part(c));
  SAN_RETURN_NOT_OK(consume_exponent_part(c));
  if (c.p == start) {
    return parse_error_at(c, "JSON parse error: invalid number at byte ");
  }
  return sanitize::Status::OK();
}

sanitize::Status skip_literal(Cursor &c, const char *lit, std::size_t n) {
  if (std::cmp_less(c.end - c.p, n) || std::memcmp(c.p, lit, n) != 0) {
    return sanitize::Status::Invalid(
        "JSON parse error: expected literal at byte ",
        std::to_string(c.offset()));
  }
  c.p += n;
  return sanitize::Status::OK();
}

} // namespace sanitize::internal::json_scan

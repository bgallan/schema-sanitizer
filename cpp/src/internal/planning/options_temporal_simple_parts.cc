// Implements cursor helpers for simple temporal regex fast paths.

#include "internal/planning/options_temporal_simple_parts.hh"

namespace sanitize::internal::options_temporal_simple_parts {

/// Consumes one capture token such as (\d{4}) from a pattern cursor.
bool consume_capture(std::string_view pattern, std::size_t *pos,
                     std::string_view token) {
  if (!pos || *pos > pattern.size() ||
      pattern.substr(*pos, token.size()) != token) {
    return false;
  }
  *pos += token.size();
  return true;
}

/// Consumes one literal separator, allowing regex-escaped punctuation.
bool consume_literal_separator(std::string_view pattern, std::size_t *pos,
                               char *out) {
  if (!pos || !out || *pos >= pattern.size()) {
    return false;
  }
  char c = pattern[*pos];
  if (c == '\\') {
    if (*pos + 1 >= pattern.size()) {
      return false;
    }
    *out = pattern[*pos + 1];
    *pos += 2;
    return true;
  }
  *out = c;
  ++*pos;
  return true;
}

/// Parses exactly n ASCII digits from a string cursor.
bool parse_n_digits(std::string_view s, std::size_t *pos, std::size_t n,
                    int *out) {
  if (!pos || !out || *pos > s.size() || n > s.size() - *pos) {
    return false;
  }
  int value = 0;
  for (std::size_t i = 0; i < n; ++i) {
    const char c = s[*pos + i];
    if (c < '0' || c > '9') {
      return false;
    }
    value = value * 10 + static_cast<int>(c - '0');
  }
  *pos += n;
  *out = value;
  return true;
}

/// Consumes one expected byte from a string cursor.
bool consume_char(std::string_view s, std::size_t *pos, char expected) {
  if (!pos || *pos >= s.size() || s[*pos] != expected) {
    return false;
  }
  ++*pos;
  return true;
}

/// Parses fractional seconds and normalizes the value to nanoseconds.
bool parse_fraction_ns(std::string_view s, std::size_t *pos, int64_t *out) {
  if (!pos || !out || *pos >= s.size()) {
    return false;
  }
  int64_t value = 0;
  std::size_t digits = 0;
  while (*pos < s.size() && digits < 9) {
    const char c = s[*pos];
    if (c < '0' || c > '9') {
      break;
    }
    value = value * 10 + static_cast<int64_t>(c - '0');
    ++*pos;
    ++digits;
  }
  if (digits == 0) {
    return false;
  }
  for (; digits < 9; ++digits) {
    value *= 10;
  }
  *out = value;
  return true;
}

/// Parses a numeric timezone offset into signed seconds.
bool parse_numeric_timezone(std::string_view s, std::size_t *pos,
                            int *out_seconds) {
  if (!pos || !out_seconds || *pos >= s.size()) {
    return false;
  }
  const char sign_ch = s[*pos];
  if (sign_ch != '+' && sign_ch != '-') {
    return false;
  }
  const int sign = sign_ch == '-' ? -1 : 1;
  ++*pos;
  int hours = 0;
  int minutes = 0;
  if (!parse_n_digits(s, pos, 2, &hours)) {
    return false;
  }
  if (*pos < s.size() && s[*pos] == ':') {
    ++*pos;
  }
  if (!parse_n_digits(s, pos, 2, &minutes)) {
    return false;
  }
  if (hours > 23 || minutes > 59) {
    return false;
  }
  *out_seconds = sign * (hours * 3600 + minutes * 60);
  return true;
}

/// Consumes the literal UTC Z suffix from a simple temporal regex pattern.
bool consume_z_timezone_pattern(std::string_view pattern, std::size_t *pos,
                                sanitize::SimpleTemporalPattern *p) {
  if (!pos || !p || pattern.substr(*pos) != "Z") {
    return false;
  }
  p->has_timezone = true;
  p->timezone_z = true;
  *pos = pattern.size();
  return true;
}

} // namespace sanitize::internal::options_temporal_simple_parts

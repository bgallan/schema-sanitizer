// Parses ISO clock times into seconds since midnight.
// The fixed-width path validates separators and civil clock ranges before
// committing an Arrow-compatible scalar value.

#include "sanitize/core/primitives.hh"

#include "core/temporal/parse_internal.hh"

#include <cstdint>
#include <string_view>

namespace sanitize {

bool parse_iso_time_to_seconds(std::string_view s, int32_t *out_seconds) {
  if (!out_seconds || s.size() != 8 || s[2] != ':' || s[5] != ':')
    return false;
  int hours = 0;
  int minutes = 0;
  int seconds = 0;
  if (!temporal_internal::parse_2d(s, 0, &hours) ||
      !temporal_internal::parse_2d(s, 3, &minutes) ||
      !temporal_internal::parse_2d(s, 6, &seconds)) {
    return false;
  }
  if (hours > 23 || minutes > 59 || seconds > 59)
    return false;
  *out_seconds = hours * 3600 + minutes * 60 + seconds;
  return true;
}

} // namespace sanitize

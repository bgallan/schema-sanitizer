// Declares string-to-scalar coercion helpers for inference/materialization.

#pragma once

#include <cstdint>
#include <string_view>

#include "sanitize/core/primitives.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

// Scalar kind bitmask used during schema inference.
// Keep this internal and stable within the project so multiple compilation
// units can share the same classification logic.
inline constexpr uint32_t K_BOOL = 1u << 0;
inline constexpr uint32_t K_TS = 1u << 1;
inline constexpr uint32_t K_DATE = 1u << 2;
inline constexpr uint32_t K_TIME = 1u << 3;
inline constexpr uint32_t K_INT = 1u << 4;
inline constexpr uint32_t K_FLOAT = 1u << 5;
inline constexpr uint32_t K_STR = 1u << 6;

// Returns whether one byte is ASCII whitespace.
inline bool is_ascii_scalar_whitespace(char value) noexcept {
  return value == ' ' || value == '\t' || value == '\n' || value == '\r' ||
         value == '\f' || value == '\v';
}

// Returns a borrowed view with surrounding ASCII whitespace removed.
inline std::string_view
trim_ascii_scalar_whitespace(std::string_view value) noexcept {
  while (!value.empty() && is_ascii_scalar_whitespace(value.front()))
    value.remove_prefix(1);
  while (!value.empty() && is_ascii_scalar_whitespace(value.back()))
    value.remove_suffix(1);
  return value;
}

// Tries strict parsing first, then retries once with surrounding whitespace
// removed. The original string remains untouched when neither attempt matches.
template <typename Parse>
inline bool parse_string_with_trim_retry(std::string_view value,
                                         Parse &&parse) noexcept {
  if (parse(value))
    return true;
  const std::string_view trimmed = trim_ascii_scalar_whitespace(value);
  if (trimmed.empty() ||
      (trimmed.data() == value.data() && trimmed.size() == value.size())) {
    return false;
  }
  return parse(trimmed);
}

// --- coercion helpers --------------------------------------------------------

inline bool coerce_bool_from_string(std::string_view sv,
                                    const PreparedOptions &opts,
                                    bool *out) noexcept {
  if (opts.true_hashes.empty() && opts.false_hashes.empty())
    return false;
  return parse_string_with_trim_retry(sv, [&](std::string_view candidate) {
    if (opts.is_true_token(candidate)) {
      if (out)
        *out = true;
      return true;
    }
    if (opts.is_false_token(candidate)) {
      if (out)
        *out = false;
      return true;
    }
    return false;
  });
}

// Coerces int64 from string.
inline bool coerce_int64_from_string(std::string_view sv,
                                     const PreparedOptions &opts,
                                     int64_t *out) noexcept {
  if (!opts.spec.parse_integers)
    return false;
  return parse_string_with_trim_retry(sv, [&](std::string_view candidate) {
    return parse_int64_strict(candidate, out);
  });
}

// Coerces float64 from string.
inline bool coerce_float64_from_string(std::string_view sv,
                                       const PreparedOptions &opts,
                                       double *out) noexcept {
  if (!opts.spec.parse_floats)
    return false;
  return parse_string_with_trim_retry(sv, [&](std::string_view candidate) {
    return parse_float64_strict(
        candidate, opts.spec.parse_float_decimal_separator.front(),
        opts.spec.parse_float_thousands_separator.front(), out);
  });
}

// Coerces timestamp ns from string.
inline bool coerce_timestamp_ns_from_string(std::string_view sv,
                                            const PreparedOptions &opts,
                                            int64_t *out_ns) noexcept {
  return parse_string_with_trim_retry(sv, [&](std::string_view candidate) {
    return opts.parse_timestamp_ns(candidate, out_ns);
  });
}

// Coerces date days from string.
inline bool coerce_date_days_from_string(std::string_view sv,
                                         const PreparedOptions &opts,
                                         int32_t *out_days) noexcept {
  return parse_string_with_trim_retry(sv, [&](std::string_view candidate) {
    return opts.parse_date_days(candidate, out_days);
  });
}

// Coerces time seconds from string.
inline bool coerce_time_seconds_from_string(std::string_view sv,
                                            const PreparedOptions &opts,
                                            int32_t *out_sec) noexcept {
  return parse_string_with_trim_retry(sv, [&](std::string_view candidate) {
    return opts.parse_time_seconds(candidate, out_sec);
  });
}

// --- inference helpers -------------------------------------------------------

inline uint32_t
infer_scalar_mask_from_string(std::string_view sv,
                              const PreparedOptions &opts) noexcept {
  if (!opts.true_hashes.empty() || !opts.false_hashes.empty()) {
    bool parsed = false;
    if (coerce_bool_from_string(sv, opts, &parsed))
      return K_BOOL;
  }

  int64_t timestamp = 0;
  if (coerce_timestamp_ns_from_string(sv, opts, &timestamp))
    return K_TS;
  int32_t date = 0;
  if (coerce_date_days_from_string(sv, opts, &date))
    return K_DATE;
  int32_t time = 0;
  if (coerce_time_seconds_from_string(sv, opts, &time))
    return K_TIME;

  int64_t integer = 0;
  if (coerce_int64_from_string(sv, opts, &integer))
    return K_INT;

  double floating = 0.0;
  if (coerce_float64_from_string(sv, opts, &floating))
    return K_FLOAT;

  return K_STR;
}

} // namespace sanitize::internal

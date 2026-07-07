// Validates user options and prepares derived runtime lookup state.

#include "sanitize/options/options.hh"

#include <cstdint>
#include <memory>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "internal/planning/options_temporal_simple.hh"
#include "sanitize/core/status.hh"
#include "sanitize/detail/hash.hh"

namespace sanitize {

namespace {

// Validates scalar options before derived state is prepared.
static sanitize::Status validate_option_values(const Options &opts) {
  const auto invalid_float_separator = [](const std::string &separator) {
    if (separator.size() != 1)
      return true;
    const auto ch = static_cast<unsigned char>(separator.front());
    return ch < 33 || ch > 126 || std::isalnum(ch) != 0 ||
           separator.front() == '+' || separator.front() == '-' ||
           separator.front() == 'e' || separator.front() == 'E';
  };

  if (opts.io_chunk_bytes <= 0) {
    return sanitize::Status::Invalid("io_chunk_bytes must be > 0");
  }

  if (opts.memory_limit_bytes < -1) {
    return sanitize::Status::Invalid("memory_limit_bytes must be >= -1");
  }

  if (opts.arrow_max_depth < 0) {
    return sanitize::Status::Invalid("arrow_max_depth must be >= 0");
  }

  if (opts.parquet_max_depth < 0) {
    return sanitize::Status::Invalid("parquet_max_depth must be >= 0");
  }

  if (invalid_float_separator(opts.parse_float_decimal_separator)) {
    return sanitize::Status::Invalid(
        "parse_float_decimal_separator must be one ASCII punctuation "
        "character");
  }
  if (invalid_float_separator(opts.parse_float_thousands_separator)) {
    return sanitize::Status::Invalid(
        "parse_float_thousands_separator must be one ASCII punctuation "
        "character");
  }
  if (opts.parse_float_decimal_separator ==
      opts.parse_float_thousands_separator) {
    return sanitize::Status::Invalid(
        "parse_float_decimal_separator and "
        "parse_float_thousands_separator must differ");
  }

  if (opts.timestamp_precision != "TIMESTAMP_MILLIS" &&
      opts.timestamp_precision != "TIMESTAMP_MICROS" &&
      opts.timestamp_precision != "TIMESTAMP_NANOS") {
    return sanitize::Status::Invalid(
        "timestamp_precision must be TIMESTAMP_MILLIS, TIMESTAMP_MICROS, or "
        "TIMESTAMP_NANOS");
  }

  if (opts.field_name_policy != "preserve" &&
      opts.field_name_policy != "lower_alpha" &&
      opts.field_name_policy != "lower_snake") {
    return sanitize::Status::Invalid(
        "field_name_policy must be preserve, lower_alpha, or lower_snake");
  }

  if (!opts.csv_delimiter.empty() && opts.csv_delimiter.size() != 1) {
    return sanitize::Status::Invalid(
        "csv_delimiter must be a 1-character string");
  }

  if (!opts.xml_row_tag.empty()) {
    for (const char ch : opts.xml_row_tag) {
      const auto c = static_cast<unsigned char>(ch);
      if (std::isspace(c) != 0 || ch == '<' || ch == '>' || ch == '/' ||
          ch == '=') {
        return sanitize::Status::Invalid(
            "xml_row_tag must be an XML element tag name");
      }
    }
  }

  return sanitize::Status::OK();
}

// Adds case-folded token hashes to a prepared option set.
static void add_token_hashes(const std::vector<std::string> &tokens,
                             std::unordered_set<uint64_t> *hashes) {
  hashes->reserve(tokens.size());
  for (const std::string &tok : tokens) {
    hashes->insert(detail::hash_key64_casefold(std::string_view(tok)));
  }
}

// Validates that true and false token sets are disjoint.
static sanitize::Status
validate_boolean_token_sets(const PreparedOptions &options) {
  for (const uint64_t h : options.true_hashes) {
    if (options.false_hashes.contains(h)) {
      return sanitize::Status::Invalid(
          "boolean tokens overlap between true_tokens and false_tokens");
    }
  }
  return sanitize::Status::OK();
}

// Prepares case-folded boolean-token hash sets.
static sanitize::Status prepare_boolean_token_hashes(const Options &opts,
                                                     PreparedOptions *out) {
  add_token_hashes(opts.true_tokens, &out->true_hashes);
  add_token_hashes(opts.false_tokens, &out->false_hashes);
  return validate_boolean_token_sets(*out);
}

// Compiles one temporal regex option group.
static sanitize::Status
compile_regexes(const std::vector<std::string> &patterns,
                std::vector<std::regex> *target, const char *label) {
  if (!target)
    return sanitize::Status::Invalid(
        "internal error: null temporal regex target");
  target->reserve(patterns.size());
  for (const std::string &p : patterns) {
    try {
      target->emplace_back(p, std::regex::ECMAScript);
    } catch (const std::regex_error &e) {
      return sanitize::Status::Invalid("invalid ", label, " regex '", p,
                                       "': ", e.what());
    }
  }
  return sanitize::Status::OK();
}

// Compiles all temporal regex option groups.
static sanitize::Status prepare_temporal_regexes(const Options &opts,
                                                 PreparedOptions *out) {
  SAN_RETURN_NOT_OK(compile_regexes(opts.timestamp_regexps,
                                    &out->compiled_timestamp_regexps,
                                    "timestamp_regexps"));
  SAN_RETURN_NOT_OK(compile_regexes(
      opts.date_regexps, &out->compiled_date_regexps, "date_regexps"));
  SAN_RETURN_NOT_OK(compile_regexes(
      opts.time_regexps, &out->compiled_time_regexps, "time_regexps"));
  for (const auto &pattern : opts.timestamp_regexps) {
    SimpleTemporalPattern simple;
    if (internal::detect_simple_timestamp_pattern(pattern, &simple))
      out->simple_timestamp_patterns.push_back(simple);
  }
  for (const auto &pattern : opts.date_regexps) {
    SimpleTemporalPattern simple;
    if (internal::detect_simple_date_pattern(pattern, &simple))
      out->simple_date_patterns.push_back(simple);
  }
  for (const auto &pattern : opts.time_regexps) {
    SimpleTemporalPattern simple;
    if (internal::detect_simple_time_pattern(pattern, &simple))
      out->simple_time_patterns.push_back(simple);
  }
  return sanitize::Status::OK();
}

} // namespace

bool PreparedOptions::is_true_token(std::string_view s) const {
  return true_hashes.contains(detail::hash_key64_casefold(s));
}

bool PreparedOptions::is_false_token(std::string_view s) const {
  return false_hashes.contains(detail::hash_key64_casefold(s));
}

sanitize::Result<PreparedOptionsPtr> prepare_options(const Options &opts) {
  auto out = std::make_shared<PreparedOptions>();
  out->spec = opts;

  SAN_RETURN_NOT_OK(validate_option_values(out->spec));
  SAN_RETURN_NOT_OK(prepare_boolean_token_hashes(opts, out.get()));
  SAN_RETURN_NOT_OK(prepare_temporal_regexes(opts, out.get()));

  return out;
}

} // namespace sanitize

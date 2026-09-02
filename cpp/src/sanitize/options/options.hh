// Defines public option enums, catalog-backed inputs, and immutable prepared
// state. Parsing, naming, evolution, threading, and error policies are
// validated once and exposed through allocation-conscious runtime lookups.

#pragma once

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace sanitize {

struct SimpleTemporalPattern {
  char date_sep1 = '\0';
  char date_sep2 = '\0';
  char datetime_sep = '\0';
  char time_sep1 = '\0';
  char time_sep2 = '\0';
  bool has_fraction = false;
  bool has_timezone = false;
  bool timezone_z = false;
};

enum class SchemaEvolutionMode : std::uint8_t {
  // Reject inferred schema if it is not compatible with the active contract.
  kStrict = 0,
  // Keep schema contract types as-is, only add newly observed fields.
  kAdditive = 2,
};

enum class FieldOrderPolicy : std::uint8_t {
  kAlphabetically = 1,
  kSchemaContractFirst = 2,
};

// Execution model for project-owned work.
enum class ThreadingMode : std::uint8_t {
  // Execute every project-owned stage inline on the calling thread.
  kSingle = 0,
  // Allow bounded parallel execution according to the derived policy.
  kMulti = 1,
};

// Row-level error handling.
enum class OnErrorPolicy : std::uint8_t {
  // Stop materialization and return an error.
  kStop = 0,
  // Skip the offending row entirely.
  kSkipRow = 1,
  // Emit the row, but write nulls for offending fields.
  kEmitNullRow = 2,
};

struct Options {
#define SCHEMA_SANITIZER_OPTION(type, name, default_expr, group, doc)          \
  type name = default_expr;
#define SCHEMA_SANITIZER_OPTION_DEFAULT(type, name, group, doc) type name;
#include "sanitize/options/options_catalog.def"
#undef SCHEMA_SANITIZER_OPTION_DEFAULT
#undef SCHEMA_SANITIZER_OPTION
};

struct PreparedOptions {
  Options spec;

  // Internal run metadata captured by the Python operation owner. This is not
  // part of the public option catalog or serialized SZOPT contract.
  std::string operation_detected_at;
  // Shared resident-byte ledger owned by the public Python operation. Native
  // pools cast this opaque handle back to internal::OperationMemoryLedger.
  std::shared_ptr<void> operation_memory_ledger;

  // Case-folded token hashes.
  std::unordered_set<uint64_t> true_hashes;
  std::unordered_set<uint64_t> false_hashes;

  std::vector<std::regex> compiled_timestamp_regexps;
  std::vector<std::regex> compiled_date_regexps;
  std::vector<std::regex> compiled_time_regexps;
  std::vector<SimpleTemporalPattern> simple_timestamp_patterns;
  std::vector<SimpleTemporalPattern> simple_date_patterns;
  std::vector<SimpleTemporalPattern> simple_time_patterns;

  /// Returns whether the value matches a configured true token.
  bool is_true_token(std::string_view s) const;
  /// Returns whether the value matches a configured false token.
  bool is_false_token(std::string_view s) const;
  /// Parses a timestamp string into Unix nanoseconds.
  bool parse_timestamp_ns(std::string_view s, int64_t *out_ns) const;
  /// Parses a date string into Arrow date32 days.
  bool parse_date_days(std::string_view s, int32_t *out_days) const;
  /// Parses a time string into seconds since midnight.
  bool parse_time_seconds(std::string_view s, int32_t *out_seconds) const;
  /// Returns whether the value matches any configured timestamp pattern.
  bool match_timestamp(std::string_view s) const;
  /// Returns whether the value matches any configured date pattern.
  bool match_date(std::string_view s) const;
  /// Returns whether the value matches any configured time pattern.
  bool match_time(std::string_view s) const;
};

using PreparedOptionsPtr = std::shared_ptr<const PreparedOptions>;

/// Validates and compiles options for the ingestion pipeline.
sanitize::Result<PreparedOptionsPtr> prepare_options(const Options &opts);

} // namespace sanitize

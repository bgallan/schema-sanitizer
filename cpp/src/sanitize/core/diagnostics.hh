// Defines row-level diagnostics and stable diagnostic codes.

#pragma once

#include <cstdint>
#include <string>

#include "sanitize/abi/cdata_types.hh"

namespace sanitize {

// Codes for sampled diagnostic events.
enum class DiagnosticCode : std::uint8_t {
  kUnknown = 0,
  // Row-level
  kRowSkipped = 2,
  // Field-level
  kTypeMismatch = 10,
  kCoercionFailure = 13,
  kRequiredMissing = 14,
};

// Stable string codes for JSON diagnostics output.
inline const char *DiagnosticCodeToString(DiagnosticCode c) {
  switch (c) {
  case DiagnosticCode::kRowSkipped:
    return "row_skipped";
  case DiagnosticCode::kTypeMismatch:
    return "type_mismatch";
  case DiagnosticCode::kCoercionFailure:
    return "coercion_failure";
  case DiagnosticCode::kRequiredMissing:
    return "required_missing";
  case DiagnosticCode::kUnknown:
  default:
    return "unknown";
  }
}

struct IngestDiagnostics {
  // Inference pass
  int64_t inferred_rows = 0;
  int64_t inferred_bytes = 0;
  int64_t arrow_schema_depth = 0;
  int64_t parquet_schema_depth = 0;

  // Materialization pass
  int64_t materialized_rows = 0;
  int64_t batches = 0;

  // Feature counters
  int64_t flattened_fields = 0;
  int64_t scalar_wrappings = 0;
  int64_t direct_arrow_input = 0;

  // Error handling
  int64_t skipped_rows = 0;

  // Canonical JSON payload for diagnostics.
  [[nodiscard]] std::string to_json() const;
};

} // namespace sanitize

// Defines bounded ingestion diagnostics and their stable external codes.
// Reader, materialization, memory, and cancellation counters can be merged and
// serialized without retaining input payloads or replacing primary failures.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "sanitize/abi/cdata_types.hh"

namespace sanitize {

namespace internal {
class MemoryPool;
}

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

/// Returns the stable JSON code for a diagnostic event.
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

struct ReaderResourceDiagnostics {
  int64_t parser_max_depth = 0;
  int64_t decoded_bytes = 0;
  int64_t records = 0;
  int64_t nodes = 0;
  int64_t compressed_bytes = 0;
  int64_t decompressed_bytes = 0;

  void merge(const ReaderResourceDiagnostics &other) noexcept;
};

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

  // Reader hardening/resource diagnostics.
  mutable int64_t current_charged_memory_bytes = 0;
  mutable int64_t peak_charged_memory_bytes = 0;
  mutable int64_t operation_memory_limit_bytes = -1;
  ReaderResourceDiagnostics reader;
  int64_t cancellations = 0;
  std::string cancellation_reason;

  /// Binds and snapshots the exact operation-scoped tracked pool. The weak
  /// reference avoids extending operation leases after stream ownership ends.
  void bind_operation_memory_pool(std::shared_ptr<void> pool) noexcept;
  void capture_operation_memory() const noexcept;
  void merge_reader(const ReaderResourceDiagnostics &delta) noexcept;
  void merge(const IngestDiagnostics &other) noexcept;
  void record_cancellation(std::string_view reason) noexcept;

  /// Serializes the diagnostic counters and cancellation state as canonical
  /// JSON.
  [[nodiscard]] std::string to_json() const;

private:
  std::weak_ptr<internal::MemoryPool> operation_memory_pool_;
};

} // namespace sanitize

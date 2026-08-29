// Collects bounded ingestion diagnostics and serializes them with stable names.
// The implementation merges reader, memory, and cancellation state without
// allowing diagnostic failures or counter overflow to replace operation
// results.

#include "sanitize/core/diagnostics.hh"

#include <algorithm>
#include <limits>
#include <string>

#include "internal/json_encoding/token_writer.hh"
#include "internal/memory/memory_pool.hh"

namespace sanitize {
namespace {

/// Adds a nonnegative counter delta without overflowing the signed total.
[[nodiscard]] int64_t saturating_add(int64_t left, int64_t right) noexcept {
  if (right <= 0) {
    return left;
  }
  const auto maximum = std::numeric_limits<int64_t>::max();
  return left > maximum - right ? maximum : left + right;
}

} // namespace

/// Merges reader counters while retaining maxima and saturating totals.
void ReaderResourceDiagnostics::merge(
    const ReaderResourceDiagnostics &other) noexcept {
  parser_max_depth = std::max(parser_max_depth, other.parser_max_depth);
  decoded_bytes = saturating_add(decoded_bytes, other.decoded_bytes);
  records = saturating_add(records, other.records);
  nodes = saturating_add(nodes, other.nodes);
  compressed_bytes = saturating_add(compressed_bytes, other.compressed_bytes);
  decompressed_bytes =
      saturating_add(decompressed_bytes, other.decompressed_bytes);
}

void IngestDiagnostics::bind_operation_memory_pool(
    std::shared_ptr<void> pool) noexcept {
  operation_memory_pool_ =
      std::static_pointer_cast<internal::MemoryPool>(std::move(pool));
  capture_operation_memory();
}

/// Refreshes live and peak memory diagnostics from the bound operation pool.
void IngestDiagnostics::capture_operation_memory() const noexcept {
  const auto pool = operation_memory_pool_.lock();
  if (!pool) {
    // Once the operation pool is destroyed no charged allocation can remain.
    // Preserve the historical peak and limit, but do not expose a stale live
    // byte sample from the final pre-destruction snapshot.
    if (operation_memory_limit_bytes >= 0) {
      current_charged_memory_bytes = 0;
    }
    return;
  }
  current_charged_memory_bytes = std::max<int64_t>(0, pool->bytes_allocated());
  peak_charged_memory_bytes = std::max(
      peak_charged_memory_bytes, std::max<int64_t>(0, pool->max_memory()));
  operation_memory_limit_bytes = pool->limit_bytes();
}

/// Applies one reader-resource delta to the aggregate diagnostics.
void IngestDiagnostics::merge_reader(
    const ReaderResourceDiagnostics &delta) noexcept {
  reader.merge(delta);
}

/// Combines an operation snapshot into this aggregate without throwing.
void IngestDiagnostics::merge(const IngestDiagnostics &other) noexcept {
  inferred_rows = saturating_add(inferred_rows, other.inferred_rows);
  inferred_bytes = saturating_add(inferred_bytes, other.inferred_bytes);
  arrow_schema_depth = std::max(arrow_schema_depth, other.arrow_schema_depth);
  parquet_schema_depth =
      std::max(parquet_schema_depth, other.parquet_schema_depth);
  materialized_rows =
      saturating_add(materialized_rows, other.materialized_rows);
  batches = saturating_add(batches, other.batches);
  flattened_fields = saturating_add(flattened_fields, other.flattened_fields);
  scalar_wrappings = saturating_add(scalar_wrappings, other.scalar_wrappings);
  direct_arrow_input =
      saturating_add(direct_arrow_input, other.direct_arrow_input);
  skipped_rows = saturating_add(skipped_rows, other.skipped_rows);
  current_charged_memory_bytes = std::max(current_charged_memory_bytes,
                                          other.current_charged_memory_bytes);
  peak_charged_memory_bytes =
      std::max(peak_charged_memory_bytes, other.peak_charged_memory_bytes);
  operation_memory_limit_bytes = std::max(operation_memory_limit_bytes,
                                          other.operation_memory_limit_bytes);
  reader.merge(other.reader);
  cancellations = saturating_add(cancellations, other.cancellations);
  if (cancellation_reason.empty() && !other.cancellation_reason.empty()) {
    try {
      cancellation_reason = other.cancellation_reason;
    } catch (...) {
    }
  }
}

/// Records a cancellation count and preserves the first available reason.
void IngestDiagnostics::record_cancellation(std::string_view reason) noexcept {
  cancellations = saturating_add(cancellations, 1);
  if (cancellation_reason.empty()) {
    try {
      cancellation_reason.assign(reason);
    } catch (...) {
      // Diagnostics must never replace the primary operation result.
    }
  }
}

std::string IngestDiagnostics::to_json() const {
  capture_operation_memory();
  std::string out;
  out.reserve(1024);

  out.push_back('{');
  bool first = true;

  internal::json_encoding::append_int_field(out, first,
                                            "diagnostics_schema_version", 1);

  // Core counters
  internal::json_encoding::append_int_field(out, first, "inferred_rows",
                                            inferred_rows);
  internal::json_encoding::append_int_field(out, first, "inferred_bytes",
                                            inferred_bytes);
  internal::json_encoding::append_int_field(out, first, "arrow_schema_depth",
                                            arrow_schema_depth);
  internal::json_encoding::append_int_field(out, first, "parquet_schema_depth",
                                            parquet_schema_depth);
  internal::json_encoding::append_int_field(out, first, "materialized_rows",
                                            materialized_rows);
  internal::json_encoding::append_int_field(out, first, "batches", batches);

  // Feature counters
  internal::json_encoding::append_int_field(out, first, "flattened_fields",
                                            flattened_fields);
  internal::json_encoding::append_int_field(out, first, "scalar_wrappings",
                                            scalar_wrappings);
  internal::json_encoding::append_int_field(out, first, "direct_arrow_input",
                                            direct_arrow_input);

  // Error handling
  internal::json_encoding::append_int_field(out, first, "skipped_rows",
                                            skipped_rows);
  internal::json_encoding::append_int_field(out, first, "cancellations",
                                            cancellations);
  internal::json_encoding::append_string_field(
      out, first, "cancellation_reason", cancellation_reason);

  // Resource and parser diagnostics. These values contain only counters and
  // stable reason codes; input payloads and field names are never included.
  internal::json_encoding::append_int_field(
      out, first, "current_charged_memory_bytes", current_charged_memory_bytes);
  internal::json_encoding::append_int_field(
      out, first, "peak_charged_memory_bytes", peak_charged_memory_bytes);
  internal::json_encoding::append_int_field(
      out, first, "operation_memory_limit_bytes", operation_memory_limit_bytes);
  internal::json_encoding::append_int_field(out, first, "parser_max_depth",
                                            reader.parser_max_depth);
  internal::json_encoding::append_int_field(out, first, "decoded_bytes",
                                            reader.decoded_bytes);
  internal::json_encoding::append_int_field(out, first, "reader_records",
                                            reader.records);
  internal::json_encoding::append_int_field(out, first, "reader_nodes",
                                            reader.nodes);
  internal::json_encoding::append_int_field(out, first, "compressed_bytes",
                                            reader.compressed_bytes);
  internal::json_encoding::append_int_field(out, first, "decompressed_bytes",
                                            reader.decompressed_bytes);
  const double decompression_ratio =
      reader.compressed_bytes > 0
          ? static_cast<double>(reader.decompressed_bytes) /
                static_cast<double>(reader.compressed_bytes)
          : 0.0;
  internal::json_encoding::append_double_field(
      out, first, "decompression_ratio", decompression_ratio);

  out.push_back('}');
  return out;
}

} // namespace sanitize

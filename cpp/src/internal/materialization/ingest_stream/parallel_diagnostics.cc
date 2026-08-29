// Implements diagnostics for ordered columnar packet handoff.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#include "internal/materialization/ingest_stream/parallel_diagnostics.hh"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>

namespace sanitize::internal {
namespace {

/// Adds diagnostic or sizing counters and clamps overflow to the destination
/// maximum.
[[nodiscard]] std::int64_t saturating_add(std::int64_t left,
                                          std::int64_t right) noexcept {
  const auto max = std::numeric_limits<std::int64_t>::max();
  if (right <= 0) {
    return left;
  }
  return right > max - left ? max : left + right;
}

/// Models vector-style retained capacity for a row count and fixed bytes per
/// row.
[[nodiscard]] std::int64_t
projected_capacity_bytes(std::int64_t rows,
                         std::int64_t bytes_per_row) noexcept {
  if (rows <= 0 || bytes_per_row <= 0) {
    return 0;
  }
  const auto unsigned_rows = static_cast<std::uint64_t>(rows);
  const auto capacity = std::bit_ceil(unsigned_rows);
  const auto max =
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
  const auto unit = static_cast<std::uint64_t>(bytes_per_row);
  if (capacity == 0 || unit > max / capacity) {
    return std::numeric_limits<std::int64_t>::max();
  }
  return static_cast<std::int64_t>(capacity * unit);
}

} // namespace

/// Merges one completed packet's ingest counters into the operation totals.
void ParallelBatchDiagnostics::merge(const IngestDiagnostics &delta) noexcept {
  if (target_) {
    target_->merge(delta);
  }
}

/// Commits accumulated direct-path diagnostics before parallel packet results
/// are merged.
void ParallelBatchDiagnostics::flush_direct() noexcept {
  if (target_ && direct_rows_ > 0) {
    target_->batches += 1;
  }
  direct_rows_ = 0;
  direct_bytes_ = 0;
  direct_max_rows_ = 0;
  direct_max_bytes_ = 0;
  direct_capacity_row_bytes_ = 0;
  direct_capacity_model_ = true;
}

/// Accounts for one serial direct-path result and detects logical batch
/// boundaries.
void ParallelBatchDiagnostics::record_direct(const ArrowArray *out,
                                             std::int64_t max_rows,
                                             std::int64_t max_bytes,
                                             std::int64_t bytes) noexcept {
  if (!target_ || !out || out->length <= 0) {
    return;
  }
  target_->materialized_rows += out->length;
  if (direct_rows_ == 0) {
    direct_max_rows_ = max_rows;
    direct_max_bytes_ = max_bytes;
  }
  if (bytes <= 0 || bytes % out->length != 0) {
    direct_capacity_model_ = false;
  } else {
    const auto packet_row_bytes = bytes / out->length;
    if (packet_row_bytes <= 0) {
      direct_capacity_model_ = false;
    } else if (direct_capacity_row_bytes_ == 0) {
      direct_capacity_row_bytes_ = packet_row_bytes;
    } else if (direct_capacity_row_bytes_ != packet_row_bytes) {
      direct_capacity_model_ = false;
    }
  }
  direct_rows_ = saturating_add(direct_rows_, out->length);
  direct_bytes_ =
      saturating_add(direct_bytes_, std::max<std::int64_t>(0, bytes));
  const bool row_limit_reached =
      direct_max_rows_ > 0 && direct_rows_ >= direct_max_rows_;
  const auto modeled_bytes =
      direct_capacity_model_
          ? projected_capacity_bytes(direct_rows_, direct_capacity_row_bytes_)
          : direct_bytes_;
  const bool byte_limit_reached =
      direct_max_bytes_ > 0 && modeled_bytes >= direct_max_bytes_;
  if (row_limit_reached || byte_limit_reached) {
    flush_direct();
  }
}

/// Records skipped rows in source order using saturating diagnostic counters.
void ParallelBatchDiagnostics::record_skipped_rows(std::int64_t rows) noexcept {
  if (target_ && rows > 0) {
    target_->skipped_rows = saturating_add(target_->skipped_rows, rows);
  }
}

/// Accounts for one completed parallel Arrow batch.
void ParallelBatchDiagnostics::record_finished(const ArrowArray *out) noexcept {
  if (!target_ || !out) {
    return;
  }
  target_->batches += 1;
  target_->materialized_rows += out->length;
}

/// Merges source-reader resource counters into the operation diagnostics.
void ParallelBatchDiagnostics::merge_reader(
    const ReaderResourceDiagnostics &delta) const noexcept {
  if (target_) {
    target_->merge_reader(delta);
  }
}

/// Records cancellation in source order using saturating diagnostic counters.
void ParallelBatchDiagnostics::record_cancellation(
    std::string_view reason) const noexcept {
  if (target_) {
    target_->record_cancellation(reason);
  }
}

/// Snapshots operation-level memory telemetry into the exported batch
/// diagnostics.
void ParallelBatchDiagnostics::capture_operation_memory() const noexcept {
  if (target_) {
    target_->capture_operation_memory();
  }
}

} // namespace sanitize::internal

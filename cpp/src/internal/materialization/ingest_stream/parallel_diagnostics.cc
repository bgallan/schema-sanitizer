// Implements diagnostics for ordered columnar packet handoff.

#include "internal/materialization/ingest_stream/parallel_diagnostics.hh"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>

namespace sanitize::internal {
namespace {

[[nodiscard]] std::int64_t saturating_add(std::int64_t left,
                                          std::int64_t right) noexcept {
  const auto max = std::numeric_limits<std::int64_t>::max();
  if (right <= 0) {
    return left;
  }
  return right > max - left ? max : left + right;
}

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

void ParallelBatchDiagnostics::merge(const IngestDiagnostics &delta) noexcept {
  if (!target_) {
    return;
  }
  target_->inferred_rows += delta.inferred_rows;
  target_->inferred_bytes += delta.inferred_bytes;
  target_->arrow_schema_depth += delta.arrow_schema_depth;
  target_->parquet_schema_depth += delta.parquet_schema_depth;
  target_->materialized_rows += delta.materialized_rows;
  target_->batches += delta.batches;
  target_->flattened_fields += delta.flattened_fields;
  target_->scalar_wrappings += delta.scalar_wrappings;
  target_->direct_arrow_input += delta.direct_arrow_input;
  target_->skipped_rows += delta.skipped_rows;
}

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

void ParallelBatchDiagnostics::record_finished(const ArrowArray *out) noexcept {
  if (!target_ || !out) {
    return;
  }
  target_->batches += 1;
  target_->materialized_rows += out->length;
}

} // namespace sanitize::internal

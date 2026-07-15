// Implements batching and row materialization for the ingest stream.

#include "internal/materialization/ingest_stream/source_internal.hh"

#include "internal/materialization/batch_sizing.hh"
#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <utility>

namespace sanitize::internal {

IngestStreamSource::BatchLimits IngestStreamSource::batch_limits() const {
  const int64_t memory_limit = opts_ ? opts_->spec.memory_limit_bytes : -1;
  const int64_t max_rows =
      rows_per_batch_from_memory_limit(memory_limit, observed_bytes_per_row_);
  const int64_t target_bytes =
      batch_target_bytes_from_memory_limit(memory_limit);
  return BatchLimits{
      .max_rows = max_rows, .max_bytes = target_bytes, .capacity = max_rows};
}

bool IngestStreamSource::appender_is_full(const BatchLimits &limits) const {
  const int64_t current_length = batch_appender_length(app_.get());
  if (limits.max_rows > 0 && current_length >= limits.max_rows) {
    return true;
  }
  return limits.max_bytes > 0 && current_length > 0 &&
         batch_appender_bytes(app_.get()) >= limits.max_bytes;
}

bool IngestStreamSource::byte_limit_reached(const BatchLimits &limits) const {
  return limits.max_bytes > 0 && batch_appender_length(app_.get()) > 0 &&
         batch_appender_bytes(app_.get()) >= limits.max_bytes;
}

sanitize::Result<bool>
IngestStreamSource::ensure_current_row(const BatchLimits &limits) {
  if (cur_i_ < cur_.rows.size()) {
    return true;
  }

  SAN_RETURN_NOT_OK(check_interrupt());
  auto next = frontend_.next_batch(limits.capacity);
  if (!next.ok()) {
    return next.status();
  }
  cur_ = std::move(next).ValueOrDie();
  cur_i_ = 0;
  if (cur_.rows.empty()) {
    eof_ = true;
    return false;
  }
  return true;
}

sanitize::Result<AppendRowResult>
IngestStreamSource::append_current_row(const RowRef &row) {
  const bool raw_only =
      (row.flags & std::to_underlying(RowFlags::kRawOnly)) != 0;
  if (raw_only) {
    if (!direct_) {
      return sanitize::Status::Invalid(
          "raw-only row encountered but frontend has no direct materializer");
    }
    return direct_->AppendRaw(app_.get(), row, *opts_, diagnostics_.get());
  }
  return append_row(app_.get(), row, *opts_, diagnostics_.get());
}

sanitize::Status IngestStreamSource::check_interrupt() const {
  if (!owned_ctx_keepalive_) {
    return sanitize::Status::OK();
  }
  return owned_ctx_keepalive_->CheckInterrupt();
}

sanitize::Status IngestStreamSource::fill_appender(const BatchLimits &limits) {
  std::size_t interrupt_countdown = 0;
  while (!appender_is_full(limits)) {
    if ((interrupt_countdown++ & std::size_t{1023}) == 0) {
      SAN_RETURN_NOT_OK(check_interrupt());
    }

    SAN_ASSIGN_OR_RAISE(bool has_row, ensure_current_row(limits));
    if (!has_row) {
      break;
    }

    const RowRef &row = cur_.rows[cur_i_++];
    const int64_t rows_before = batch_appender_length(app_.get());
    const int64_t skipped_before =
        diagnostics_ ? diagnostics_->skipped_rows : 0;
    auto result = append_current_row(row);
    if (!result.ok()) {
      return result.status();
    }
    (void)result.ValueOrDie();
    if (diagnostics_ &&
        opts_->spec.on_error == sanitize::OnErrorPolicy::kSkipRow &&
        batch_appender_length(app_.get()) == rows_before &&
        diagnostics_->skipped_rows == skipped_before) {
      diagnostics_->skipped_rows += 1;
    }
    row_index_++;

    if (byte_limit_reached(limits)) {
      break;
    }
  }
  return sanitize::Status::OK();
}

void IngestStreamSource::record_finished_batch(const ArrowArray *out) {
  if (diagnostics_) {
    diagnostics_->batches += 1;
    diagnostics_->materialized_rows += out->length;
  }
}

} // namespace sanitize::internal

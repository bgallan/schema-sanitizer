// Tracks diagnostics for ordered columnar packet handoff.

#pragma once

#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/diagnostics.hh"

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

namespace sanitize::internal {

class ParallelBatchDiagnostics final {
public:
  explicit ParallelBatchDiagnostics(
      std::shared_ptr<IngestDiagnostics> target) noexcept
      : target_(std::move(target)) {}

  void merge(const IngestDiagnostics &delta) noexcept;
  void flush_direct() noexcept;
  void record_direct(const ArrowArray *out, std::int64_t max_rows,
                     std::int64_t max_bytes, std::int64_t bytes) noexcept;
  void record_finished(const ArrowArray *out) noexcept;
  void record_skipped_rows(std::int64_t rows) noexcept;
  void merge_reader(const ReaderResourceDiagnostics &delta) const noexcept;
  void record_cancellation(std::string_view reason) const noexcept;
  void capture_operation_memory() const noexcept;

private:
  std::shared_ptr<IngestDiagnostics> target_;
  std::int64_t direct_rows_ = 0;
  std::int64_t direct_bytes_ = 0;
  std::int64_t direct_max_rows_ = 0;
  std::int64_t direct_max_bytes_ = 0;
  std::int64_t direct_capacity_row_bytes_ = 0;
  bool direct_capacity_model_ = true;
};

} // namespace sanitize::internal

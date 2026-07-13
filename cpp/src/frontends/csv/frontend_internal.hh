// Declares CSV frontend state shared by lifecycle and batching units.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "frontends/builtin_frontends.hh"
#include "frontends/csv/column_projection.hh"
#include "internal/parsing/row_scanner.hh"
#include "internal/parsing/streaming/csv/scanner.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal::csv_frontend_detail {

struct CsvBatchStorage;

class CsvFrontend final {
public:
  CsvFrontend(ChunkSourcePtr src, const Options &options);

  void set_plan(const CompiledPlan *plan) noexcept;
  void reset() noexcept;
  sanitize::Result<RowBatch> next_batch(int64_t capacity);

private:
  void append_record(CsvBatchStorage *storage, const TextSlice &record);
  sanitize::Result<bool> consume_header_record(const TextSlice &record);
  sanitize::Status process_header_record(const TextSlice &record);
  static char resolved_delimiter(const Options &options) noexcept;

  ChunkSourcePtr source_;
  int64_t chunk_bytes_ = int64_t{1} << 20;
  sanitize::Status reset_status_ = sanitize::Status::OK();
  char delimiter_ = ',';
  CsvColumnProjection projection_;
  std::size_t last_header_source_index_ = 0;
  bool last_header_source_ready_ = false;
  std::unique_ptr<CsvStreamingScanner> scanner_;
};

} // namespace sanitize::internal::csv_frontend_detail

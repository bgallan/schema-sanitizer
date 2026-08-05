// Declares CSV frontend state shared by lifecycle and batching units.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "frontends/builtin_frontends.hh"
#include "frontends/csv/column_projection.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/row_scanner.hh"
#include "internal/parsing/streaming/csv/scanner.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal::csv_frontend_detail {

struct CsvBatchStorage;

class CsvFrontend final {
public:
  CsvFrontend(ChunkSourcePtr src, const Options &options,
              CsvSourceProjectionSetPtr source_projections = nullptr);

  void set_plan(const CompiledPlan *plan) noexcept;
  void set_memory_pool(std::shared_ptr<void> pool) noexcept;
  void set_task_arena(std::shared_ptr<OperationTaskArena> task_arena) noexcept;
  void reset() noexcept;
  sanitize::Result<RowBatch> next_batch(int64_t capacity);

private:
  sanitize::Status append_record(CsvBatchStorage *storage,
                                 const TextSlice &record);
  sanitize::Status append_records_parallel(CsvBatchStorage *storage,
                                           std::span<const TextSlice> records);
  sanitize::Result<bool> consume_header_record(const TextSlice &record);
  sanitize::Status process_header_record(const TextSlice &record);
  static char resolved_delimiter(const Options &options) noexcept;
  static char resolved_escape_char(const Options &options) noexcept;

  ChunkSourcePtr source_;
  int64_t chunk_bytes_ = int64_t{1} << 20;
  std::size_t arena_block_bytes_ = std::size_t{1} << 20;
  std::size_t max_record_bytes_ = kMaxCsvRecordBytes;
  std::size_t max_field_bytes_ = kMaxCsvFieldBytes;
  std::size_t max_record_segments_ = kMaxCsvRecordSegments;
  sanitize::Status reset_status_ = sanitize::Status::OK();
  char delimiter_ = ',';
  char escape_char_ = '\0';
  CsvColumnProjection projection_;
  std::size_t last_header_source_index_ = 0;
  bool last_header_source_ready_ = false;
  std::shared_ptr<void> memory_pool_;
  std::unique_ptr<CsvStreamingScanner> scanner_;
  std::shared_ptr<OperationTaskArena> task_arena_;
};

} // namespace sanitize::internal::csv_frontend_detail

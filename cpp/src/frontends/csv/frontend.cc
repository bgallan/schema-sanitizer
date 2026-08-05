// Owns the CSV frontend lifecycle, batching, and vtable wiring.

#include "frontends/builtin_frontends.hh"
#include "frontends/csv/frontend_internal.hh"

#include "internal/runtime/thread_compat.hh"
#include <algorithm>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <new>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/memory/arena.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor.hh"

namespace sanitize::internal::csv_frontend_detail {

struct CsvParseRange {
  std::size_t begin = 0;
  std::size_t end = 0;
};

struct CsvParsedRow {
  std::size_t cell_begin = 0;
  std::size_t cell_count = 0;
};

struct CsvParsedChunk {
  CsvParsedChunk(std::shared_ptr<void> pool, std::size_t block_bytes,
                 std::size_t row_count, std::size_t cell_hint)
      : pool_keepalive(std::move(pool)), pmr_pool(pool_keepalive),
        arena(pool_keepalive.get(), block_bytes), cells(&pmr_pool),
        rows(&pmr_pool) {
    rows.reserve(row_count);
    if (cell_hint != 0 && row_count <= kMaxCsvCellsPerRecord / cell_hint) {
      cells.reserve(row_count * cell_hint);
    }
  }

  std::shared_ptr<void> pool_keepalive;
  PoolResource pmr_pool;
  BumpArena arena;
  std::pmr::vector<std::string_view> cells;
  std::pmr::vector<CsvParsedRow> rows;
};

struct CsvBatchStorage {
  CsvBatchStorage(std::shared_ptr<void> pool, std::size_t arena_block_bytes)
      : pool_keepalive(std::move(pool)), pmr_pool(pool_keepalive),
        source_arena(pool_keepalive.get(), arena_block_bytes),
        parse_arena(pool_keepalive.get(), arena_block_bytes), batch(&pmr_pool),
        cells(&pmr_pool), parsed_chunks(&pmr_pool), keepalive(&pmr_pool) {}

  std::shared_ptr<void> pool_keepalive;
  PoolResource pmr_pool;
  BumpArena source_arena;
  BumpArena parse_arena;
  FlatRowBatch batch;
  std::pmr::vector<std::string_view> cells;
  std::pmr::vector<std::shared_ptr<CsvParsedChunk>> parsed_chunks;
  std::pmr::vector<std::shared_ptr<const void>> keepalive;
  const void *last_data_owner_ptr = nullptr;
  const void *last_source_name_owner_ptr = nullptr;

  void keep_data_owner(const std::shared_ptr<const void> &owner) {
    if (!owner || owner.get() == last_data_owner_ptr) {
      return;
    }
    last_data_owner_ptr = owner.get();
    keepalive.push_back(owner);
  }

  void keep_source_name(const std::shared_ptr<const std::string> &owner) {
    if (!owner || owner.get() == last_source_name_owner_ptr) {
      return;
    }
    last_source_name_owner_ptr = owner.get();
    keepalive.push_back(std::static_pointer_cast<const void>(owner));
  }
};

CsvFrontend::CsvFrontend(ChunkSourcePtr src, const Options &options,
                         CsvSourceProjectionSetPtr source_projections)
    : source_(std::move(src)), delimiter_(resolved_delimiter(options)),
      escape_char_(resolved_escape_char(options)),
      projection_(options, delimiter_, std::move(source_projections)) {
  const auto budget =
      internal::memory_budget_from_limit(options.memory_limit_bytes);
  chunk_bytes_ = budget.io_chunk_bytes;
  arena_block_bytes_ = static_cast<std::size_t>(chunk_bytes_);
  const auto total =
      static_cast<std::size_t>(std::max<std::int64_t>(1, budget.total_bytes));
  max_record_bytes_ = std::min<std::size_t>(kMaxCsvRecordBytes, total);
  max_field_bytes_ = std::min<std::size_t>(
      kMaxCsvFieldBytes, std::max<std::size_t>(1, max_record_bytes_ / 2U));
  const auto chunk = std::max<std::size_t>(
      1, static_cast<std::size_t>(std::max<std::int64_t>(1, chunk_bytes_)));
  max_record_segments_ = std::min<std::size_t>(kMaxCsvRecordSegments,
                                               max_record_bytes_ / chunk + 2U);
  scanner_ = std::make_unique<CsvStreamingScanner>(
      source_, chunk_bytes_, max_record_bytes_, max_record_segments_, nullptr,
      escape_char_);
  reset_status_ = scanner_->Reset();
}

void CsvFrontend::set_plan(const CompiledPlan *plan) noexcept {
  projection_.set_plan(plan);
}

void CsvFrontend::set_memory_pool(std::shared_ptr<void> pool) noexcept {
  memory_pool_ = std::move(pool);
  try {
    scanner_ = std::make_unique<CsvStreamingScanner>(
        source_, chunk_bytes_, max_record_bytes_, max_record_segments_,
        memory_pool_ ? memory_pool_.get() : nullptr, escape_char_);
    reset_status_ = scanner_->Reset();
  } catch (const std::bad_alloc &) {
    scanner_.reset();
    reset_status_ = sanitize::Status::OutOfMemory(
        "CSV scanner: operation-budget metadata allocation failed");
  }
}

void CsvFrontend::set_task_arena(
    std::shared_ptr<OperationTaskArena> task_arena) noexcept {
  task_arena_ = std::move(task_arena);
}

void CsvFrontend::reset() noexcept {
  if (scanner_) {
    reset_status_ = scanner_->Reset();
  } else if (source_) {
    reset_status_ = source_->Reset();
  }
  projection_.reset_header();
  last_header_source_index_ = 0;
  last_header_source_ready_ = false;
}

sanitize::Result<RowBatch> CsvFrontend::next_batch(int64_t capacity) {
  RowBatch out;
  if (capacity <= 0) {
    return out;
  }
  if (!reset_status_.ok()) {
    return reset_status_;
  }
  if (!scanner_) {
    return sanitize::Status::Invalid("CSV frontend: scanner is null");
  }

  auto storage =
      std::make_shared<CsvBatchStorage>(memory_pool_, arena_block_bytes_);
  storage->cells.reserve(projection_.column_count_hint());
  std::pmr::vector<TextSlice> records(&storage->pmr_pool);
  records.reserve(
      static_cast<std::size_t>(std::min<int64_t>(capacity, int64_t{4096})));
  while (static_cast<int64_t>(records.size()) < capacity) {
    SAN_ASSIGN_OR_RAISE(TextSlice record,
                        scanner_->next_record(&storage->source_arena));
    if (record.view.empty() && scanner_->done()) {
      break;
    }
    if (record.view.empty()) {
      continue;
    }
    SAN_ASSIGN_OR_RAISE(const bool is_header, consume_header_record(record));
    if (is_header) {
      continue;
    }
    storage->keep_data_owner(record.owner);
    storage->keep_source_name(record.source_file_owner);
    out.reader_diagnostics.records += 1;
    out.reader_diagnostics.decoded_bytes +=
        static_cast<std::int64_t>(record.view.size());
    records.push_back(std::move(record));
  }

  // The caller-provided capacity is a safety ceiling, not a prediction of the
  // current source size. Reserve row metadata only after framing this batch so
  // small files do not pay for unused operation-wide capacity.
  storage->batch.reset(static_cast<std::int64_t>(records.size()));

  const auto arena_workers =
      task_arena_ ? task_arena_->worker_count() : std::size_t{1};
  const bool parallel_parse =
      !projection_.can_use_raw_only() && task_arena_ &&
      !task_arena_->inline_mode() && arena_workers > 1 &&
      records.size() >= std::max<std::size_t>(8, arena_workers * 2);
  if (parallel_parse) {
    SAN_RETURN_NOT_OK(append_records_parallel(storage.get(), records));
  } else {
    for (const auto &record : records) {
      SAN_RETURN_NOT_OK(append_record(storage.get(), record));
    }
  }

  storage->batch.export_rows(&out.rows);
  out.owner = std::move(storage);
  return out;
}

sanitize::Status CsvFrontend::append_record(CsvBatchStorage *storage,
                                            const TextSlice &record) {
  if (projection_.can_use_raw_only()) {
    SAN_RETURN_NOT_OK(validate_csv_utf8(record.view, record.base_offset));
    storage->batch.start_row(record.view, record.base_offset,
                             std::to_underlying(RowFlags::kRawOnly),
                             projection_.direct_context(), record.source_file);
    storage->batch.end_row();
    return sanitize::Status::OK();
  }

  storage->batch.start_row(record.view, record.base_offset, 0, nullptr,
                           record.source_file);
  SAN_RETURN_NOT_OK(parse_csv_cells(
      record.view, delimiter_, &storage->cells, &storage->parse_arena,
      record.base_offset, max_field_bytes_, max_record_bytes_, escape_char_));
  SAN_RETURN_NOT_OK(projection_.append_parsed_cells(
      &storage->batch, storage->cells, record.source_index,
      record.has_source_index));
  storage->batch.end_row();
  return sanitize::Status::OK();
}

sanitize::Status
CsvFrontend::append_records_parallel(CsvBatchStorage *storage,
                                     std::span<const TextSlice> records) {
  using Executor =
      OrderedExecutor<CsvParseRange, std::shared_ptr<CsvParsedChunk>>;
  const auto worker_count = std::min<std::size_t>(
      {task_arena_->worker_count(), records.size(), std::size_t{16}});
  const auto chunk_count =
      std::min<std::size_t>(records.size(), worker_count * 2U);
  std::pmr::vector<CsvParseRange> ranges(&storage->pmr_pool);
  ranges.reserve(chunk_count);
  for (std::size_t chunk = 0; chunk < chunk_count; ++chunk) {
    const auto begin = records.size() * chunk / chunk_count;
    const auto end = records.size() * (chunk + 1U) / chunk_count;
    ranges.push_back(CsvParseRange{.begin = begin, .end = end});
  }

  const auto cell_hint = projection_.column_count_hint();
  auto worker = [pool = memory_pool_, delimiter = delimiter_, cell_hint,
                 escape_char = escape_char_, max_field_bytes = max_field_bytes_,
                 max_record_bytes = max_record_bytes_,
                 records](CsvParseRange &&range, std::size_t,
                          sanitize::internal::StopToken stop)
      -> sanitize::Result<std::shared_ptr<CsvParsedChunk>> {
    if (stop.stop_requested()) {
      return sanitize::Status::Cancelled(
          "CSV frontend parse cancelled before record decoding");
    }
    std::size_t source_bytes = 0;
    for (std::size_t index = range.begin; index < range.end; ++index) {
      source_bytes += records[index].view.size();
    }
    const auto block_bytes = std::clamp<std::size_t>(
        source_bytes / 8U, std::size_t{4096}, std::size_t{262144});
    try {
      auto parsed = std::make_shared<CsvParsedChunk>(
          pool, block_bytes, range.end - range.begin, cell_hint);
      std::pmr::vector<std::string_view> row_cells(&parsed->pmr_pool);
      row_cells.reserve(cell_hint);
      for (std::size_t index = range.begin; index < range.end; ++index) {
        SAN_RETURN_NOT_OK(
            parse_csv_cells(records[index].view, delimiter, &row_cells,
                            &parsed->arena, records[index].base_offset,
                            max_field_bytes, max_record_bytes, escape_char));
        const auto begin = parsed->cells.size();
        parsed->cells.insert(parsed->cells.end(), row_cells.begin(),
                             row_cells.end());
        parsed->rows.push_back(
            CsvParsedRow{.cell_begin = begin, .cell_count = row_cells.size()});
      }
      return parsed;
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "CSV frontend parallel parse allocation failed");
    }
  };
  SAN_ASSIGN_OR_RAISE(auto executor,
                      Executor::Make(worker_count, worker_count * 2U,
                                     worker_count * 2U, std::move(worker),
                                     task_arena_, TaskArenaLane::kUpstream,
                                     TaskTelemetryKind::kInput));

  std::size_t submitted = 0;
  std::size_t committed = 0;
  auto take_and_append = [&]() -> sanitize::Status {
    SAN_ASSIGN_OR_RAISE(auto outcome, executor->TakeNext());
    if (!outcome.result.ok()) {
      executor->Cancel();
      return outcome.result.status();
    }
    const auto &range = ranges[static_cast<std::size_t>(outcome.ordinal)];
    auto parsed = std::move(outcome.result).ValueOrDie();
    const std::span<const std::string_view> parsed_cells(parsed->cells);
    for (std::size_t offset = 0; offset < parsed->rows.size(); ++offset) {
      const auto &record = records[range.begin + offset];
      const auto row = parsed->rows[offset];
      storage->batch.start_row(record.view, record.base_offset, 0, nullptr,
                               record.source_file);
      SAN_RETURN_NOT_OK(projection_.append_parsed_cells(
          &storage->batch, parsed_cells.subspan(row.cell_begin, row.cell_count),
          record.source_index, record.has_source_index));
      storage->batch.end_row();
    }
    storage->parsed_chunks.push_back(std::move(parsed));
    ++committed;
    return sanitize::Status::OK();
  };

  while (submitted < ranges.size()) {
    if (executor->in_flight() >= executor->dispatch_window()) {
      SAN_RETURN_NOT_OK(take_and_append());
    }
    SAN_RETURN_NOT_OK(executor->Submit(typename Executor::Packet{
        .ordinal = submitted, .payload = ranges[submitted]}));
    ++submitted;
  }
  SAN_RETURN_NOT_OK(executor->FinishSubmission());
  while (committed < submitted) {
    SAN_RETURN_NOT_OK(take_and_append());
  }
  return sanitize::Status::OK();
}

sanitize::Result<bool>
CsvFrontend::consume_header_record(const TextSlice &record) {
  if (!projection_.has_header()) {
    return false;
  }
  if (record.has_source_index) {
    if (last_header_source_ready_ &&
        record.source_index == last_header_source_index_) {
      return false;
    }
    SAN_RETURN_NOT_OK(process_header_record(record));
    last_header_source_index_ = record.source_index;
    last_header_source_ready_ = true;
    return true;
  }
  if (projection_.has_source_projections()) {
    if (last_header_source_ready_) {
      return false;
    }
    SAN_RETURN_NOT_OK(process_header_record(record));
    last_header_source_index_ = 0;
    last_header_source_ready_ = true;
    return true;
  }
  if (projection_.header_ready()) {
    return false;
  }
  SAN_RETURN_NOT_OK(process_header_record(record));
  return true;
}

sanitize::Status CsvFrontend::process_header_record(const TextSlice &record) {
  PoolResource resource(memory_pool_);
  BumpArena arena(memory_pool_.get(), arena_block_bytes_);
  std::pmr::vector<std::string_view> cells(&resource);
  cells.reserve(projection_.column_count_hint());
  SAN_RETURN_NOT_OK(parse_csv_cells(record.view, delimiter_, &cells, &arena,
                                    record.base_offset, max_field_bytes_,
                                    max_record_bytes_, escape_char_));
  SAN_RETURN_NOT_OK(projection_.validate_header_cells(cells));
  if (record.has_source_index) {
    SAN_RETURN_NOT_OK(
        projection_.validate_source_header(record.source_index, cells));
  } else {
    SAN_RETURN_NOT_OK(projection_.validate_source_header(0, cells));
  }
  if (projection_.has_source_projections()) {
    return sanitize::Status::OK();
  }
  if (projection_.header_ready()) {
    if (!projection_.header_cells_equal(cells)) {
      return sanitize::Status::Invalid("CSV directory header mismatch");
    }
    return sanitize::Status::OK();
  }
  projection_.set_header_cells(cells);
  return sanitize::Status::OK();
}

char CsvFrontend::resolved_delimiter(const Options &options) noexcept {
  if (options.csv_delimiter.empty() || options.csv_delimiter[0] == '\0') {
    return ',';
  }
  return options.csv_delimiter[0];
}

char CsvFrontend::resolved_escape_char(const Options &options) noexcept {
  return options.csv_escape_char.empty() ? '\0' : options.csv_escape_char[0];
}

} // namespace sanitize::internal::csv_frontend_detail

namespace sanitize::internal {
namespace {

using csv_frontend_detail::CsvFrontend;

void csv_reset(void *self) noexcept {
  static_cast<CsvFrontend *>(self)->reset();
}

void csv_set_plan(void *self, const CompiledPlan *plan) noexcept {
  static_cast<CsvFrontend *>(self)->set_plan(plan);
}

void csv_set_memory_pool(void *self, std::shared_ptr<void> pool) noexcept {
  static_cast<CsvFrontend *>(self)->set_memory_pool(std::move(pool));
}

void csv_set_task_arena(
    void *self, std::shared_ptr<OperationTaskArena> task_arena) noexcept {
  static_cast<CsvFrontend *>(self)->set_task_arena(std::move(task_arena));
}

sanitize::Result<RowBatch> csv_next_batch(void *self, int64_t capacity) {
  return static_cast<CsvFrontend *>(self)->next_batch(capacity);
}

void csv_destroy(void *self) noexcept {
  delete static_cast<CsvFrontend *>(self);
}

const FrontendVTable kCsvVTable{
    .reset = &csv_reset,
    .next_batch = &csv_next_batch,
    .set_plan = &csv_set_plan,
    .destroy = &csv_destroy,
    .set_memory_pool = &csv_set_memory_pool,
    .set_task_arena = &csv_set_task_arena,
};

} // namespace

FrontendHandle make_csv_frontend(ChunkSourcePtr csv, const Options &options,
                                 CsvSourceProjectionSetPtr source_projections) {
  auto *frontend =
      new CsvFrontend(std::move(csv), options, std::move(source_projections));
  return {frontend, &kCsvVTable};
}

} // namespace sanitize::internal

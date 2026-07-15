// Owns the CSV frontend lifecycle, batching, and vtable wiring.

#include "frontends/builtin_frontends.hh"
#include "frontends/csv/frontend_internal.hh"

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/memory/arena.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/flat_row_batch.hh"

namespace sanitize::internal::csv_frontend_detail {

struct CsvBatchStorage {
  BumpArena arena;
  FlatRowBatch batch;
  std::vector<std::string_view> cells;
  std::vector<std::shared_ptr<const void>> keepalive;
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

CsvFrontend::CsvFrontend(ChunkSourcePtr src, const Options &options)
    : source_(std::move(src)), delimiter_(resolved_delimiter(options)),
      projection_(options, delimiter_) {
  chunk_bytes_ = internal::memory_budget_from_limit(
                     options.memory_limit_bytes)
                     .io_chunk_bytes;
  scanner_ = std::make_unique<CsvStreamingScanner>(source_, chunk_bytes_);
  reset_status_ = scanner_->Reset();
}

void CsvFrontend::set_plan(const CompiledPlan *plan) noexcept {
  projection_.set_plan(plan);
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

  auto storage = std::make_shared<CsvBatchStorage>();
  storage->batch.reset(capacity);
  storage->cells.reserve(projection_.column_count_hint());
  int64_t produced = 0;
  while (produced < capacity) {
    SAN_ASSIGN_OR_RAISE(TextSlice record,
                        scanner_->next_record(&storage->arena));
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
    SAN_RETURN_NOT_OK(append_record(storage.get(), record));
    ++produced;
  }

  storage->batch.export_rows(&out.rows);
  out.owner = std::move(storage);
  return out;
}

sanitize::Status CsvFrontend::append_record(CsvBatchStorage *storage,
                                const TextSlice &record) {
  storage->keep_data_owner(record.owner);
  storage->keep_source_name(record.source_file_owner);
  if (projection_.can_use_raw_only()) {
    storage->batch.start_row(record.view, record.base_offset,
                             std::to_underlying(RowFlags::kRawOnly),
                             projection_.direct_context(), record.source_file);
    storage->batch.end_row();
    return sanitize::Status::OK();
  }

  storage->batch.start_row(record.view, record.base_offset, 0, nullptr,
                           record.source_file);
  SAN_RETURN_NOT_OK(
      parse_csv_cells(record.view, delimiter_, &storage->cells, &storage->arena));
  projection_.append_parsed_cells(&storage->batch, storage->cells);
  storage->batch.end_row();
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
  if (projection_.header_ready()) {
    return false;
  }
  SAN_RETURN_NOT_OK(process_header_record(record));
  return true;
}

sanitize::Status CsvFrontend::process_header_record(const TextSlice &record) {
  BumpArena arena;
  std::vector<std::string_view> cells;
  cells.reserve(projection_.column_count_hint());
  SAN_RETURN_NOT_OK(parse_csv_cells(record.view, delimiter_, &cells, &arena));
  SAN_RETURN_NOT_OK(projection_.validate_header_cells(cells));
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
};

} // namespace

FrontendHandle make_csv_frontend(ChunkSourcePtr csv, const Options &options) {
  auto *frontend = new CsvFrontend(std::move(csv), options);
  return {frontend, &kCsvVTable};
}

} // namespace sanitize::internal

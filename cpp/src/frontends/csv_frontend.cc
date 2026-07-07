// Parses CSV input into flat row batches for ingestion.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/frontends/builtin_frontends.hh"
#include "internal/frontends/csv_column_projection.hh"
#include "internal/memory/arena.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/parsing/row_scanner.hh"
#include "internal/parsing/streaming/csv_streaming_scanner.hh"
#include "sanitize/core/status.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

namespace {

constexpr int64_t kDefaultChunkBytes = 1024LL * 1024LL;

struct BatchStorage {
  BumpArena arena;
  FlatRowBatch batch;
  std::vector<std::string_view> cells;

  // Keep backing chunks alive when rows/cells alias ChunkSource buffers.
  std::vector<std::shared_ptr<const void>> keepalive;
  const void *last_owner_ptr = nullptr;

  // Retains storage for a referenced value.
  void keep(const std::shared_ptr<const void> &owner) {
    if (!owner)
      return;
    const void *p = owner.get();
    if (p == last_owner_ptr)
      return;
    last_owner_ptr = p;
    keepalive.push_back(owner);
  }

  // Retains source-name storage for generated row metadata.
  void keep_source_name(const std::shared_ptr<const std::string> &owner) {
    keep(std::static_pointer_cast<const void>(owner));
  }
};

class CsvFrontend final {
public:
  // Creates a streaming CSV frontend with option-derived scanner state.
  CsvFrontend(ChunkSourcePtr src, const Options &opts)
      : src_(std::move(src)), projection_(opts, resolved_delimiter(opts)) {
    if (!opts.csv_delimiter.empty())
      delimiter_ = opts.csv_delimiter[0];
    if (delimiter_ == '\0')
      delimiter_ = ',';

    chunk_bytes_ =
        (opts.io_chunk_bytes > 0) ? opts.io_chunk_bytes : kDefaultChunkBytes;
    scanner_ = std::make_unique<CsvStreamingScanner>(src_, chunk_bytes_);
    reset_status_ = scanner_->Reset();
  }

  // Installs a compiled plan and invalidates derived CSV column mappings.
  void set_plan(const CompiledPlan *p) noexcept { projection_.set_plan(p); }

  // Rewinds CSV scanning and invalidates derived header state.
  void reset() noexcept {
    if (scanner_) {
      reset_status_ = scanner_->Reset();
    } else if (src_) {
      reset_status_ = src_->Reset();
    }
    projection_.reset_header();
    last_header_source_index_ = 0;
    last_header_source_ready_ = false;
  }

  // Reads and materializes the next CSV row batch.
  sanitize::Result<RowBatch> next_batch(int64_t capacity) {
    RowBatch out;
    if (capacity <= 0)
      return out;

    if (!reset_status_.ok())
      return reset_status_;
    if (!scanner_)
      return sanitize::Status::Invalid("CSV frontend: scanner is null");

    auto storage = std::make_shared<BatchStorage>();
    storage->batch.reset(capacity);
    storage->cells.clear();
    storage->arena.reset();

    int64_t produced = 0;
    while (produced < capacity) {
      SAN_ASSIGN_OR_RAISE(TextSlice rec,
                          scanner_->next_record(&storage->arena));
      if (rec.view.empty() && scanner_->done()) {
        break;
      }
      if (rec.view.empty()) {
        continue; // skip empty records
      }
      SAN_ASSIGN_OR_RAISE(const bool is_header, consume_header_record(rec));
      if (is_header) {
        continue;
      }

      append_record(storage.get(), rec);
      produced++;
    }

    storage->batch.export_rows(&out.rows);
    out.owner = std::move(storage);
    return out;
  }

private:
  // Appends one CSV record using the direct or parsed materialization path.
  void append_record(BatchStorage *storage, const TextSlice &rec) {
    storage->keep(rec.owner);
    storage->keep_source_name(rec.source_file_owner);
    if (projection_.can_use_raw_only()) {
      // Direct materialization path: defer CSV parsing to the materializer.
      storage->batch.start_row(rec.view, rec.base_offset,
                               std::to_underlying(RowFlags::kRawOnly),
                               direct_context(), rec.source_file);
      storage->batch.end_row();
      return;
    }

    storage->batch.start_row(rec.view, rec.base_offset, 0, nullptr,
                             rec.source_file);
    parse_csv_cells(rec.view, delimiter_, &storage->cells, &storage->arena);
    projection_.append_parsed_cells(&storage->batch, storage->cells);
    storage->batch.end_row();
  }

  // Returns whether one CSV record is the configured per-source header.
  sanitize::Result<bool> consume_header_record(const TextSlice &rec) {
    if (!projection_.has_header()) {
      return false;
    }
    if (rec.has_source_index) {
      if (last_header_source_ready_ &&
          rec.source_index == last_header_source_index_) {
        return false;
      }
      SAN_RETURN_NOT_OK(process_header_record(rec));
      last_header_source_index_ = rec.source_index;
      last_header_source_ready_ = true;
      return true;
    }
    if (projection_.header_ready()) {
      return false;
    }
    SAN_RETURN_NOT_OK(process_header_record(rec));
    return true;
  }

  // Parses and validates one header record.
  sanitize::Status process_header_record(const TextSlice &rec) {
    BumpArena tmp_arena;
    std::vector<std::string_view> cells;
    parse_csv_cells(rec.view, delimiter_, &cells, &tmp_arena);
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

public:
  // For direct materialization (RowFlags::kRawOnly), materialize needs access
  // to the CSV header mapping computed here.
  [[nodiscard]] const CsvDirectContext *direct_context() const noexcept {
    return projection_.direct_context();
  }

  // Resolves the effective one-byte CSV delimiter from user options.
  static char resolved_delimiter(const Options &opts) noexcept {
    if (opts.csv_delimiter.empty() || opts.csv_delimiter[0] == '\0') {
      return ',';
    }
    return opts.csv_delimiter[0];
  }

  ChunkSourcePtr src_;
  int64_t chunk_bytes_ = kDefaultChunkBytes;
  sanitize::Status reset_status_ = sanitize::Status::OK();

  char delimiter_ = ',';
  CsvColumnProjection projection_;
  std::size_t last_header_source_index_ = 0;
  bool last_header_source_ready_ = false;

  std::unique_ptr<CsvStreamingScanner> scanner_;
};

// Adapts CsvFrontend::reset to the frontend vtable.
static void csv_reset(void *self) noexcept {
  static_cast<CsvFrontend *>(self)->reset();
}

// Adapts CsvFrontend::set_plan to the frontend vtable.
static void csv_set_plan(void *self, const CompiledPlan *plan) noexcept {
  static_cast<CsvFrontend *>(self)->set_plan(plan);
}

// Adapts CsvFrontend::next_batch to the frontend vtable.
static sanitize::Result<RowBatch> csv_next_batch(void *self, int64_t capacity) {
  return static_cast<CsvFrontend *>(self)->next_batch(capacity);
}

// Releases a CsvFrontend stored behind a frontend handle.
static void csv_destroy(void *self) noexcept {
  delete static_cast<CsvFrontend *>(self);
}

static const FrontendVTable kCsvVTable{
    .reset = &csv_reset,
    .next_batch = &csv_next_batch,
    .set_plan = &csv_set_plan,
    .destroy = &csv_destroy,
};

} // namespace

FrontendHandle make_csv_frontend(ChunkSourcePtr csv, const Options &options) {
  auto *fe = new CsvFrontend(std::move(csv), options);
  return {fe, &kCsvVTable};
}

} // namespace sanitize::internal

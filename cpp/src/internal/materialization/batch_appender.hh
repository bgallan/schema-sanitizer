// Internal Arrow C Data batch builders and row appenders.
//
// This module is intentionally private to the native implementation. It keeps
// materialization independent from Arrow C++ while exposing a compact API to
// the stream layer and direct JSON/CSV frontends.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

struct ArrowArray;

namespace sanitize {
struct CompiledPlan;
} // namespace sanitize

namespace sanitize::internal {

class PoolResource;

class BumpArena;
struct CsvDirectContext;
class JsonOnDemandDoc;
struct JsonValidatedRowTokens;

struct AppendRowResult {
  DiagnosticCode code = DiagnosticCode::kUnknown;
  uint32_t path_id = 0;
  std::string detail;
};

struct Cell {
  bool is_null = true;
  sanitize::LogicalKind kind = sanitize::LogicalKind::kNull;
  bool b = false;
  int64_t i64 = 0;
  double f64 = 0.0;
  std::string str;
  std::vector<Cell> children;
  std::vector<Cell> elements;

  // Creates a null materialization cell for one logical kind.
  static Cell Null(sanitize::LogicalKind kind) {
    Cell cell;
    cell.is_null = true;
    cell.kind = kind;
    return cell;
  }
};

enum class PreparedRowAction : std::uint8_t {
  kAppendCells = 0,
  kAppendNull = 1,
  kSkip = 2,
};

struct PreparedRow {
  PreparedRowAction action = PreparedRowAction::kAppendCells;
  std::vector<Cell> cells;
  AppendRowResult result;
};

class BatchAppender;
struct BatchAppenderDeleter {
  // Invokes the callable.
  void operator()(BatchAppender *app) const noexcept;
};
using BatchAppenderPtr = std::unique_ptr<BatchAppender, BatchAppenderDeleter>;

// Creates batch appender.
sanitize::Result<BatchAppenderPtr>
make_batch_appender(const sanitize::CompiledPlan &plan,
                    std::shared_ptr<PoolResource> pool);

// Performs the batch appender reset operation.
sanitize::Status batch_appender_reset(BatchAppender *app);
// Performs the batch appender length operation.
int64_t batch_appender_length(const BatchAppender *app) noexcept;
// Performs the batch appender bytes operation.
int64_t batch_appender_bytes(const BatchAppender *app) noexcept;
// Reserves predictable storage for one scalar packet.
sanitize::Status batch_appender_reserve(BatchAppender *app, int64_t rows,
                                        int64_t source_bytes);
// Performs the batch appender finish operation.
sanitize::Status batch_appender_finish(BatchAppender *app, ArrowArray *out);
// Appends a compatible Arrow struct array into the current batch.
sanitize::Status batch_appender_append_array(BatchAppender *app,
                                             const ArrowArray *array);

// Converts one row into immutable coordinator-owned cells without touching an
// Arrow builder.
sanitize::Result<PreparedRow>
prepare_row(const sanitize::CompiledPlan &plan, const sanitize::RowRef &row,
            const sanitize::PreparedOptions &opts,
            sanitize::IngestDiagnostics *diagnostics);

// Commits one prepared row into the single-owner Arrow batch appender.
sanitize::Status append_prepared_row(BatchAppender *app, PreparedRow row);

// Appends row.
sanitize::Result<AppendRowResult>
append_row(BatchAppender *app, const sanitize::RowRef &row,
           const sanitize::PreparedOptions &opts,
           sanitize::IngestDiagnostics *diagnostics);

// Converts one raw JSON row into coordinator-owned cells.
sanitize::Result<PreparedRow>
prepare_row_json_text(const sanitize::CompiledPlan &plan, JsonOnDemandDoc *doc,
                      std::vector<sanitize::FieldRef> *fields,
                      std::string_view raw, std::size_t base_offset,
                      std::string_view source_file,
                      const sanitize::PreparedOptions &opts,
                      sanitize::IngestDiagnostics *diagnostics);

// Appends row json text.
sanitize::Result<AppendRowResult>
append_row_json_text(BatchAppender *app, JsonOnDemandDoc *doc,
                     std::string_view raw, std::size_t base_offset,
                     std::string_view source_file,
                     const sanitize::PreparedOptions &opts,
                     sanitize::IngestDiagnostics *diagnostics);

// Converts a syntax-validated JSON object through its immutable field spans.
sanitize::Result<PreparedRow> prepare_row_json_tokens(
    const sanitize::CompiledPlan &plan, JsonOnDemandDoc *doc,
    std::vector<sanitize::FieldRef> *fields, std::string_view raw,
    std::size_t base_offset, std::string_view source_file,
    const JsonValidatedRowTokens &tokens, const sanitize::PreparedOptions &opts,
    sanitize::IngestDiagnostics *diagnostics);

// Appends a syntax-validated JSON object through its immutable field spans.
sanitize::Result<AppendRowResult> append_row_json_tokens(
    BatchAppender *app, JsonOnDemandDoc *doc, std::string_view raw,
    std::size_t base_offset, std::string_view source_file,
    const JsonValidatedRowTokens &tokens, bool plan_ordered_tokens,
    const sanitize::PreparedOptions &opts,
    sanitize::IngestDiagnostics *diagnostics);

// Converts one raw CSV row into coordinator-owned cells.
sanitize::Result<PreparedRow> prepare_row_csv_text(
    const sanitize::CompiledPlan &plan, const CsvDirectContext &ctx,
    BumpArena *arena, std::vector<std::string_view> *cells,
    std::vector<sanitize::FieldRef> *fields, std::string_view raw,
    std::size_t base_offset, std::string_view source_file,
    const sanitize::PreparedOptions &opts,
    sanitize::IngestDiagnostics *diagnostics);

// Appends row csv text.
sanitize::Result<AppendRowResult>
append_row_csv_text(BatchAppender *app, const CsvDirectContext &ctx,
                    BumpArena *arena, std::vector<std::string_view> *cells,
                    std::string_view raw, std::size_t base_offset,
                    std::string_view source_file,
                    const sanitize::PreparedOptions &opts,
                    sanitize::IngestDiagnostics *diagnostics);

} // namespace sanitize::internal

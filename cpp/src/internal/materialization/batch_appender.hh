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

struct AppendRowResult {
  DiagnosticCode code = DiagnosticCode::kUnknown;
  uint32_t path_id = 0;
  std::string detail;
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
// Performs the batch appender finish operation.
sanitize::Status batch_appender_finish(BatchAppender *app, ArrowArray *out);

// Appends row.
sanitize::Result<AppendRowResult>
append_row(BatchAppender *app, const sanitize::RowRef &row,
           const sanitize::PreparedOptions &opts,
           sanitize::IngestDiagnostics *diagnostics);

// Appends row json text.
sanitize::Result<AppendRowResult>
append_row_json_text(BatchAppender *app, JsonOnDemandDoc *doc,
                     std::string_view raw, std::size_t base_offset,
                     std::string_view source_file,
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

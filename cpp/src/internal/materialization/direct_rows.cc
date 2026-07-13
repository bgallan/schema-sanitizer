// Implements direct raw-row materialization for the internal pipeline.

#include "internal/materialization/direct_rows.hh"

#include <memory>
#include <new>
#include <string_view>
#include <vector>

#include "internal/materialization/batch_appender.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/csv_direct.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

namespace {

class JsonDirectMaterializer final : public DirectMaterializer {
public:
  // Creates a direct JSON materializer with parser storage from the memory
  // pool.
  explicit JsonDirectMaterializer(PoolResource *pmr_pool) : doc_(pmr_pool) {}

  // Appends one raw JSON row directly into the batch appender.
  sanitize::Result<AppendRowResult>
  AppendRaw(BatchAppender *app, const RowRef &row, const PreparedOptions &opts,
            IngestDiagnostics *diagnostics) override {
    return append_row_json_text(app, &doc_, row.raw, row.base_offset,
                                row.source_file, opts, diagnostics);
  }

private:
  JsonOnDemandDoc doc_;
};

class CsvDirectMaterializer final : public DirectMaterializer {
public:
  // Creates a direct CSV materializer with temporary parser arena storage.
  explicit CsvDirectMaterializer(PoolResource *pmr_pool)
      : arena_(pmr_pool ? pmr_pool->pool() : nullptr) {}

  // Appends one raw CSV row directly into the batch appender.
  sanitize::Result<AppendRowResult>
  AppendRaw(BatchAppender *app, const RowRef &row, const PreparedOptions &opts,
            IngestDiagnostics *diagnostics) override {
    const auto *ctx = static_cast<const CsvDirectContext *>(row.direct_ctx);
    if (!ctx) {
      return sanitize::Status::Invalid(
          "raw-only CSV row encountered but row.direct_ctx is null (frontend "
          "did not attach direct context)");
    }
    return append_row_csv_text(app, *ctx, &arena_, &cells_, row.raw,
                               row.base_offset, row.source_file, opts,
                               diagnostics);
  }

private:
  BumpArena arena_;
  std::vector<std::string_view> cells_;
};

// Creates a frontend-specific direct row materializer.
template <typename DirectT>
sanitize::Result<std::unique_ptr<DirectMaterializer>>
make_frontend_direct_materializer(PoolResource *pmr_pool, const char *name) {
  if (!pmr_pool)
    return sanitize::Status::Invalid(
        name, " direct row materializer requires pmr_pool");
  try {
    return std::make_unique<DirectT>(pmr_pool);
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(name, " direct row materializer: OOM");
  }
}

} // namespace

sanitize::Result<std::unique_ptr<DirectMaterializer>>
make_direct_materializer(std::string_view frontend_name,
                         PoolResource *pmr_pool) {
  if (frontend_name == "json" || frontend_name == "json_array")
    return make_frontend_direct_materializer<JsonDirectMaterializer>(pmr_pool,
                                                                     "Json");
  if (frontend_name == "csv")
    return make_frontend_direct_materializer<CsvDirectMaterializer>(pmr_pool,
                                                                    "Csv");
  if (frontend_name == "arrow")
    return nullptr;
  if (frontend_name == "xml")
    return nullptr;

  return sanitize::Status::Invalid(
      "unsupported direct row materializer frontend: ", frontend_name);
}

} // namespace sanitize::internal

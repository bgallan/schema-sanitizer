// Implements direct raw-row materialization for the internal pipeline.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#include "internal/materialization/direct_rows.hh"

#include <memory>
#include <new>
#include <string_view>
#include <vector>

#include "internal/materialization/batch_appender_internal.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/csv_direct.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/json/validated_row.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

namespace {

class JsonDirectMaterializer final : public DirectMaterializer {
public:
  /// Initializes a direct JSON materializer with parser storage from the
  /// operation pool.
  explicit JsonDirectMaterializer(PoolResource *pmr_pool) : doc_(pmr_pool) {}

  /// Checks that a raw frontend row exposes the parser state required by its
  /// declared row kind.
  [[nodiscard]] static sanitize::Status
  validate_frontend_row_contract(const RowRef &row) {
    if ((row.flags & std::to_underlying(RowFlags::kJsonObjectRequired)) == 0) {
      return sanitize::Status::OK();
    }
    auto value = row.raw;
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t' ||
                              value.front() == '\r' || value.front() == '\n')) {
      value.remove_prefix(1);
    }
    if (value.empty() || value.front() != '{') {
      return sanitize::Status::Invalid("json_array requires object elements");
    }
    return sanitize::Status::OK();
  }

  /// Converts one raw JSON row using worker-local parser scratch.
  sanitize::Result<PreparedRow>
  PrepareRaw(const sanitize::CompiledPlan &plan, const RowRef &row,
             const PreparedOptions &opts,
             IngestDiagnostics *diagnostics) override {
    SAN_RETURN_NOT_OK(validate_frontend_row_contract(row));
    if (const auto *tokens = json_validated_row_tokens(row)) {
      return prepare_row_json_tokens(plan, &doc_, &fields_, row.raw,
                                     row.base_offset, row.source_file, *tokens,
                                     opts, diagnostics);
    }
    return prepare_row_json_text(plan, &doc_, &fields_, row.raw,
                                 row.base_offset, row.source_file, opts,
                                 diagnostics);
  }

  /// Validates the frontend row contract before appending one raw row to the
  /// active batch builder.
  sanitize::Result<AppendRowResult>
  AppendRaw(BatchAppender *app, const RowRef &row, const PreparedOptions &opts,
            IngestDiagnostics *diagnostics) override {
    SAN_RETURN_NOT_OK(validate_frontend_row_contract(row));
    if (const auto *tokens = json_validated_row_tokens(row)) {
      return append_row_json_tokens(
          app, &doc_, row.raw, row.base_offset, row.source_file, *tokens,
          json_validated_row_tokens_are_plan_ordered(row), opts, diagnostics);
    }
    return append_row_json_text(app, &doc_, row.raw, row.base_offset,
                                row.source_file, opts, diagnostics);
  }

private:
  JsonOnDemandDoc doc_;
  std::vector<sanitize::FieldRef> fields_;
};

class CsvDirectMaterializer final : public DirectMaterializer {
public:
  /// Creates a direct CSV materializer with temporary parser arena storage.
  explicit CsvDirectMaterializer(PoolResource *pmr_pool)
      : arena_(pmr_pool ? pmr_pool->pool() : nullptr) {}

  /// Converts one raw CSV row using worker-local parser scratch.
  sanitize::Result<PreparedRow>
  PrepareRaw(const sanitize::CompiledPlan &plan, const RowRef &row,
             const PreparedOptions &opts,
             IngestDiagnostics *diagnostics) override {
    const auto *ctx = static_cast<const CsvDirectContext *>(row.direct_ctx);
    if (!ctx) {
      return sanitize::Status::Invalid(
          "raw-only CSV row encountered but row.direct_ctx is null (frontend "
          "did not attach direct context)");
    }
    return prepare_row_csv_text(plan, *ctx, &arena_, &cells_, &fields_, row.raw,
                                row.base_offset, row.source_file, opts,
                                diagnostics);
  }

private:
  BumpArena arena_;
  std::vector<std::string_view> cells_;
  std::vector<sanitize::FieldRef> fields_;
};

/// Creates a frontend-specific direct row materializer.
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

sanitize::Result<AppendRowResult>
DirectMaterializer::AppendRaw(BatchAppender *app, const RowRef &row,
                              const PreparedOptions &opts,
                              IngestDiagnostics *diagnostics) {
  if (!app) {
    return sanitize::Status::Invalid(
        "DirectMaterializer::AppendRaw: app is null");
  }
  SAN_ASSIGN_OR_RAISE(auto prepared,
                      PrepareRaw(app->plan(), row, opts, diagnostics));
  const auto result = prepared.result;
  SAN_RETURN_NOT_OK(append_prepared_row(app, std::move(prepared)));
  return result;
}

sanitize::Result<std::unique_ptr<DirectMaterializer>>
make_direct_materializer(std::string_view frontend_name,
                         PoolResource *pmr_pool) {
  if (frontend_name == "json" || frontend_name == "jsonl" ||
      frontend_name == "json_array")
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

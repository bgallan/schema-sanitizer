// Adapts CSV and JSON inputs into materialized Arrow rows.

#include "frontends/json/text_row_pipeline.hh"
#include "internal/materialization/batch_appender_internal.hh"
#include "internal/materialization/conversion/variants.hh"
#include "internal/planning/variant_field_names.hh"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "internal/memory/arena.hh"
#include "internal/parsing/csv_direct.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "sanitize/core/value_view.hh"

namespace sanitize::internal {
namespace {

constexpr std::size_t kRetainedDirectCsvCellCapacity = 4096;

class CsvDirectScratchReset final {
public:
  CsvDirectScratchReset(BumpArena *arena,
                        std::vector<std::string_view> *cells) noexcept
      : arena_(arena), cells_(cells) {}

  ~CsvDirectScratchReset() noexcept {
    if (cells_) {
      cells_->clear();
      if (cells_->capacity() > kRetainedDirectCsvCellCapacity) {
        std::vector<std::string_view>().swap(*cells_);
      }
    }
    if (arena_) {
      arena_->reset();
    }
  }

private:
  BumpArena *arena_ = nullptr;
  std::vector<std::string_view> *cells_ = nullptr;
};

class JsonDirectScratchReset final {
public:
  explicit JsonDirectScratchReset(JsonOnDemandDoc *doc) noexcept : doc_(doc) {}

  ~JsonDirectScratchReset() noexcept {
    if (doc_) {
      doc_->Reset();
    }
  }

private:
  JsonOnDemandDoc *doc_ = nullptr;
};

bool should_snapshot_root_fields(const sanitize::CompiledPlan &plan) noexcept {
  constexpr std::size_t kWideRootThreshold = 8;
  if (plan.columns.size() >= kWideRootThreshold) {
    return true;
  }
  return std::ranges::any_of(plan.columns,
                             &sanitize::ColumnPlan::has_variant_sibling);
}

PreparedRow result_for_policy(const PreparedOptions &opts,
                              sanitize::IngestDiagnostics *diag,
                              const CoerceError &err) {
  PreparedRow prepared;
  prepared.result.code = err.code;
  prepared.result.path_id = err.path_id;
  prepared.result.detail = err.detail;
  switch (opts.spec.on_error) {
  case sanitize::OnErrorPolicy::kStop:
    break;
  case sanitize::OnErrorPolicy::kSkipRow:
    if (diag) {
      diag->skipped_rows += 1;
    }
    prepared.action = PreparedRowAction::kSkip;
    prepared.result.code = DiagnosticCode::kRowSkipped;
    return prepared;
  case sanitize::OnErrorPolicy::kEmitNullRow:
    prepared.action = PreparedRowAction::kAppendNull;
    prepared.result = AppendRowResult{};
    return prepared;
  }
  return prepared;
}

} // namespace

namespace {

sanitize::Result<std::optional<CoerceError>> convert_materialized_cells(
    const sanitize::CompiledPlan &plan, const sanitize::RowRef &row,
    const PreparedOptions &opts, sanitize::IngestDiagnostics *diagnostics,
    std::vector<Cell> *cells, RowFieldSnapshot *reusable_snapshot = nullptr) {
  if (!cells) {
    return Status::Invalid("convert_materialized_cells: cells is null");
  }
  FieldLookup lookup{&row};
  RowFieldSnapshot local_snapshot;
  RowFieldSnapshot &snapshot =
      reusable_snapshot ? *reusable_snapshot : local_snapshot;
  const bool use_snapshot = should_snapshot_root_fields(plan);

  if (opts.spec.arrow_schema_contract &&
      opts.spec.schema_evolution == sanitize::SchemaEvolutionMode::kStrict) {
    std::string extra;
    SAN_ASSIGN_OR_RAISE(
        bool has_unplanned,
        lookup.has_unplanned_field(plan.root_layout, opts, &extra));
    if (has_unplanned) {
      return Status::Invalid("Strict schema evolution: observed extra field '" +
                             extra + "'");
    }
  }
  if (use_snapshot) {
    SAN_RETURN_NOT_OK(snapshot.build(row, plan, opts));
  }

  cells->clear();
  cells->resize(plan.columns.size());
  CoerceError err;
  ConvertCtx ctx{
      .opts = opts,
      .diagnostics = diagnostics,
      .error = &err,
  };

  for (std::size_t i = 0; i < plan.columns.size(); ++i) {
    const auto &column = plan.columns[i];
    const bool has_variant = column.has_variant_sibling;
    bool found = false;
    ValueView value = ValueView::Null();
    if (column.name == "source_file" && !row.source_file.empty()) {
      value = ValueView::String(row.source_file);
      found = true;
    } else if (use_snapshot) {
      found = snapshot.find(i, &value);
    } else {
      SAN_ASSIGN_OR_RAISE(value, lookup.find(column.name, opts, &found));
      if (!found) {
        const std::string_view original = unflattened_name(column.name);
        if (!original.empty()) {
          SAN_ASSIGN_OR_RAISE(value, lookup.find(original, opts, &found));
        }
      }
      if (!found && has_variant) {
        const std::string_view family_base = variant_family_base(column.name);
        if (family_base != column.name) {
          SAN_ASSIGN_OR_RAISE(value, lookup.find(family_base, opts, &found));
        }
      }
    }

    bool empty_container = false;
    SAN_RETURN_NOT_OK(value.container_is_empty(&empty_container));
    if (empty_container) {
      value = ValueView::Null();
    }

    Status status = Status::OK();
    if (has_variant && found && !value.is_null() &&
        preferred_root_variant_sibling(plan, column, value, opts) != &column) {
      if (ctx.error) {
        *ctx.error = CoerceError{};
      }
      status = convert_null(column, &(*cells)[i]);
    } else {
      status = (!found || value.is_null())
                   ? convert_null(column, &(*cells)[i])
                   : convert_value(column, value, ctx, &(*cells)[i]);
    }
    if (status.ok()) {
      continue;
    }
    if (has_variant) {
      if (ctx.error) {
        *ctx.error = CoerceError{};
      }
      SAN_RETURN_NOT_OK(convert_null(column, &(*cells)[i]));
      continue;
    }
    if (err.detail.empty()) {
      err.code = DiagnosticCode::kTypeMismatch;
      err.path_id = static_cast<uint32_t>(column.path_id);
      err.detail = status.message();
    }
    return std::optional<CoerceError>(std::move(err));
  }

  return std::optional<CoerceError>{};
}

sanitize::Result<PreparedRow>
prepared_error_for_policy(const PreparedOptions &opts,
                          sanitize::IngestDiagnostics *diagnostics,
                          const CoerceError &error) {
  if (opts.spec.on_error == sanitize::OnErrorPolicy::kStop) {
    return Status::Invalid(error.detail);
  }
  return result_for_policy(opts, diagnostics, error);
}

} // namespace

sanitize::Result<PreparedRow> prepare_materialized_row(
    const sanitize::CompiledPlan &plan, const sanitize::RowRef &row,
    const PreparedOptions &opts, sanitize::IngestDiagnostics *diagnostics) {
  PreparedRow prepared;
  SAN_ASSIGN_OR_RAISE(auto error,
                      convert_materialized_cells(plan, row, opts, diagnostics,
                                                 &prepared.cells));
  if (error) {
    return prepared_error_for_policy(opts, diagnostics, *error);
  }
  return prepared;
}

sanitize::Result<AppendRowResult>
append_materialized_row_reuse(BatchAppender *app, const sanitize::RowRef &row,
                              const PreparedOptions &opts,
                              sanitize::IngestDiagnostics *diagnostics) {
  if (!app) {
    return Status::Invalid("append_materialized_row_reuse: app is null");
  }
  SAN_ASSIGN_OR_RAISE(
      auto direct, try_append_direct_scalar_row(app, row, opts, diagnostics));
  if (direct) {
    return std::move(*direct);
  }
  auto &cells = app->prepare_row_cells(app->plan().columns.size());
  SAN_ASSIGN_OR_RAISE(auto error, convert_materialized_cells(
                                      app->plan(), row, opts, diagnostics,
                                      &cells, &app->prepare_row_snapshot()));
  if (!error) {
    SAN_RETURN_NOT_OK(app->append_prepared_cells());
    return AppendRowResult{};
  }

  SAN_ASSIGN_OR_RAISE(auto prepared,
                      prepared_error_for_policy(opts, diagnostics, *error));
  const auto result = prepared.result;
  switch (prepared.action) {
  case PreparedRowAction::kAppendCells:
    return Status::Invalid(
        "append_materialized_row_reuse: invalid error action");
  case PreparedRowAction::kAppendNull:
    SAN_RETURN_NOT_OK(app->append_null_row());
    break;
  case PreparedRowAction::kSkip:
    break;
  }
  return result;
}

sanitize::Result<PreparedRow> prepare_row_csv_text(
    const sanitize::CompiledPlan &plan, const CsvDirectContext &ctx,
    BumpArena *arena, std::vector<std::string_view> *cells,
    std::vector<sanitize::FieldRef> *fields, std::string_view raw,
    std::size_t base_offset, std::string_view source_file,
    const PreparedOptions &opts, sanitize::IngestDiagnostics *diagnostics) {
  if (!cells) {
    return Status::Invalid("prepare_row_csv_text: cells is null");
  }
  if (!fields) {
    return Status::Invalid("prepare_row_csv_text: fields is null");
  }
  if (arena) {
    arena->reset();
  }
  CsvDirectScratchReset scratch_reset(arena, cells);
  SAN_RETURN_NOT_OK(parse_csv_cells(raw, ctx.delimiter, cells, arena,
                                    base_offset, ctx.max_field_bytes,
                                    ctx.max_decoded_record_bytes));

  fields->clear();
  if (fields->capacity() < plan.columns.size()) {
    fields->reserve(plan.columns.size());
  }
  for (std::size_t i = 0; i < plan.columns.size(); ++i) {
    int32_t csv_index = -1;
    if (i < ctx.col_to_csv.size()) {
      csv_index = ctx.col_to_csv[i];
    }
    if (csv_index < 0 || static_cast<std::size_t>(csv_index) >= cells->size()) {
      continue;
    }
    const std::string_view cell = (*cells)[static_cast<std::size_t>(csv_index)];
    fields->push_back(sanitize::FieldRef{
        .key = plan.columns[i].name,
        .key_hash = plan.columns[i].name_hash,
        .value = cell.empty() ? ValueView::Null() : ValueView::String(cell),
    });
  }

  const sanitize::RowRef row{
      .fields = fields->empty() ? nullptr : fields->data(),
      .size = fields->size(),
      .raw = raw,
      .base_offset = base_offset,
      .source_file = source_file,
  };
  return prepare_materialized_row(plan, row, opts, diagnostics);
}

sanitize::Result<AppendRowResult>
append_row_csv_text(BatchAppender *app, const CsvDirectContext &ctx,
                    BumpArena *arena, std::vector<std::string_view> *cells,
                    std::string_view raw, std::size_t base_offset,
                    std::string_view source_file, const PreparedOptions &opts,
                    sanitize::IngestDiagnostics *diagnostics) {
  if (!app) {
    return Status::Invalid("append_row_csv_text: app is null");
  }
  auto &fields = app->prepare_field_refs(app->plan().columns.size());
  SAN_ASSIGN_OR_RAISE(auto prepared,
                      prepare_row_csv_text(app->plan(), ctx, arena, cells,
                                           &fields, raw, base_offset,
                                           source_file, opts, diagnostics));
  const auto result = prepared.result;
  SAN_RETURN_NOT_OK(append_prepared_row(app, std::move(prepared)));
  return result;
}

sanitize::Result<PreparedRow>
prepare_row_json_text(const sanitize::CompiledPlan &plan, JsonOnDemandDoc *doc,
                      std::vector<sanitize::FieldRef> *fields,
                      std::string_view raw, std::size_t base_offset,
                      std::string_view source_file, const PreparedOptions &opts,
                      sanitize::IngestDiagnostics *diagnostics) {
  if (!doc) {
    return Status::Invalid("prepare_row_json_text: doc is null");
  }
  if (!fields) {
    return Status::Invalid("prepare_row_json_text: fields is null");
  }
  doc->Reset();
  JsonDirectScratchReset scratch_reset(doc);
  auto parsed = doc->ParseValue(raw, base_offset);
  if (!parsed.ok()) {
    const auto status = parsed.status();
    if (json_error_exceeds_hard_safety_limit(status)) {
      return status;
    }
    CoerceError error;
    error.code = DiagnosticCode::kCoercionFailure;
    error.detail = status.message();
    return prepared_error_for_policy(opts, diagnostics, error);
  }
  ValueView root = std::move(parsed).ValueOrDie();

  fields->clear();
  if (fields->capacity() < 16) {
    fields->reserve(16);
  }
  if (root.is_object()) {
    SAN_RETURN_NOT_OK(
        root.for_each_object_field([&](std::string_view key, uint64_t key_hash,
                                       ValueView value) -> Status {
          if (fields->size() >= kMaxMaterializedFieldsPerRow) {
            return Status::Invalid(
                "JSON object field count exceeds safety limit: ",
                fields->size() + 1U, " > ", kMaxMaterializedFieldsPerRow);
          }
          fields->push_back(sanitize::FieldRef{
              .key = key,
              .key_hash = key_hash,
              .value = value,
          });
          return Status::OK();
        }));
  } else {
    fields->push_back(sanitize::FieldRef{
        .key = opts.spec.default_key_name,
        .key_hash = 0,
        .value = root,
    });
  }

  const sanitize::RowRef row{
      .fields = fields->empty() ? nullptr : fields->data(),
      .size = fields->size(),
      .raw = raw,
      .base_offset = base_offset,
      .source_file = source_file,
  };
  return prepare_materialized_row(plan, row, opts, diagnostics);
}

sanitize::Result<AppendRowResult>
append_row_json_text(BatchAppender *app, JsonOnDemandDoc *doc,
                     std::string_view raw, std::size_t base_offset,
                     std::string_view source_file, const PreparedOptions &opts,
                     sanitize::IngestDiagnostics *diagnostics) {
  if (!app) {
    return Status::Invalid("append_row_json_text: app is null");
  }
  auto &fields = app->prepare_field_refs(16);
  if (!doc) {
    return Status::Invalid("append_row_json_text: doc is null");
  }
  doc->Reset();
  JsonDirectScratchReset scratch_reset(doc);
  auto parsed = doc->ParseValue(raw, base_offset);
  if (!parsed.ok()) {
    const auto status = parsed.status();
    if (json_error_exceeds_hard_safety_limit(status)) {
      return status;
    }
    CoerceError error;
    error.code = DiagnosticCode::kCoercionFailure;
    error.detail = status.message();
    SAN_ASSIGN_OR_RAISE(auto prepared,
                        prepared_error_for_policy(opts, diagnostics, error));
    const auto result = prepared.result;
    SAN_RETURN_NOT_OK(append_prepared_row(app, std::move(prepared)));
    return result;
  }
  ValueView root = std::move(parsed).ValueOrDie();
  fields.clear();
  if (fields.capacity() < 16) {
    fields.reserve(16);
  }
  if (root.is_object()) {
    SAN_RETURN_NOT_OK(
        root.for_each_object_field([&](std::string_view key, uint64_t key_hash,
                                       ValueView value) -> Status {
          if (fields.size() >= kMaxMaterializedFieldsPerRow) {
            return Status::Invalid(
                "JSON object field count exceeds safety limit: ",
                fields.size() + 1U, " > ", kMaxMaterializedFieldsPerRow);
          }
          fields.push_back(sanitize::FieldRef{
              .key = key, .key_hash = key_hash, .value = value});
          return Status::OK();
        }));
  } else {
    fields.push_back(sanitize::FieldRef{
        .key = opts.spec.default_key_name, .key_hash = 0, .value = root});
  }
  const sanitize::RowRef row{
      .fields = fields.empty() ? nullptr : fields.data(),
      .size = fields.size(),
      .raw = raw,
      .base_offset = base_offset,
      .source_file = source_file,
  };
  return append_materialized_row_reuse(app, row, opts, diagnostics);
}

} // namespace sanitize::internal

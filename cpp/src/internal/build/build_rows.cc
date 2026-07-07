// Row adapters and materialized-row ingestion.

#include "internal/build/build_internal.hh"
#include "internal/build/build_variant_routing.hh"
#include "internal/planning/variant_field_names.hh"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "internal/core/value_view_util.hh"
#include "internal/memory/arena.hh"
#include "internal/parsing/csv_direct.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/json_ondemand.hh"
#include "sanitize/core/value_view.hh"

namespace sanitize::internal {
namespace {

using sanitize::DiagnosticCode;
using sanitize::PreparedOptions;
using sanitize::Status;
using sanitize::ValueView;

// Returns whether root row conversion benefits from source-field
// pre-resolution.
bool should_snapshot_root_fields(const sanitize::CompiledPlan &plan) noexcept {
  constexpr std::size_t kWideRootThreshold = 8;
  if (plan.columns.size() >= kWideRootThreshold) {
    return true;
  }
  for (const auto &column : plan.columns) {
    if (column.has_variant_sibling) {
      return true;
    }
  }
  return false;
}

// Performs the result for policy operation.
AppendRowResult result_for_policy(BatchAppender *app,
                                  const PreparedOptions &opts,
                                  sanitize::IngestDiagnostics *diag,
                                  const CoerceError &err) {
  AppendRowResult result;
  result.code = err.code;
  result.path_id = err.path_id;
  result.detail = err.detail;
  switch (opts.spec.on_error) {
  case sanitize::OnErrorPolicy::kStop:
    break;
  case sanitize::OnErrorPolicy::kSkipRow:
    if (diag)
      diag->skipped_rows += 1;
    result.code = DiagnosticCode::kRowSkipped;
    return result;
  case sanitize::OnErrorPolicy::kEmitNullRow:
    if (app)
      (void)app->append_null_row();
    return AppendRowResult{};
  }
  return result;
}

} // namespace

sanitize::Result<AppendRowResult>
append_materialized_row(BatchAppender *app, const sanitize::RowRef &row,
                        const PreparedOptions &opts,
                        sanitize::IngestDiagnostics *diagnostics) {
  if (!app)
    return Status::Invalid("append_row: app is null");

  const auto &plan = app->plan();
  FieldLookup lookup{&row};
  RowFieldSnapshot snapshot;
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

  std::vector<Cell> cells;
  cells.resize(plan.columns.size());
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
        std::string_view original = unflattened_name(column.name);
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
    SAN_RETURN_NOT_OK(value_view_container_is_empty(value, &empty_container));
    if (empty_container)
      value = ValueView::Null();

    Status st = Status::OK();
    if (has_variant && found && !value.is_null() &&
        preferred_root_variant_sibling(plan, column, value, opts) != &column) {
      if (ctx.error)
        *ctx.error = CoerceError{};
      st = convert_null(column, &cells[i]);
    } else {
      st = (!found || value.is_null())
               ? convert_null(column, &cells[i])
               : convert_value(column, value, ctx, &cells[i]);
    }
    if (!st.ok()) {
      if (has_variant) {
        if (ctx.error)
          *ctx.error = CoerceError{};
        SAN_RETURN_NOT_OK(convert_null(column, &cells[i]));
        continue;
      }
      if (err.detail.empty()) {
        err.code = DiagnosticCode::kTypeMismatch;
        err.path_id = static_cast<uint32_t>(column.path_id);
        err.detail = st.message();
      }
      if (opts.spec.on_error == sanitize::OnErrorPolicy::kStop)
        return Status::Invalid(err.detail);
      return result_for_policy(app, opts, diagnostics, err);
    }
  }

  SAN_RETURN_NOT_OK(app->append_cells(std::move(cells)));
  return AppendRowResult{};
}

sanitize::Result<AppendRowResult>
append_row_json_text(BatchAppender *app, JsonOnDemandDoc *doc,
                     std::string_view raw, std::size_t base_offset,
                     std::string_view source_file, const PreparedOptions &opts,
                     sanitize::IngestDiagnostics *diagnostics) {
  if (!doc)
    return Status::Invalid("append_row_json_text: doc is null");
  doc->Reset();
  SAN_ASSIGN_OR_RAISE(ValueView root, doc->ParseValue(raw, base_offset));

  std::vector<sanitize::FieldRef> fields;
  if (root.is_object()) {
    SAN_RETURN_NOT_OK(
        root.for_each_object_field([&](std::string_view key, uint64_t key_hash,
                                       ValueView value) -> Status {
          fields.push_back(sanitize::FieldRef{
              .key = key,
              .key_hash = key_hash,
              .value = value,
          });
          return Status::OK();
        }));
  } else {
    const auto &default_key = opts.spec.default_key_name;
    fields.push_back(sanitize::FieldRef{
        .key = default_key,
        .key_hash = 0,
        .value = root,
    });
  }

  sanitize::RowRef row;
  row.fields = fields.empty() ? nullptr : fields.data();
  row.size = fields.size();
  row.raw = raw;
  row.base_offset = base_offset;
  row.source_file = source_file;
  return append_materialized_row(app, row, opts, diagnostics);
}

sanitize::Result<AppendRowResult>
append_row_csv_text(BatchAppender *app, const CsvDirectContext &ctx,
                    BumpArena *arena, std::vector<std::string_view> *cells,
                    std::string_view raw, std::size_t base_offset,
                    std::string_view source_file, const PreparedOptions &opts,
                    sanitize::IngestDiagnostics *diagnostics) {
  if (!app)
    return Status::Invalid("append_row_csv_text: app is null");
  if (!cells)
    return Status::Invalid("append_row_csv_text: cells is null");
  if (arena)
    arena->reset();
  parse_csv_cells(raw, ctx.delimiter, cells, arena);

  const auto &plan = app->plan();
  std::vector<sanitize::FieldRef> fields;
  fields.reserve(plan.columns.size());
  for (std::size_t i = 0; i < plan.columns.size(); ++i) {
    int32_t csv_idx = -1;
    if (i < ctx.col_to_csv.size())
      csv_idx = ctx.col_to_csv[i];
    if (csv_idx < 0 || static_cast<std::size_t>(csv_idx) >= cells->size())
      continue;
    std::string_view cell = (*cells)[static_cast<std::size_t>(csv_idx)];
    ValueView value =
        cell.empty() ? ValueView::Null() : ValueView::String(cell);
    fields.push_back(sanitize::FieldRef{
        .key = plan.columns[i].name,
        .key_hash = 0,
        .value = value,
    });
  }

  sanitize::RowRef row;
  row.fields = fields.empty() ? nullptr : fields.data();
  row.size = fields.size();
  row.raw = raw;
  row.base_offset = base_offset;
  row.source_file = source_file;
  return append_materialized_row(app, row, opts, diagnostics);
}

} // namespace sanitize::internal

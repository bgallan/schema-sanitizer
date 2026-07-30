// Appends flat scalar rows directly into Arrow builders without owning text.

#include "internal/materialization/batch_appender_internal.hh"
#include "internal/materialization/conversion/detail.hh"

#include <optional>
#include <string>

#include "sanitize/core/logical_schema.hh"

namespace sanitize::internal {

sanitize::Result<std::optional<AppendRowResult>>
handle_direct_scalar_conversion_error(BatchAppender *app,
                                      const sanitize::PreparedOptions &opts,
                                      sanitize::IngestDiagnostics *diagnostics,
                                      const CoerceError &error,
                                      const sanitize::Status &status) {
  if (opts.spec.on_error == sanitize::OnErrorPolicy::kStop) {
    return sanitize::Status::Invalid(error.detail.empty() ? status.message()
                                                          : error.detail);
  }

  AppendRowResult result;
  result.code = error.code;
  result.path_id = error.path_id;
  result.detail = error.detail.empty() ? status.message() : error.detail;
  if (opts.spec.on_error == sanitize::OnErrorPolicy::kSkipRow) {
    if (diagnostics) {
      diagnostics->skipped_rows += 1;
    }
    result.code = sanitize::DiagnosticCode::kRowSkipped;
    return std::optional<AppendRowResult>(std::move(result));
  }

  SAN_RETURN_NOT_OK(app->append_null_row());
  return std::optional<AppendRowResult>(AppendRowResult{});
}

sanitize::Result<std::optional<AppendRowResult>>
try_append_direct_scalar_row(BatchAppender *app, const sanitize::RowRef &row,
                             const sanitize::PreparedOptions &opts,
                             sanitize::IngestDiagnostics *diagnostics) {
  if (!app) {
    return sanitize::Status::Invalid(
        "try_append_direct_scalar_row: app is null");
  }
  if (!app->supports_direct_scalar_rows()) {
    return std::optional<AppendRowResult>{};
  }

  FieldLookup lookup{&row};
  if (opts.spec.arrow_schema_contract &&
      opts.spec.schema_evolution == sanitize::SchemaEvolutionMode::kStrict) {
    std::string extra;
    SAN_ASSIGN_OR_RAISE(
        bool has_unplanned,
        lookup.has_unplanned_field(app->plan().root_layout, opts, &extra));
    if (has_unplanned) {
      return sanitize::Status::Invalid(
          "Strict schema evolution: observed extra field '" + extra + "'");
    }
  }

  RowFieldSnapshot &snapshot = app->prepare_row_snapshot();
  SAN_RETURN_NOT_OK(snapshot.build(row, app->plan(), opts));
  auto &values = app->prepare_direct_scalars();
  CoerceError error;
  ConvertCtx ctx{
      .opts = opts,
      .diagnostics = diagnostics,
      .error = &error,
  };

  for (std::size_t index = 0; index < app->plan().columns.size(); ++index) {
    const auto &column = app->plan().columns[index];
    bool found = false;
    sanitize::ValueView value = sanitize::ValueView::Null();
    if (column.name == "source_file" && !row.source_file.empty()) {
      value = sanitize::ValueView::String(row.source_file);
      found = true;
    } else {
      found = snapshot.find(index, &value);
    }

    bool empty_container = false;
    SAN_RETURN_NOT_OK(value.container_is_empty(&empty_container));
    if (!found || empty_container || value.is_null()) {
      values[index].reset(column.logical_type.kind);
      continue;
    }

    const sanitize::Status status =
        convert_direct_scalar(column, value, ctx, &values[index]);
    if (status.ok()) {
      continue;
    }
    if (error.detail.empty()) {
      error.code = sanitize::DiagnosticCode::kTypeMismatch;
      error.path_id = static_cast<uint32_t>(column.path_id);
      error.detail = status.message();
    }
    return handle_direct_scalar_conversion_error(app, opts, diagnostics, error,
                                                 status);
  }

  SAN_RETURN_NOT_OK(app->append_direct_scalars());
  return std::optional<AppendRowResult>(AppendRowResult{});
}

} // namespace sanitize::internal

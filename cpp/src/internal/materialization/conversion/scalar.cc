// Converts scalar ValueView inputs into typed in-memory cells.
//
// Handles primitive coercion, temporal scalar parsing, UTF-8 fallback
// formatting, and shared conversion-error recording for build conversion.

#include "internal/materialization/conversion/detail.hh"

#include "internal/materialization/conversion/scalar_text.hh"

#include <cstdint>
#include <limits>
#include <string>
#include <utility>

#include "internal/parsing/string_scalar.hh"
#include "sanitize/core/logical_schema.hh"

namespace sanitize::internal {
namespace {

using sanitize::ColumnPlan;
using sanitize::DiagnosticCode;
using sanitize::LogicalKind;
using sanitize::Status;
using sanitize::ValueView;

// clang-format off
// Converts a value to a bool cell.
Status convert_bool_scalar(const ColumnPlan &plan, ValueView value,
                           ConvertCtx &ctx, Cell *cell) {
  if (value.is_bool()) {
    cell->b = value.as_bool();
    return Status::OK();
  }
  if (value.is_string()) {
    bool parsed = false;
    if (!coerce_bool_from_string(value.as_string_view(), ctx.opts, &parsed)) {
      return set_conversion_error(
          ctx, plan, DiagnosticCode::kCoercionFailure,
          "failed to coerce string to bool for field '" + plan.name + "'");
    }
    cell->b = parsed;
    return Status::OK();
  }
  return set_conversion_error(ctx, plan, DiagnosticCode::kTypeMismatch,
                              "expected bool for field '" + plan.name + "'");
}

// Converts a value to an int64 cell.
Status convert_int64_scalar(const ColumnPlan &plan, ValueView value,
                            ConvertCtx &ctx, Cell *cell) {
  if (value.is_int()) {
    cell->i64 = value.as_int();
    return Status::OK();
  }
  if (value.is_string()) {
    int64_t parsed = 0;
    if (!coerce_int64_from_string(value.as_string_view(), ctx.opts, &parsed)) {
      return set_conversion_error(
          ctx, plan, DiagnosticCode::kCoercionFailure,
          "failed to coerce string to int64 for field '" + plan.name + "'");
    }
    cell->i64 = parsed;
    return Status::OK();
  }
  return set_conversion_error(ctx, plan, DiagnosticCode::kTypeMismatch,
                              "expected int64 for field '" + plan.name + "'");
}

// Converts a value to a float64 cell.
Status convert_float64_scalar(const ColumnPlan &plan, ValueView value,
                              ConvertCtx &ctx, Cell *cell) {
  if (value.is_float()) {
    cell->f64 = value.as_float();
    return Status::OK();
  }
  if (value.is_int()) {
    cell->f64 = static_cast<double>(value.as_int());
    return Status::OK();
  }
  if (value.is_string()) {
    double parsed = 0.0;
    if (!coerce_float64_from_string(value.as_string_view(), ctx.opts,
                                    &parsed)) {
      return set_conversion_error(
          ctx, plan, DiagnosticCode::kCoercionFailure,
          "failed to coerce string to float64 for field '" + plan.name + "'");
    }
    cell->f64 = parsed;
    return Status::OK();
  }
  return set_conversion_error(ctx, plan, DiagnosticCode::kTypeMismatch,
                              "expected float64 for field '" + plan.name + "'");
}
// Returns whether a signed integer fits Arrow int32-style logical types.
bool fits_int32(int64_t value) noexcept {
  return value >= std::numeric_limits<int32_t>::min() &&
         value <= std::numeric_limits<int32_t>::max();
}

// Returns the divisor that scales parsed nanoseconds to the configured output
// timestamp unit.
int64_t timestamp_precision_divisor(const PreparedOptions &opts) {
  const std::string &precision = opts.spec.timestamp_precision;
  if (precision == "TIMESTAMP_MILLIS")
    return 1000000;
  if (precision == "TIMESTAMP_MICROS")
    return 1000;
  return 1;
}

// Converts a value to a timestamp cell in the configured output unit.
Status convert_timestamp_scalar(const ColumnPlan &plan, ValueView value,
                                ConvertCtx &ctx, Cell *cell) {
  if (value.is_int()) {
    cell->i64 = value.as_int();
    return Status::OK();
  }
  if (value.is_string()) {
    int64_t parsed = 0;
    if (!coerce_timestamp_ns_from_string(value.as_string_view(), ctx.opts,
                                         &parsed)) {
      return set_conversion_error(
          ctx, plan, DiagnosticCode::kCoercionFailure,
          "failed to coerce string to timestamp for field '" + plan.name + "'");
    }
    cell->i64 = parsed / timestamp_precision_divisor(ctx.opts);
    return Status::OK();
  }
  return set_conversion_error(ctx, plan, DiagnosticCode::kTypeMismatch,
                              "expected timestamp for field '" + plan.name +
                                  "'");
}

// Converts a value to a date32 cell.
Status convert_date32_scalar(const ColumnPlan &plan, ValueView value,
                             ConvertCtx &ctx, Cell *cell) {
  if (value.is_int()) {
    int64_t candidate = value.as_int();
    if (!fits_int32(candidate)) {
      return set_conversion_error(ctx, plan, DiagnosticCode::kCoercionFailure,
                                  "date32 integer out of range for field '" +
                                      plan.name + "'");
    }
    cell->i64 = candidate;
    return Status::OK();
  }
  if (value.is_string()) {
    int32_t parsed = 0;
    if (!coerce_date_days_from_string(value.as_string_view(), ctx.opts,
                                      &parsed)) {
      return set_conversion_error(
          ctx, plan, DiagnosticCode::kCoercionFailure,
          "failed to coerce string to date32 for field '" + plan.name + "'");
    }
    cell->i64 = parsed;
    return Status::OK();
  }
  return set_conversion_error(ctx, plan, DiagnosticCode::kTypeMismatch,
                              "expected date32 for field '" + plan.name + "'");
}

// Converts a value to a time32[s] cell.
Status convert_time32_scalar(const ColumnPlan &plan, ValueView value,
                             ConvertCtx &ctx, Cell *cell) {
  if (value.is_int()) {
    int64_t candidate = value.as_int();
    if (!fits_int32(candidate)) {
      return set_conversion_error(ctx, plan, DiagnosticCode::kCoercionFailure,
                                  "time32 integer out of range for field '" +
                                      plan.name + "'");
    }
    cell->i64 = candidate;
    return Status::OK();
  }
  if (value.is_string()) {
    int32_t parsed = 0;
    if (!coerce_time_seconds_from_string(value.as_string_view(), ctx.opts,
                                         &parsed)) {
      return set_conversion_error(
          ctx, plan, DiagnosticCode::kCoercionFailure,
          "failed to coerce string to time32[s] for field '" + plan.name + "'");
    }
    cell->i64 = parsed;
    return Status::OK();
  }
  return set_conversion_error(ctx, plan, DiagnosticCode::kTypeMismatch,
                              "expected time32[s] for field '" + plan.name +
                                  "'");
}
// clang-format on

// Converts scalar value kinds into a non-null cell.
Status convert_scalar_cell(const ColumnPlan &plan, ValueView value,
                           ConvertCtx &ctx, Cell *cell) {
  switch (plan.logical_type.kind) {
  case LogicalKind::kNull:
    *cell = Cell::Null(LogicalKind::kNull);
    return Status::OK();
  case LogicalKind::kBool:
    return convert_bool_scalar(plan, value, ctx, cell);
  case LogicalKind::kInt64:
    return convert_int64_scalar(plan, value, ctx, cell);
  case LogicalKind::kFloat64:
    return convert_float64_scalar(plan, value, ctx, cell);
  case LogicalKind::kUtf8:
    cell->str = value_to_scalar_string(value);
    return Status::OK();
  case LogicalKind::kTimestampNs:
    return convert_timestamp_scalar(plan, value, ctx, cell);
  case LogicalKind::kDate32:
    return convert_date32_scalar(plan, value, ctx, cell);
  case LogicalKind::kTime32s:
    return convert_time32_scalar(plan, value, ctx, cell);
  case LogicalKind::kStruct:
  case LogicalKind::kList:
    return Status::Invalid("convert_scalar called with nested logical type");
  }
  return Status::Invalid("convert_scalar: unknown logical type");
}

} // namespace

Status set_conversion_error(ConvertCtx &ctx, const ColumnPlan &plan,
                            DiagnosticCode code, std::string detail) {
  if (ctx.error && ctx.error->detail.empty()) {
    ctx.error->code = code;
    ctx.error->path_id = static_cast<uint32_t>(plan.path_id);
    ctx.error->detail = std::move(detail);
  }
  return Status::Invalid(ctx.error ? ctx.error->detail : "conversion failed");
}

Status convert_scalar(const ColumnPlan &plan, ValueView value, ConvertCtx &ctx,
                      Cell *out) {
  if (!out) {
    return Status::Invalid("convert_scalar: out is null");
  }
  if (value.is_null()) {
    return convert_null(plan, out);
  }

  Cell cell;
  cell.is_null = false;
  cell.kind = plan.logical_type.kind;
  SAN_RETURN_NOT_OK(convert_scalar_cell(plan, value, ctx, &cell));

  *out = std::move(cell);
  return Status::OK();
}

Status convert_direct_scalar(const ColumnPlan &plan, ValueView value,
                             ConvertCtx &ctx, DirectScalarValue *out) {
  if (!out) {
    return Status::Invalid("convert_direct_scalar: out is null");
  }
  out->reset(plan.logical_type.kind);
  if (value.is_null()) {
    return Status::OK();
  }
  if (plan.logical_type.kind == LogicalKind::kStruct ||
      plan.logical_type.kind == LogicalKind::kList) {
    return Status::Invalid(
        "convert_direct_scalar called with nested logical type");
  }

  out->is_null = false;
  if (plan.logical_type.kind == LogicalKind::kUtf8 && value.is_string()) {
    out->borrows_utf8 = true;
    out->borrowed_utf8 = value.as_string_view();
    return Status::OK();
  }

  Cell converted;
  SAN_RETURN_NOT_OK(convert_scalar(plan, value, ctx, &converted));
  out->is_null = converted.is_null;
  out->b = converted.b;
  out->i64 = converted.i64;
  out->f64 = converted.f64;
  if (plan.logical_type.kind == LogicalKind::kUtf8) {
    out->owned_utf8 = std::move(converted.str);
  }
  return Status::OK();
}

} // namespace sanitize::internal

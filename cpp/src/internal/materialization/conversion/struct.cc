// Converts scalar and object inputs into STRUCT materialization cells.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#include "internal/materialization/conversion/struct.hh"

#include <string>
#include <string_view>
#include <utility>

#include "internal/materialization/conversion/detail.hh"
#include "internal/materialization/conversion/object_struct/api.hh"
#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/planned_name_matcher.hh"
#include "sanitize/detail/hash.hh"

namespace sanitize::internal {
namespace {

using sanitize::ColumnPlan;
using sanitize::DiagnosticCode;
using sanitize::LogicalKind;
using sanitize::Status;
using sanitize::ValueView;

/// Returns whether a struct has a child matching the default scalar key.
bool has_default_key_child(const ColumnPlan &plan, std::string_view default_key,
                           const sanitize::PreparedOptions &opts) {
  if (plan.layout &&
      find_planned_field(*plan.layout, default_key,
                         sanitize::detail::hash_key64(default_key),
                         opts) != nullptr) {
    return true;
  }
  for (const auto &child : plan.children) {
    if (field_name_matches_output(default_key, child.name, opts)) {
      return true;
    }
  }
  return false;
}

/// Converts a scalar value by wrapping it into a struct default-key child.
Status convert_wrapped_struct(const ColumnPlan &plan, ValueView value,
                              ConvertCtx &ctx, Cell *cell) {
  const std::string &default_key = ctx.opts.spec.default_key_name;
  if (!has_default_key_child(plan, default_key, ctx.opts)) {
    return set_conversion_error(ctx, plan, DiagnosticCode::kTypeMismatch,
                                "expected struct for field '" + plan.name +
                                    "'");
  }

  if (ctx.diagnostics) {
    ctx.diagnostics->scalar_wrappings += 1;
  }
  for (std::size_t i = 0; i < plan.children.size(); ++i) {
    const auto &child = plan.children[i];
    if (field_name_matches_output(default_key, child.name, ctx.opts)) {
      SAN_RETURN_NOT_OK(convert_value(child, value, ctx, &cell->children[i]));
    } else {
      SAN_RETURN_NOT_OK(convert_null(child, &cell->children[i]));
    }
  }
  return Status::OK();
}

} // namespace

Status convert_struct(const ColumnPlan &plan, ValueView value, ConvertCtx &ctx,
                      Cell *out) {
  if (!out) {
    return Status::Invalid("convert_struct: out is null");
  }
  if (value.is_null()) {
    return convert_null(plan, out);
  }
  bool empty_container = false;
  SAN_RETURN_NOT_OK(value.container_is_empty(&empty_container));
  if (empty_container) {
    return convert_null(plan, out);
  }

  Cell cell;
  cell.is_null = false;
  cell.kind = LogicalKind::kStruct;
  cell.children.resize(plan.children.size());

  if (value.is_object()) {
    SAN_RETURN_NOT_OK(convert_object_struct(plan, value, ctx, &cell));
    *out = std::move(cell);
    return Status::OK();
  }

  SAN_RETURN_NOT_OK(convert_wrapped_struct(plan, value, ctx, &cell));
  *out = std::move(cell);
  return Status::OK();
}

} // namespace sanitize::internal

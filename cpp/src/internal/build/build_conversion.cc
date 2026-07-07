// Converts public ValueView inputs into typed in-memory cells.

#include "internal/build/build_conversion_detail.hh"
#include "internal/build/build_internal.hh"
#include "internal/build/build_struct_conversion.hh"

#include "internal/core/value_view_util.hh"

#include <utility>

#include "sanitize/core/logical_schema.hh"

namespace sanitize::internal {
namespace {

using sanitize::ColumnPlan;
using sanitize::DiagnosticCode;
using sanitize::LogicalKind;
using sanitize::Status;
using sanitize::ValueView;

// Converts arrays and scalar fallbacks into a typed list cell.
Status convert_list(const ColumnPlan &plan, ValueView value, ConvertCtx &ctx,
                    Cell *out) {
  if (!out)
    return Status::Invalid("convert_list: out is null");
  if (value.is_null())
    return convert_null(plan, out);
  if (!plan.value)
    return set_conversion_error(ctx, plan, DiagnosticCode::kTypeMismatch,
                                "list field '" + plan.name +
                                    "' has no element plan");

  Cell cell;
  cell.is_null = false;
  cell.kind = LogicalKind::kList;

  if (value.is_array()) {
    auto st = value.for_each_array_element([&](ValueView element) -> Status {
      Cell element_cell;
      SAN_RETURN_NOT_OK(
          convert_value(*plan.value, element, ctx, &element_cell));
      cell.elements.push_back(std::move(element_cell));
      return Status::OK();
    });
    if (!st.ok())
      return st;
  } else {
    if (ctx.diagnostics)
      ctx.diagnostics->scalar_wrappings += 1;
    Cell element_cell;
    SAN_RETURN_NOT_OK(convert_value(*plan.value, value, ctx, &element_cell));
    cell.elements.push_back(std::move(element_cell));
  }

  *out = std::move(cell);
  return Status::OK();
}

} // namespace

Status convert_null(const ColumnPlan &plan, Cell *out) {
  if (!out)
    return Status::Invalid("convert_null: out is null");
  *out = Cell::Null(plan.logical_type.kind);
  if (plan.logical_type.kind == LogicalKind::kStruct) {
    out->children.resize(plan.children.size());
    for (std::size_t i = 0; i < plan.children.size(); ++i)
      SAN_RETURN_NOT_OK(convert_null(plan.children[i], &out->children[i]));
  } else if (plan.logical_type.kind == LogicalKind::kList && plan.value) {
    out->elements.clear();
  }
  return Status::OK();
}

Status convert_value(const ColumnPlan &plan, ValueView value, ConvertCtx &ctx,
                     Cell *out) {
  bool empty_container = false;
  SAN_RETURN_NOT_OK(value_view_container_is_empty(value, &empty_container));
  if (empty_container)
    return convert_null(plan, out);

  switch (plan.logical_type.kind) {
  case LogicalKind::kStruct:
    return convert_struct(plan, value, ctx, out);
  case LogicalKind::kList:
    return convert_list(plan, value, ctx, out);
  default:
    return convert_scalar(plan, value, ctx, out);
  }
}

} // namespace sanitize::internal

// Materializes object-like inputs into STRUCT cells.

#include "internal/materialization/conversion/object_struct/api.hh"

#include <string>

#include "internal/materialization/conversion/detail.hh"
#include "internal/materialization/conversion/object_fields.hh"
#include "internal/materialization/conversion/object_struct/fields.hh"
#include "internal/materialization/conversion/variants.hh"

namespace sanitize::internal {

using sanitize::ColumnPlan;
using sanitize::DiagnosticCode;
using sanitize::Status;
using sanitize::ValueView;

Status convert_object_struct(const ColumnPlan &plan, ValueView value,
                             ConvertCtx &ctx, Cell *cell) {
  using object_struct_detail::find_object_child_value;
  using object_struct_detail::find_strict_extra_field;
  using object_struct_detail::should_check_strict_struct;
  using object_struct_detail::should_snapshot_object_fields;

  ObjectFieldSnapshot snapshot;
  const bool use_snapshot = should_snapshot_object_fields(plan);
  if (use_snapshot) {
    SAN_RETURN_NOT_OK(snapshot.build(value, plan, ctx.opts));
  }

  if (should_check_strict_struct(plan, ctx)) {
    std::string extra;
    SAN_RETURN_NOT_OK(
        use_snapshot ? find_strict_extra_field(plan, snapshot, ctx.opts, &extra)
                     : find_strict_extra_field(plan, value, ctx.opts, &extra));
    if (!extra.empty()) {
      return set_conversion_error(ctx, plan, DiagnosticCode::kTypeMismatch,
                                  "unexpected field '" + extra +
                                      "' in strict struct '" + plan.name + "'");
    }
  }

  for (std::size_t i = 0; i < plan.children.size(); ++i) {
    const auto &child = plan.children[i];
    const bool has_variant = child.has_variant_sibling;
    ValueView child_value = ValueView::Null();
    bool found = false;
    if (use_snapshot) {
      found = find_object_child_value(child, snapshot, i, ctx, &child_value);
    } else {
      SAN_RETURN_NOT_OK(
          find_object_child_value(child, value, ctx, &child_value, &found));
    }
    bool empty_container = false;
    SAN_RETURN_NOT_OK(child_value.container_is_empty(&empty_container));
    if (empty_container) {
      child_value = ValueView::Null();
    }

    if (has_variant && found && !child_value.is_null() &&
        preferred_child_variant_sibling(plan, child, child_value, ctx.opts) !=
            &child) {
      if (ctx.error) {
        *ctx.error = CoerceError{};
      }
      SAN_RETURN_NOT_OK(convert_null(child, &cell->children[i]));
      continue;
    }
    Status child_status =
        (!found || child_value.is_null())
            ? convert_null(child, &cell->children[i])
            : convert_value(child, child_value, ctx, &cell->children[i]);
    if (!child_status.ok() && has_variant) {
      if (ctx.error) {
        *ctx.error = CoerceError{};
      }
      SAN_RETURN_NOT_OK(convert_null(child, &cell->children[i]));
      continue;
    }
    SAN_RETURN_NOT_OK(child_status);
  }
  return Status::OK();
}

} // namespace sanitize::internal

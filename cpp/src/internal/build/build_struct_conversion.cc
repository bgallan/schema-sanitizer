// Implements STRUCT conversion for Arrow C Data materialization.

#include "internal/build/build_struct_conversion.hh"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/build/build_conversion_detail.hh"
#include "internal/build/build_object_field_snapshot.hh"
#include "internal/build/build_variant_routing.hh"
#include "internal/core/value_view_util.hh"
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

/// Finds a non-empty field in an object view.
Status object_find(ValueView object, std::string_view key,
                   const sanitize::PreparedOptions &opts, ValueView *out,
                   bool *found) {
  *found = false;
  if (!object.is_object()) {
    return Status::OK();
  }
  return object.for_each_object_field([&](std::string_view candidate, uint64_t,
                                          ValueView value) -> Status {
    if (!*found && field_name_matches_output(candidate, key, opts)) {
      bool empty_container = false;
      SAN_RETURN_NOT_OK(value_view_container_is_empty(value, &empty_container));
      if (empty_container) {
        return Status::OK();
      }
      *found = true;
      if (out) {
        *out = value;
      }
    }
    return Status::OK();
  });
}

/// Returns whether a struct conversion benefits from a source-field snapshot.
bool should_snapshot_object_fields(const ColumnPlan &plan) noexcept {
  constexpr std::size_t kWideStructThreshold = 8;
  if (plan.children.size() >= kWideStructThreshold) {
    return true;
  }
  for (const auto &child : plan.children) {
    if (child.has_variant_sibling) {
      return true;
    }
  }
  return false;
}

/// Returns whether strict struct extra-field checks apply.
bool should_check_strict_struct(const ColumnPlan &plan,
                                const ConvertCtx &ctx) noexcept {
  return ctx.opts.spec.arrow_schema_contract &&
         ctx.opts.spec.schema_evolution ==
             sanitize::SchemaEvolutionMode::kStrict &&
         plan.layout;
}

/// Returns the first object field missing from a strict struct layout.
Status find_strict_extra_field(const ColumnPlan &plan, ValueView value,
                               const sanitize::PreparedOptions &opts,
                               std::string *extra) {
  auto st = value.for_each_object_field([&](std::string_view key, uint64_t hash,
                                            ValueView child_value) -> Status {
    if (!extra->empty()) {
      return Status::OK();
    }
    bool empty_container = false;
    SAN_RETURN_NOT_OK(
        value_view_container_is_empty(child_value, &empty_container));
    if (empty_container) {
      return Status::OK();
    }
    if (!matches_planned_field(*plan.layout, key, hash, opts)) {
      extra->assign(key.data(), key.size());
    }
    return Status::OK();
  });
  return st;
}

/// Returns the first snapshotted field missing from a strict struct layout.
Status find_strict_extra_field(const ColumnPlan &plan,
                               const ObjectFieldSnapshot &snapshot,
                               const sanitize::PreparedOptions &opts,
                               std::string *extra) {
  for (const auto &field : snapshot.fields) {
    if (matches_planned_field(*plan.layout, field.key, field.key_hash, opts)) {
      continue;
    }
    bool empty_container = false;
    SAN_RETURN_NOT_OK(
        value_view_container_is_empty(field.value, &empty_container));
    if (!empty_container) {
      extra->assign(field.key.data(), field.key.size());
      return Status::OK();
    }
  }
  return Status::OK();
}

/// Converts one planned struct child from an object view.
Status find_object_child_value(const ColumnPlan &child, ValueView object,
                               ConvertCtx &ctx, ValueView *child_value,
                               bool *found) {
  SAN_RETURN_NOT_OK(
      object_find(object, child.name, ctx.opts, child_value, found));
  if (!*found) {
    std::string_view original = unflattened_name(child.name);
    if (!original.empty()) {
      SAN_RETURN_NOT_OK(
          object_find(object, original, ctx.opts, child_value, found));
    }
  }
  return Status::OK();
}

/// Finds one planned struct child in a precomputed object snapshot.
bool find_object_child_value(const ColumnPlan &child,
                             const ObjectFieldSnapshot &snapshot,
                             std::size_t child_index, ConvertCtx &ctx,
                             ValueView *child_value) {
  bool found = snapshot.find(child_index, child.name, ctx.opts, child_value);
  if (!found) {
    std::string_view original = unflattened_name(child.name);
    if (!original.empty()) {
      found = snapshot.find(child_index, original, ctx.opts, child_value);
    }
  }
  return found;
}

/// Converts a JSON/object-like value into a struct cell.
Status convert_object_struct(const ColumnPlan &plan, ValueView value,
                             ConvertCtx &ctx, Cell *cell) {
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
    SAN_RETURN_NOT_OK(
        value_view_container_is_empty(child_value, &empty_container));
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

/// Converts one value into a STRUCT materialization cell.
Status convert_struct(const ColumnPlan &plan, ValueView value, ConvertCtx &ctx,
                      Cell *out) {
  if (!out) {
    return Status::Invalid("convert_struct: out is null");
  }
  if (value.is_null()) {
    return convert_null(plan, out);
  }
  bool empty_container = false;
  SAN_RETURN_NOT_OK(value_view_container_is_empty(value, &empty_container));
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

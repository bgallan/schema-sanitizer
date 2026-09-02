// Resolves fields and strict-schema checks for object STRUCT conversion.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#include "internal/materialization/conversion/object_struct/fields.hh"

#include <string_view>

#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/planned_name_matcher.hh"

namespace sanitize::internal::object_struct_detail {
namespace {

using sanitize::ColumnPlan;
using sanitize::Status;
using sanitize::ValueView;

/// Visits an object once to locate a field by hash and exact key bytes.
Status object_find(ValueView object, std::string_view key,
                   const sanitize::PreparedOptions &opts, ValueView *out,
                   bool *found) {
  *found = false;
  if (!object.is_object()) {
    return Status::OK();
  }
  return object.for_each_object_field(
      [&](std::string_view candidate, uint64_t, ValueView value) -> Status {
        if (!*found && field_name_matches_output(candidate, key, opts)) {
          bool empty_container = false;
          SAN_RETURN_NOT_OK(value.container_is_empty(&empty_container));
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

} // namespace

/// Reports whether a wide object should be indexed once before STRUCT
/// conversion.
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

/// Reports whether strict STRUCT conversion must search for unexpected object
/// fields.
bool should_check_strict_struct(const ColumnPlan &plan,
                                const ConvertCtx &ctx) noexcept {
  return ctx.opts.spec.arrow_schema_contract &&
         ctx.opts.spec.schema_evolution ==
             sanitize::SchemaEvolutionMode::kStrict &&
         plan.layout;
}

/// Locates strict extra field without allocating and preserves the caller's
/// source indexing.
Status find_strict_extra_field(const ColumnPlan &plan, ValueView value,
                               const sanitize::PreparedOptions &opts,
                               std::string *extra) {
  return value.for_each_object_field([&](std::string_view key, uint64_t hash,
                                         ValueView child_value) -> Status {
    if (!extra->empty()) {
      return Status::OK();
    }
    bool empty_container = false;
    SAN_RETURN_NOT_OK(child_value.container_is_empty(&empty_container));
    if (!empty_container &&
        !matches_planned_field(*plan.layout, key, hash, opts)) {
      extra->assign(key.data(), key.size());
    }
    return Status::OK();
  });
}

/// Locates strict extra field without allocating and preserves the caller's
/// source indexing.
Status find_strict_extra_field(const ColumnPlan &plan,
                               const ObjectFieldSnapshot &snapshot,
                               const sanitize::PreparedOptions &opts,
                               std::string *extra) {
  for (const auto &field : snapshot.fields) {
    if (matches_planned_field(*plan.layout, field.key, field.key_hash, opts)) {
      continue;
    }
    bool empty_container = false;
    SAN_RETURN_NOT_OK(field.value.container_is_empty(&empty_container));
    if (!empty_container) {
      extra->assign(field.key.data(), field.key.size());
      return Status::OK();
    }
  }
  return Status::OK();
}

/// Locates object child value without allocating and preserves the caller's
/// source indexing.
Status find_object_child_value(const ColumnPlan &child, ValueView object,
                               ConvertCtx &ctx, ValueView *child_value,
                               bool *found) {
  SAN_RETURN_NOT_OK(
      object_find(object, child.name, ctx.opts, child_value, found));
  if (!*found) {
    const std::string_view original = unflattened_name(child.name);
    if (!original.empty()) {
      SAN_RETURN_NOT_OK(
          object_find(object, original, ctx.opts, child_value, found));
    }
  }
  return Status::OK();
}

/// Locates object child value without allocating and preserves the caller's
/// source indexing.
bool find_object_child_value(const ColumnPlan &child,
                             const ObjectFieldSnapshot &snapshot,
                             std::size_t child_index, ConvertCtx &ctx,
                             ValueView *child_value) {
  bool found = snapshot.find(child_index, child.name, ctx.opts, child_value);
  if (!found) {
    const std::string_view original = unflattened_name(child.name);
    if (!original.empty()) {
      found = snapshot.find(child_index, original, ctx.opts, child_value);
    }
  }
  return found;
}

} // namespace sanitize::internal::object_struct_detail

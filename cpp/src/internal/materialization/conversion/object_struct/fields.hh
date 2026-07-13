// Declares field lookup and strictness helpers for object STRUCT conversion.

#pragma once

#include <cstddef>
#include <string>

#include "internal/materialization/batch_appender_internal.hh"
#include "internal/materialization/conversion/object_fields.hh"

namespace sanitize::internal::object_struct_detail {

[[nodiscard]] bool
should_snapshot_object_fields(const sanitize::ColumnPlan &plan) noexcept;

[[nodiscard]] bool should_check_strict_struct(const sanitize::ColumnPlan &plan,
                                              const ConvertCtx &ctx) noexcept;

[[nodiscard]] sanitize::Status find_strict_extra_field(
    const sanitize::ColumnPlan &plan, sanitize::ValueView value,
    const sanitize::PreparedOptions &opts, std::string *extra);

[[nodiscard]] sanitize::Status find_strict_extra_field(
    const sanitize::ColumnPlan &plan, const ObjectFieldSnapshot &snapshot,
    const sanitize::PreparedOptions &opts, std::string *extra);

[[nodiscard]] sanitize::Status
find_object_child_value(const sanitize::ColumnPlan &child,
                        sanitize::ValueView object, ConvertCtx &ctx,
                        sanitize::ValueView *child_value, bool *found);

[[nodiscard]] bool find_object_child_value(const sanitize::ColumnPlan &child,
                                           const ObjectFieldSnapshot &snapshot,
                                           std::size_t child_index,
                                           ConvertCtx &ctx,
                                           sanitize::ValueView *child_value);

} // namespace sanitize::internal::object_struct_detail

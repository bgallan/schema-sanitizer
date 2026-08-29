// Declares helpers for routing values across versioned field families.
//
// Versioned fields share one logical source name but have incompatible nested
// layouts. These helpers choose the widest compatible sibling before coercion.

#pragma once

#include "sanitize/core/value_view.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

/// Returns a coarse score describing how directly a value fits one column plan.
[[nodiscard]] int
variant_compatibility_score(const sanitize::ColumnPlan &plan,
                            sanitize::ValueView value,
                            const sanitize::PreparedOptions &opts) noexcept;

/// Returns the preferred root column for a value inside a version family.
[[nodiscard]] const sanitize::ColumnPlan *preferred_root_variant_sibling(
    const sanitize::CompiledPlan &plan, const sanitize::ColumnPlan &column,
    sanitize::ValueView value, const sanitize::PreparedOptions &opts) noexcept;

/// Returns the preferred child column for a value inside a version family.
[[nodiscard]] const sanitize::ColumnPlan *preferred_child_variant_sibling(
    const sanitize::ColumnPlan &parent, const sanitize::ColumnPlan &child,
    sanitize::ValueView value, const sanitize::PreparedOptions &opts) noexcept;

} // namespace sanitize::internal

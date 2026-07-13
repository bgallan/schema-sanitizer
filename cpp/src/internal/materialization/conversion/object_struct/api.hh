// Declares object-to-STRUCT conversion for Arrow C Data materialization.

#pragma once

#include "internal/materialization/batch_appender_internal.hh"

namespace sanitize::internal {

// Converts one object-like input into a preallocated STRUCT cell.
[[nodiscard]] sanitize::Status
convert_object_struct(const sanitize::ColumnPlan &plan,
                      sanitize::ValueView value, ConvertCtx &ctx, Cell *cell);

} // namespace sanitize::internal

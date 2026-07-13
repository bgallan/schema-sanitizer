// Declares STRUCT conversion helpers for Arrow C Data materialization.

#pragma once

#include "internal/materialization/batch_appender_internal.hh"

namespace sanitize::internal {

// Converts object and scalar inputs into a struct cell.
sanitize::Status convert_struct(const sanitize::ColumnPlan &plan,
                                sanitize::ValueView value, ConvertCtx &ctx,
                                Cell *out);

} // namespace sanitize::internal

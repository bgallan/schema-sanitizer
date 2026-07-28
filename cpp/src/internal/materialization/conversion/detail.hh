// Declares private helpers shared by build conversion implementations.
//
// These functions are internal to the Arrow C Data build layer and keep scalar
// and nested conversion code split across smaller translation units.

#pragma once

#include "internal/materialization/batch_appender_internal.hh"

#include <string>

namespace sanitize::internal {

// Records the first conversion error and returns it as an invalid status.
sanitize::Status set_conversion_error(ConvertCtx &ctx,
                                      const sanitize::ColumnPlan &plan,
                                      sanitize::DiagnosticCode code,
                                      std::string detail);

// Converts a scalar ValueView into a typed materialization cell.
sanitize::Status convert_scalar(const sanitize::ColumnPlan &plan,
                                sanitize::ValueView value, ConvertCtx &ctx,
                                Cell *out);

// Converts one scalar ValueView into non-owning direct append scratch when
// possible, retaining owned text only for formatted coercion fallbacks.
sanitize::Status convert_direct_scalar(const sanitize::ColumnPlan &plan,
                                       sanitize::ValueView value,
                                       ConvertCtx &ctx, DirectScalarValue *out);

} // namespace sanitize::internal

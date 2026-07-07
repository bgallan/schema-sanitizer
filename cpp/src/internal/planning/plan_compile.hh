// Declares logical-schema to compiled-plan conversion.

#pragma once

#include "sanitize/core/status.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize {

// Compiles a logical schema into a materialization plan.
sanitize::Result<CompiledPlan>
compile_plan(const LogicalSchema &logical_schema);

} // namespace sanitize

// Builds field-name lookup metadata for compiled struct plans.
// The helpers normalize private planning state without leaking wire or layout
// details into public APIs.

#pragma once

#include <vector>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

StructLayout make_struct_layout(const std::vector<LogicalField> &fields);

} // namespace sanitize::internal

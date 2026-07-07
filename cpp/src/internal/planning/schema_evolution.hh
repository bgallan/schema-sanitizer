// Declares schema-contract reconciliation for inferred schemas.

#pragma once

#include "sanitize/core/logical_schema.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal {

// Reorders fields recursively according to a field-ordering policy.
sanitize::LogicalSchema
reorder_schema_fields(const sanitize::LogicalSchema &schema,
                      const sanitize::LogicalSchema *base,
                      FieldOrderPolicy field_order);

// Reconciles inferred fields against an optional schema contract.
sanitize::Result<sanitize::LogicalSchema>
evolve_schema(const sanitize::LogicalSchema &base,
              const sanitize::LogicalSchema &inferred, SchemaEvolutionMode mode,
              FieldOrderPolicy field_order);

} // namespace sanitize::internal

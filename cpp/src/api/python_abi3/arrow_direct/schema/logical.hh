// Declares Arrow C schema parsing for direct ingestion.

#pragma once

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_model.hh"

#include <vector>

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

// Parses an Arrow C stream schema into the logical schema and direct node plan.
sanitize::Result<sanitize::LogicalSchema>
logical_schema_from_arrow_schema(const ArrowSchema *schema,
                                 std::vector<ArrowInputNode> *fields,
                                 const ArrowDirectOptions &options);

} // namespace core_abi3_internal

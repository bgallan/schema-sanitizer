// Declares Arrow C schema parsing helpers for direct ingestion.

#pragma once

#include "api/python_abi3/_core_abi3_arrow_direct.hh"
#include "api/python_abi3/_core_abi3_arrow_direct_model.hh"

#include <string>
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

// Encodes an Arrow C schema into the Python options logical-schema payload.
sanitize::Result<std::string>
logical_schema_payload_from_arrow_schema(const ArrowSchema *schema,
                                         const ArrowDirectOptions &options);

} // namespace core_abi3_internal

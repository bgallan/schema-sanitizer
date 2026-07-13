// Declares Arrow C logical-schema payload encoding.

#pragma once

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"

#include <string>

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

// Encodes an Arrow C schema into the Python options logical-schema payload.
sanitize::Result<std::string>
logical_schema_payload_from_arrow_schema(const ArrowSchema *schema,
                                         const ArrowDirectOptions &options);

} // namespace core_abi3_internal

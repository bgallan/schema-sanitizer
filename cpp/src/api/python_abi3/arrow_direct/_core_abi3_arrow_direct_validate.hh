// Declares logical Arrow C Data validation before direct batch materialization.
// The entry point checks an imported array against its parsed direct-ingestion
// schema.

#pragma once

#include <cstdint>
#include <vector>

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_model.hh"
#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

/// Validates a record-batch root and every child logical range.
/// This rejects malformed logical metadata but cannot authenticate raw
/// pointers.
sanitize::Status
validate_arrow_direct_batch(const ArrowArray &root,
                            const std::vector<ArrowInputNode> &fields,
                            std::int64_t memory_limit_bytes);

} // namespace core_abi3_internal

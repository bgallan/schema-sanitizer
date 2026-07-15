// Validates logical Arrow C Data bounds before direct batch materialization.
#pragma once

#include <cstdint>
#include <vector>

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_model.hh"
#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

// Validates one record-batch root and all child logical ranges. Arrow C Data
// does not expose physical allocation sizes, so this rejects malformed logical
// metadata but cannot authenticate arbitrary raw pointers from hostile code.
sanitize::Status validate_arrow_direct_batch(
    const ArrowArray &root, const std::vector<ArrowInputNode> &fields,
    std::int64_t memory_limit_bytes);

} // namespace core_abi3_internal

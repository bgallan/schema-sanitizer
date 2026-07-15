// Declares Arrow direct RowBatch construction helpers.

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_model.hh"
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_values.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

// Builds stable RowRef and FieldRef storage for one bounded slice of an Arrow
// C Data batch while sharing ownership of the original buffers.
sanitize::Result<sanitize::RowBatch> build_arrow_direct_row_batch(
    std::shared_ptr<ArrowArrayStorage> array_owner,
    const std::vector<ArrowInputNode> &fields, int64_t row_offset,
    int64_t row_count);

} // namespace core_abi3_internal

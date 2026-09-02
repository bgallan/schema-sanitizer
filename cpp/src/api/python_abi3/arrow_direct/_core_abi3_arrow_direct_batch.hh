// Declares Arrow direct RowBatch construction helpers. These routines keep
// Arrow schema interpretation and buffer ownership explicit at the ABI
// boundary.

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_model.hh"
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_values.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

/// Builds stable row and field references for a bounded Arrow C Data batch
/// slice. The returned batch shares ownership of the original buffers.
sanitize::Result<sanitize::RowBatch>
build_arrow_direct_row_batch(std::shared_ptr<ArrowArrayStorage> array_owner,
                             const std::vector<ArrowInputNode> &fields,
                             int64_t row_offset, int64_t row_count);

} // namespace core_abi3_internal

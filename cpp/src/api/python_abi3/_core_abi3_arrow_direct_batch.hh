// Declares Arrow direct RowBatch construction helpers.

#pragma once

#include <memory>
#include <vector>

#include "api/python_abi3/_core_abi3_arrow_direct_model.hh"
#include "api/python_abi3/_core_abi3_arrow_direct_values.hh"
#include "sanitize/core/row_stream.hh"

namespace core_abi3_internal {

// Builds RowRef and FieldRef storage for one Arrow C Data batch.
sanitize::RowBatch
build_arrow_direct_row_batch(std::shared_ptr<ArrowBatchStorage> storage,
                             const std::vector<ArrowInputNode> &fields);

} // namespace core_abi3_internal

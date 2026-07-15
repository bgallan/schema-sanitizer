// Declares Arrow C Data value extraction for the direct frontend.

#pragma once

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_model.hh"

#include <deque>
#include <string>
#include <vector>

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/value_view.hh"

namespace core_abi3_internal {

struct ArrowBatchStorage;

// Identifies one row value inside an ArrowArray.
struct ArrowValueRef {
  const ArrowInputNode *node = nullptr;
  const ArrowArray *array = nullptr;
  int64_t row = 0;
  ArrowBatchStorage *storage = nullptr;
};

// Owns the foreign ArrowArray and releases it exactly once.
struct ArrowArrayStorage {
  ArrowArray array{};

  ~ArrowArrayStorage();
};

// Owns one bounded RowBatch view while sharing the foreign Arrow buffers.
struct ArrowBatchStorage {
  std::shared_ptr<ArrowArrayStorage> array_owner;
  std::vector<sanitize::FieldRef> fields;
  std::vector<sanitize::RowRef> rows;
  std::deque<ArrowValueRef> values;
  std::deque<std::string> strings;

};

// Stores a value reference owned by the batch storage.
const ArrowValueRef *store_value_ref(ArrowBatchStorage *storage,
                                     const ArrowInputNode *node,
                                     const ArrowArray *array, int64_t row);

// Converts one Arrow value reference into the internal ValueView model.
sanitize::ValueView value_from_ref(const ArrowValueRef *ref);

// Returns a value for one row, retaining a heap reference only when the
// resulting ValueView is a nested container that must outlive this call.
sanitize::ValueView value_at(ArrowBatchStorage *storage,
                             const ArrowInputNode *node,
                             const ArrowArray *array, int64_t row);

} // namespace core_abi3_internal

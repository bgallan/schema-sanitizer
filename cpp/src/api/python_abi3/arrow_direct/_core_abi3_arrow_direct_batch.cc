// Builds internal RowBatch views from Arrow C Data batches.

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_batch.hh"

#include <cstddef>
#include <utility>

namespace core_abi3_internal {

sanitize::RowBatch
build_arrow_direct_row_batch(std::shared_ptr<ArrowBatchStorage> storage,
                             const std::vector<ArrowInputNode> &fields) {
  sanitize::RowBatch out;
  const int64_t row_count = storage->array.length;
  const auto field_count = fields.size();
  storage->fields.reserve(static_cast<std::size_t>(row_count) * field_count);
  storage->rows.reserve(static_cast<std::size_t>(row_count));
  for (int64_t row = 0; row < row_count; ++row) {
    const std::size_t start = static_cast<std::size_t>(row) * field_count;
    for (std::size_t col = 0; col < field_count; ++col) {
      const ArrowArray *child_array = storage->array.children[col];
      const ArrowValueRef *value_ref =
          store_value_ref(storage.get(), &fields[col], child_array, row);
      storage->fields.push_back(sanitize::FieldRef{
          .key = fields[col].name,
          .key_hash = 0,
          .value = value_from_ref(value_ref),
      });
    }
    storage->rows.push_back(sanitize::RowRef{
        .fields = storage->fields.data() + start,
        .size = field_count,
        .raw = {},
        .base_offset = 0,
        .direct_ctx = nullptr,
        .source_file = {},
        .flags = std::to_underlying(sanitize::RowFlags::kNone),
    });
  }
  out.rows = storage->rows;
  out.owner = std::move(storage);
  return out;
}

} // namespace core_abi3_internal

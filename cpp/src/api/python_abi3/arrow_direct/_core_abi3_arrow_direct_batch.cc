// Builds internal RowBatch views from Arrow C Data batches.

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_batch.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <utility>

namespace core_abi3_internal {
namespace {

constexpr std::size_t kMaxArrowDirectFieldRefs = 100'000'000;

sanitize::Result<std::size_t> checked_field_ref_count(int64_t row_count,
                                                      std::size_t field_count) {
  if (row_count < 0) {
    return sanitize::Status::Invalid(
        "Arrow direct row slice has a negative length");
  }
  const auto rows = static_cast<std::size_t>(row_count);
  if (field_count != 0 &&
      rows > std::numeric_limits<std::size_t>::max() / field_count) {
    return sanitize::Status::OutOfMemory(
        "Arrow direct row/field count overflows addressable memory");
  }
  const auto refs = rows * field_count;
  if (refs > kMaxArrowDirectFieldRefs) {
    return sanitize::Status::OutOfMemory(
        "Arrow direct row/field reference count exceeds safety limit");
  }
  return refs;
}

} // namespace

sanitize::Result<sanitize::RowBatch>
build_arrow_direct_row_batch(std::shared_ptr<ArrowArrayStorage> array_owner,
                             const std::vector<ArrowInputNode> &fields,
                             int64_t row_offset, int64_t row_count) {
  if (!array_owner) {
    return sanitize::Status::Invalid(
        "Arrow direct row batch has no array owner");
  }
  const ArrowArray &array = array_owner->array;
  if (row_offset < 0 || row_count < 0 || row_offset > array.length ||
      row_count > array.length - row_offset) {
    return sanitize::Status::Invalid(
        "Arrow direct row slice is outside the input batch");
  }
  if (array.n_children < 0 ||
      static_cast<std::uint64_t>(array.n_children) != fields.size() ||
      (!fields.empty() && !array.children)) {
    return sanitize::Status::Invalid(
        "Arrow direct batch/schema child count mismatch");
  }
  SAN_ASSIGN_OR_RAISE(const auto field_refs,
                      checked_field_ref_count(row_count, fields.size()));

  try {
    auto storage = std::make_shared<ArrowBatchStorage>();
    storage->array_owner = std::move(array_owner);
    storage->fields.reserve(field_refs);
    storage->rows.reserve(static_cast<std::size_t>(row_count));

    for (int64_t relative_row = 0; relative_row < row_count; ++relative_row) {
      const int64_t source_row = row_offset + relative_row;
      const std::size_t start =
          static_cast<std::size_t>(relative_row) * fields.size();
      for (std::size_t col = 0; col < fields.size(); ++col) {
        const ArrowArray *child_array = array.children[col];
        storage->fields.push_back(sanitize::FieldRef{
            .key = fields[col].name,
            .key_hash = 0,
            .value =
                value_at(storage.get(), &fields[col], child_array, source_row),
        });
      }
      storage->rows.push_back(sanitize::RowRef{
          .fields = storage->fields.data() + start,
          .size = fields.size(),
          .raw = {},
          .base_offset = 0,
          .direct_ctx = nullptr,
          .source_file = {},
          .flags = std::to_underlying(sanitize::RowFlags::kNone),
      });
    }

    sanitize::RowBatch out;
    out.rows = storage->rows;
    out.owner = std::move(storage);
    return out;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "Arrow direct row batch allocation failed");
  }
}

} // namespace core_abi3_internal

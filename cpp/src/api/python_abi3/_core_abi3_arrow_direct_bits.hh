/*
 * Inline Arrow C Data bitmap and primitive access helpers.
 *
 * These helpers are shared by Arrow direct value extraction units and keep the
 * ABI3 implementation away from repeated local bitmap arithmetic.
 */
#pragma once

#include <cstdint>

#include "nanoarrow/nanoarrow.h"

namespace core_abi3_internal {

// Returns one bit from a packed Arrow bitmap.
inline bool bit_at(const uint8_t *bitmap, int64_t index) noexcept {
  return bitmap && (((bitmap[index >> 3] >> (index & 7)) & 1U) != 0);
}

// Returns whether a row slot is null according to the Arrow validity bitmap.
inline bool is_null_at(const ArrowArray *array, int64_t row) noexcept {
  if (!array || array->null_count == 0) {
    return false;
  }
  const auto *bitmap = static_cast<const uint8_t *>(array->buffers[0]);
  if (!bitmap) {
    return false;
  }
  const int64_t bit_index = array->offset + row;
  return !bit_at(bitmap, bit_index);
}

// Returns the primitive values buffer with the requested type.
template <typename T> inline const T *values_buffer(const ArrowArray *array) {
  if (!array || !array->buffers || !array->buffers[1]) {
    return nullptr;
  }
  return static_cast<const T *>(array->buffers[1]);
}

// Returns one primitive Arrow value at a logical row index.
template <typename T>
inline T primitive_at(const ArrowArray *array, int64_t row) {
  const auto *values = values_buffer<T>(array);
  return values[array->offset + row];
}

} // namespace core_abi3_internal

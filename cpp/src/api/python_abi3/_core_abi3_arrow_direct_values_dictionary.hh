// Declares Arrow direct dictionary-index helpers.

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

#include "nanoarrow/nanoarrow.h"

namespace core_abi3_internal {

// Returns an Arrow dictionary index for supported integer index arrays.
std::optional<int64_t> dictionary_index_at(const ArrowArray *array,
                                           std::string_view format,
                                           int64_t row);

} // namespace core_abi3_internal

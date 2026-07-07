// Declares Arrow direct interval formatting helpers.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "nanoarrow/nanoarrow.h"

namespace core_abi3_internal {

// Formats one Arrow interval value as stable text for direct ingestion.
std::string arrow_interval_to_string(const ArrowArray *array, int64_t row,
                                     std::string_view format);

} // namespace core_abi3_internal

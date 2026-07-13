// Shared JSON number rendering for ABI3 JSON encoders.

#pragma once

#include <string>

namespace core_abi3_internal {

void append_json_double(std::string &out, double value);

} // namespace core_abi3_internal

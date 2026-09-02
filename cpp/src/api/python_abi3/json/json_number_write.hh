// Declares shared JSON number rendering for ABI3 JSON encoders. The routines
// preserve JSON value semantics while enforcing bounded native ownership and
// Python errors.

#pragma once

#include <string>

namespace core_abi3_internal {

void append_json_double(std::string &out, double value);

} // namespace core_abi3_internal

// Declares private ValueView string formatting helpers for build conversion.

#pragma once

#include <string>

#include "sanitize/core/value_view.hh"

namespace sanitize::internal {

// Converts value to scalar string.
std::string value_to_scalar_string(sanitize::ValueView value);

} // namespace sanitize::internal

// Declares binary serialization for public option state.

#pragma once

#include <string>
#include <string_view>

#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize {

// Portable binary decoding for sanitize::Options.

sanitize::Result<Options> deserialize_options(std::string_view bytes);

} // namespace sanitize

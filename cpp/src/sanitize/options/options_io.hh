// Declares stable binary serialization for public option state.
// The SZOPT envelope preserves cross-language option semantics while allowing
// decoders to reject unsupported versions and malformed bounded fields.

#pragma once

#include <string>
#include <string_view>

#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize {

/// Decodes options from the portable versioned binary representation.
sanitize::Result<Options> deserialize_options(std::string_view bytes);

} // namespace sanitize

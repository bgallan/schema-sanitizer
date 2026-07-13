// Internal option-field deserialization contract.
#pragma once

#include <cstddef>
#include <string_view>

#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal::options_io {

sanitize::Status read_option_fields(std::string_view bytes, std::size_t *pos,
                                    sanitize::Options *out);

} // namespace sanitize::internal::options_io

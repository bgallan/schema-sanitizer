// Defines the memory-governed byte buffer used by text output encoders.
#pragma once

#include <memory_resource>
#include <string>

namespace sanitize::internal {

using TextBuffer = std::pmr::string;

} // namespace sanitize::internal

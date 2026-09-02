// Defines the memory-governed byte buffer used by text output encoders.
// The helpers bound parallel text encoding memory while committing prepared
// fragments in source order.

#pragma once

#include <memory_resource>
#include <string>

namespace sanitize::internal {

using TextBuffer = std::pmr::string;

} // namespace sanitize::internal

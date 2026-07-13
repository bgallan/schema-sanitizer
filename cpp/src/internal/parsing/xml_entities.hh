// Declares XML text entity decoding helpers.

#pragma once

#include <string>
#include <string_view>

namespace sanitize::internal {

// Decodes predefined and numeric XML character entities.
std::string decode_xml_entities(std::string_view text);

} // namespace sanitize::internal

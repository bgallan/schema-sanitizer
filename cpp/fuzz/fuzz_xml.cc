#include "internal/parsing/xml/document.hh"

#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  const std::string_view input(reinterpret_cast<const char *>(data), size);
  sanitize::internal::XmlParser parser(input);
  auto document = parser.parse_document();
  if (document.ok()) {
    sanitize::internal::build_xml_node_model(document.ValueOrDie().get());
  }
  return 0;
}

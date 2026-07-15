#include "internal/parsing/json/ondemand/document.hh"

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string_view>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  const std::string_view input(reinterpret_cast<const char *>(data), size);
  sanitize::internal::JsonOnDemandDoc document(std::pmr::new_delete_resource());
  (void)document.ParseValue(input);
  (void)sanitize::internal::json_skip_value(input, 0);
  return 0;
}

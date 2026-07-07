// Implements Arrow direct dictionary-index helpers.

#include "api/python_abi3/_core_abi3_arrow_direct_values_dictionary.hh"

#include "api/python_abi3/_core_abi3_arrow_direct_bits.hh"

namespace core_abi3_internal {

std::optional<int64_t> dictionary_index_at(const ArrowArray *array,
                                           std::string_view format,
                                           int64_t row) {
  if (format == "c") {
    return primitive_at<int8_t>(array, row);
  }
  if (format == "C") {
    return primitive_at<uint8_t>(array, row);
  }
  if (format == "s") {
    return primitive_at<int16_t>(array, row);
  }
  if (format == "S") {
    return primitive_at<uint16_t>(array, row);
  }
  if (format == "i") {
    return primitive_at<int32_t>(array, row);
  }
  if (format == "I") {
    return primitive_at<uint32_t>(array, row);
  }
  if (format == "l") {
    return primitive_at<int64_t>(array, row);
  }
  return std::nullopt;
}

} // namespace core_abi3_internal

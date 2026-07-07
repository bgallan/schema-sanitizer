// Implements Arrow direct interval formatting helpers.

#include "api/python_abi3/_core_abi3_arrow_direct_values_temporal.hh"

#include "api/python_abi3/_core_abi3_arrow_direct_bits.hh"
#include "api/python_abi3/_core_abi3_arrow_direct_formatters.hh"

namespace core_abi3_internal {
namespace {

struct DayTimeInterval {
  int32_t days = 0;
  int32_t milliseconds = 0;
};

struct MonthDayNanoInterval {
  int32_t months = 0;
  int32_t days = 0;
  int64_t nanoseconds = 0;
};

} // namespace

std::string arrow_interval_to_string(const ArrowArray *array, int64_t row,
                                     std::string_view format) {
  if (format == "tiM") {
    return month_interval_to_string(primitive_at<int32_t>(array, row));
  }
  if (!array || !array->buffers || !array->buffers[1]) {
    return {};
  }
  if (format == "tiD") {
    const auto *values =
        static_cast<const DayTimeInterval *>(array->buffers[1]);
    const auto value = values[array->offset + row];
    return day_time_interval_to_string(value.days, value.milliseconds);
  }
  const auto *values =
      static_cast<const MonthDayNanoInterval *>(array->buffers[1]);
  const auto value = values[array->offset + row];
  return month_day_nano_interval_to_string(value.months, value.days,
                                           value.nanoseconds);
}

} // namespace core_abi3_internal

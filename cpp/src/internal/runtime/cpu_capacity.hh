// Detects the CPU capacity visible to the current process without
// configuration.
#pragma once

#include <algorithm>
#include <bit>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <thread>

#if defined(__linux__)
#include <sched.h>
#elif defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

namespace sanitize::internal {
namespace cpu_capacity_detail {

[[nodiscard]] inline std::int64_t hardware_count() noexcept {
  const auto detected = std::thread::hardware_concurrency();
  return detected == 0U ? 1 : static_cast<std::int64_t>(detected);
}

#if defined(__linux__)

[[nodiscard]] inline std::int64_t affinity_count() noexcept {
  cpu_set_t affinity;
  CPU_ZERO(&affinity);
  if (sched_getaffinity(0, sizeof(affinity), &affinity) != 0) {
    return hardware_count();
  }
  const auto count = CPU_COUNT(&affinity);
  return count <= 0 ? 1 : static_cast<std::int64_t>(count);
}

[[nodiscard]] inline bool read_line(const char *path, char *buffer,
                                    std::size_t capacity) noexcept {
  if (path == nullptr || buffer == nullptr || capacity < 2U) {
    return false;
  }
  std::FILE *file = std::fopen(path, "r");
  if (file == nullptr) {
    return false;
  }
  const bool read =
      std::fgets(buffer, static_cast<int>(capacity), file) != nullptr;
  const int close_status = std::fclose(file);
  return read && close_status == 0;
}

[[nodiscard]] inline bool parse_positive(std::string_view text,
                                         std::int64_t &value) noexcept {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
    text.remove_prefix(1U);
  }
  const auto *first = text.data();
  const auto *last = first + text.size();
  const auto parsed = std::from_chars(first, last, value);
  return parsed.ec == std::errc{} && value > 0;
}

[[nodiscard]] inline std::int64_t quota_capacity(std::int64_t quota,
                                                 std::int64_t period) noexcept {
  if (quota <= 0 || period <= 0) {
    return std::numeric_limits<std::int64_t>::max();
  }
  return std::max<std::int64_t>(1, (quota + period - 1) / period);
}

[[nodiscard]] inline std::int64_t cgroup_v2_capacity() noexcept {
  char line[256]{};
  if (!read_line("/sys/fs/cgroup/cpu.max", line, sizeof(line))) {
    return std::numeric_limits<std::int64_t>::max();
  }
  std::string_view text(line);
  while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
    text.remove_suffix(1U);
  }
  const auto separator = text.find_first_of(" \t");
  if (separator == std::string_view::npos ||
      text.substr(0U, separator) == "max") {
    return std::numeric_limits<std::int64_t>::max();
  }
  std::int64_t quota = 0;
  std::int64_t period = 0;
  if (!parse_positive(text.substr(0U, separator), quota) ||
      !parse_positive(text.substr(separator + 1U), period)) {
    return std::numeric_limits<std::int64_t>::max();
  }
  return quota_capacity(quota, period);
}

[[nodiscard]] inline bool read_integer(const char *path,
                                       std::int64_t &value) noexcept {
  char line[128]{};
  if (!read_line(path, line, sizeof(line))) {
    return false;
  }
  return parse_positive(std::string_view(line), value);
}

[[nodiscard]] inline std::int64_t cgroup_v1_capacity() noexcept {
  constexpr const char *roots[] = {"/sys/fs/cgroup/cpu",
                                   "/sys/fs/cgroup/cpu,cpuacct"};
  for (const auto *root : roots) {
    char quota_path[160]{};
    char period_path[160]{};
    const int quota_length = std::snprintf(quota_path, sizeof(quota_path),
                                           "%s/cpu.cfs_quota_us", root);
    const int period_length = std::snprintf(period_path, sizeof(period_path),
                                            "%s/cpu.cfs_period_us", root);
    if (quota_length <= 0 || period_length <= 0 ||
        static_cast<std::size_t>(quota_length) >= sizeof(quota_path) ||
        static_cast<std::size_t>(period_length) >= sizeof(period_path)) {
      continue;
    }
    std::int64_t quota = 0;
    std::int64_t period = 0;
    if (read_integer(quota_path, quota) && read_integer(period_path, period)) {
      return quota_capacity(quota, period);
    }
  }
  return std::numeric_limits<std::int64_t>::max();
}

[[nodiscard]] inline std::int64_t platform_count() noexcept {
  return std::max<std::int64_t>(
      1, std::min({hardware_count(), affinity_count(), cgroup_v2_capacity(),
                   cgroup_v1_capacity()}));
}

#elif defined(_WIN32)

[[nodiscard]] inline std::int64_t platform_count() noexcept {
  DWORD_PTR process_mask = 0;
  DWORD_PTR system_mask = 0;
  if (GetProcessAffinityMask(GetCurrentProcess(), &process_mask,
                             &system_mask) != 0 &&
      process_mask != 0) {
    const auto count = std::popcount(process_mask);
    if (count > 0) {
      return static_cast<std::int64_t>(count);
    }
  }
  const DWORD active = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
  return active == 0U ? hardware_count() : static_cast<std::int64_t>(active);
}

#elif defined(__APPLE__)

[[nodiscard]] inline std::int64_t platform_count() noexcept {
  std::int32_t count = 0;
  std::size_t size = sizeof(count);
  if (sysctlbyname("hw.activecpu", &count, &size, nullptr, 0) == 0 &&
      count > 0) {
    return static_cast<std::int64_t>(count);
  }
  count = 0;
  size = sizeof(count);
  if (sysctlbyname("hw.ncpu", &count, &size, nullptr, 0) == 0 && count > 0) {
    return static_cast<std::int64_t>(count);
  }
  return hardware_count();
}

#else

[[nodiscard]] inline std::int64_t platform_count() noexcept {
  return hardware_count();
}

#endif

} // namespace cpu_capacity_detail

[[nodiscard]] inline std::int64_t available_cpu_capacity() noexcept {
  return cpu_capacity_detail::platform_count();
}

} // namespace sanitize::internal

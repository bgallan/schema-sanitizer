// Detects the memory safely available to one automatic-budget operation.
#include "internal/memory/memory_budget.hh"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <limits>
#include <optional>
#include <string>

#if defined(_WIN32)
#define NOMINMAX
#include <windows.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#include <mach/mach_host.h>
#include <unistd.h>
#else
#include <unistd.h>
#endif

namespace sanitize::internal {
namespace {

#if defined(__linux__)
[[nodiscard]] std::optional<std::uint64_t>
read_unsigned_file(const char *path) noexcept {
  try {
    std::ifstream input(path);
    std::string value;
    if (!(input >> value) || value == "max") {
      return std::nullopt;
    }
    std::size_t consumed = 0;
    const auto parsed = std::stoull(value, &consumed);
    if (consumed != value.size()) {
      return std::nullopt;
    }
    return parsed;
  } catch (...) {
    return std::nullopt;
  }
}

[[nodiscard]] std::optional<std::uint64_t>
remaining_limit(const char *limit_path, const char *usage_path) noexcept {
  const auto limit = read_unsigned_file(limit_path);
  const auto usage = read_unsigned_file(usage_path);
  if (!limit || !usage) {
    return std::nullopt;
  }
  if (*limit <= *usage) {
    return std::uint64_t{1};
  }
  return *limit - *usage;
}

[[nodiscard]] std::optional<std::uint64_t> linux_mem_available() noexcept {
  try {
    std::ifstream input("/proc/meminfo");
    std::string name;
    std::uint64_t kibibytes = 0;
    std::string unit;
    while (input >> name >> kibibytes >> unit) {
      if (name != "MemAvailable:") {
        continue;
      }
      constexpr auto kScale = std::uint64_t{1024};
      if (kibibytes > std::numeric_limits<std::uint64_t>::max() / kScale) {
        return std::nullopt;
      }
      return kibibytes * kScale;
    }
  } catch (...) {
  }
  return std::nullopt;
}
#endif

[[nodiscard]] std::optional<std::uint64_t>
platform_available_memory() noexcept {
#if defined(_WIN32)
  MEMORYSTATUSEX state{};
  state.dwLength = sizeof(state);
  if (GlobalMemoryStatusEx(&state) == 0) {
    return std::nullopt;
  }
  return static_cast<std::uint64_t>(state.ullAvailPhys);
#elif defined(__APPLE__)
  const auto host = mach_host_self();
  mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
  vm_statistics64_data_t stats{};
  if (host_statistics64(host, HOST_VM_INFO64,
                        reinterpret_cast<host_info64_t>(&stats),
                        &count) != KERN_SUCCESS) {
    mach_port_deallocate(mach_task_self(), host);
    return std::nullopt;
  }
  vm_size_t page_size = 0;
  if (host_page_size(host, &page_size) != KERN_SUCCESS) {
    mach_port_deallocate(mach_task_self(), host);
    return std::nullopt;
  }
  mach_port_deallocate(mach_task_self(), host);
  const auto pages = static_cast<std::uint64_t>(stats.free_count) +
                     static_cast<std::uint64_t>(stats.inactive_count) +
                     static_cast<std::uint64_t>(stats.speculative_count);
  if (pages > std::numeric_limits<std::uint64_t>::max() / page_size) {
    return std::nullopt;
  }
  return pages * page_size;
#elif defined(__linux__)
  if (const auto available = linux_mem_available()) {
    return available;
  }
  return std::nullopt;
#else
  const auto pages = sysconf(_SC_AVPHYS_PAGES);
  const auto page_size = sysconf(_SC_PAGESIZE);
  if (pages <= 0 || page_size <= 0) {
    return std::nullopt;
  }
  const auto unsigned_pages = static_cast<std::uint64_t>(pages);
  const auto unsigned_page_size = static_cast<std::uint64_t>(page_size);
  if (unsigned_pages >
      std::numeric_limits<std::uint64_t>::max() / unsigned_page_size) {
    return std::nullopt;
  }
  return unsigned_pages * unsigned_page_size;
#endif
}

[[nodiscard]] std::optional<std::uint64_t>
container_available_memory() noexcept {
#if defined(__linux__)
  if (const auto v2 = remaining_limit("/sys/fs/cgroup/memory.max",
                                      "/sys/fs/cgroup/memory.current")) {
    return v2;
  }
  return remaining_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes",
                         "/sys/fs/cgroup/memory/memory.usage_in_bytes");
#else
  return std::nullopt;
#endif
}

} // namespace

std::int64_t automatic_memory_limit_bytes() noexcept {
  auto available = platform_available_memory();
  if (const auto container = container_available_memory()) {
    available = available ? std::min(*available, *container) : container;
  }
  if (!available) {
    return kDefaultMemoryLimitBytes;
  }
  const auto bounded = std::min<std::uint64_t>(
      *available,
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()));
  return automatic_memory_limit_from_available(
      static_cast<std::int64_t>(bounded));
}

} // namespace sanitize::internal

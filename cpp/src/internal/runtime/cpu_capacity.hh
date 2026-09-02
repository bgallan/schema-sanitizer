// Detects CPU capacity visible to the current process without
// configuration. Hardware, affinity, and cached cgroup quota/cpuset limits
// combine into one positive bound.

#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <thread>

#include "internal/runtime/cgroup_view.hh"

#if defined(__linux__)
#include <sched.h>
#include <unistd.h>
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

inline constinit std::atomic<std::int64_t> g_hardware_count{0};
#if defined(SCHEMA_SANITIZER_TEST_CPU_CAPACITY_OVERRIDE)
// Only standalone native test executables call the explicit setter below.
// Production configuration has no environment or public override surface.
inline constinit std::atomic<std::int64_t> g_test_capacity_override{0};
#endif

/// Returns the platform's online hardware-thread count with a safe minimum
/// of one.
[[nodiscard]] inline std::int64_t hardware_count() noexcept {
  const auto cached = g_hardware_count.load(std::memory_order_acquire);
  if (cached > 0) {
    return cached;
  }
  const auto detected = std::thread::hardware_concurrency();
  const auto normalized =
      detected == 0U ? std::int64_t{1} : static_cast<std::int64_t>(detected);
  std::int64_t empty = 0;
  (void)g_hardware_count.compare_exchange_strong(
      empty, normalized, std::memory_order_release, std::memory_order_relaxed);
  return empty > 0 ? empty : normalized;
}

#if defined(__linux__)

/// Counts CPUs permitted by the current process affinity mask
/// when supported.
[[nodiscard]] inline std::int64_t affinity_count() noexcept {
  cpu_set_t affinity;
  CPU_ZERO(&affinity);
  if (sched_getaffinity(0, sizeof(affinity), &affinity) != 0) {
    return hardware_count();
  }
  const auto count = CPU_COUNT(&affinity);
  return count <= 0 ? 1 : static_cast<std::int64_t>(count);
}

/// Reads one complete small text file while reporting an absent
/// path separately.
[[nodiscard]] inline bool read_line(const char *path, char *buffer,
                                    std::size_t capacity,
                                    bool *missing = nullptr) noexcept {
  if (missing != nullptr) {
    *missing = false;
  }
  if (path == nullptr || buffer == nullptr || capacity < 2U ||
      capacity > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return false;
  }
  errno = 0;
  std::FILE *file = std::fopen(path, "r");
  if (file == nullptr) {
    if (missing != nullptr) {
      *missing = errno == ENOENT;
    }
    return false;
  }
  const bool read =
      std::fgets(buffer, static_cast<int>(capacity), file) != nullptr;
  const bool complete =
      read && (std::strchr(buffer, '\n') != nullptr ||
               (std::strlen(buffer) + 1U < capacity && std::feof(file) != 0));
  const bool input_error = std::ferror(file) != 0;
  const int close_status = std::fclose(file);
  return read && complete && !input_error && close_status == 0;
}

/// Parses a strictly positive signed integer from trimmed text.
[[nodiscard]] inline bool parse_positive(std::string_view text,
                                         std::int64_t &value) noexcept {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
    text.remove_prefix(1U);
  }
  while (!text.empty() && (text.back() == ' ' || text.back() == '\t' ||
                           text.back() == '\r' || text.back() == '\n')) {
    text.remove_suffix(1U);
  }
  const auto *first = text.data();
  const auto *last = first + text.size();
  const auto parsed = std::from_chars(first, last, value);
  return parsed.ec == std::errc{} && parsed.ptr == last && value > 0;
}

/// Parses either the sole v1 unlimited sentinel or a strict positive quota.
[[nodiscard]] inline bool parse_v1_quota(std::string_view text,
                                         std::int64_t &value) noexcept {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
    text.remove_prefix(1U);
  }
  while (!text.empty() && (text.back() == ' ' || text.back() == '\t' ||
                           text.back() == '\r' || text.back() == '\n')) {
    text.remove_suffix(1U);
  }
  if (text == "-1") {
    value = -1;
    return true;
  }
  return parse_positive(text, value);
}

/// Converts a cgroup quota and period into conservative whole-CPU capacity.
[[nodiscard]] inline std::int64_t quota_capacity(std::int64_t quota,
                                                 std::int64_t period) noexcept {
  if (quota <= 0 || period <= 0) {
    return std::numeric_limits<std::int64_t>::max();
  }
  return std::max<std::int64_t>(1, quota / period);
}

/// Parses one exact cgroup-v2 cpu.max record into its effective capacity.
[[nodiscard]] inline bool parse_v2_cpu_max(std::string_view text,
                                           std::int64_t &capacity) noexcept {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
    text.remove_prefix(1U);
  }
  while (!text.empty() && (text.back() == ' ' || text.back() == '\t' ||
                           text.back() == '\n' || text.back() == '\r')) {
    text.remove_suffix(1U);
  }
  const auto separator = text.find_first_of(" \t");
  if (separator == std::string_view::npos) {
    return false;
  }
  const auto quota_field = text.substr(0U, separator);
  auto period_field = text.substr(separator + 1U);
  while (!period_field.empty() &&
         (period_field.front() == ' ' || period_field.front() == '\t')) {
    period_field.remove_prefix(1U);
  }
  if (period_field.empty() ||
      period_field.find_first_of(" \t") != std::string_view::npos) {
    return false;
  }
  std::int64_t period = 0;
  if (!parse_positive(period_field, period)) {
    return false;
  }
  if (quota_field == "max") {
    capacity = std::numeric_limits<std::int64_t>::max();
    return true;
  }
  std::int64_t quota = 0;
  if (!parse_positive(quota_field, quota)) {
    return false;
  }
  capacity = quota_capacity(quota, period);
  return true;
}

/// Reads CPU capacity from the effective cgroup v2 cpu.max hierarchy.
[[nodiscard]] inline std::int64_t cgroup_v2_capacity() noexcept {
  char current[4096]{};
  char mountpoint[4096]{};
  bool hierarchy_complete = false;
  if (!cgroup_view_detail::resolve_directory(
          "cpu", current, sizeof(current), mountpoint, sizeof(mountpoint),
          nullptr, 0U, nullptr, &hierarchy_complete) ||
      !hierarchy_complete) {
    return 1;
  }
  auto effective = std::numeric_limits<std::int64_t>::max();
  char line[256]{};
  for (;;) {
    char path[4096]{};
    const auto written =
        std::snprintf(path, sizeof(path), "%s/cpu.max", current);
    if (written <= 0 || static_cast<std::size_t>(written) >= sizeof(path)) {
      return 1;
    }
    bool missing = false;
    if (!read_line(path, line, sizeof(line), &missing)) {
      if (std::strcmp(current, mountpoint) == 0 && missing) {
        // cgroup2 exempts its root from resource control, so cpu.max normally
        // does not exist there. Other open/read/parse failures remain closed.
        break;
      }
      return 1;
    }
    std::int64_t capacity = 0;
    if (!parse_v2_cpu_max(std::string_view(line), capacity)) {
      return 1;
    }
    effective = std::min(effective, capacity);
    if (std::strcmp(current, mountpoint) == 0) {
      break;
    }
    if (!cgroup_view_detail::parent_directory_in_place(current, mountpoint)) {
      return 1;
    }
  }
  return effective;
}

/// Reads a complete signed integer from a small controller file.
[[nodiscard]] inline bool read_integer(const char *path,
                                       std::int64_t &value) noexcept {
  char line[128]{};
  if (!read_line(path, line, sizeof(line))) {
    return false;
  }
  return parse_positive(std::string_view(line), value);
}

/// Reads CPU capacity from the effective cgroup v1 quota hierarchy.
[[nodiscard]] inline std::int64_t cgroup_v1_capacity() noexcept {
  char current[4096]{};
  char mountpoint[4096]{};
  bool hierarchy_complete = false;
  if (!cgroup_view_detail::resolve_directory(
          "cpu", current, sizeof(current), mountpoint, sizeof(mountpoint),
          nullptr, 0U, nullptr, &hierarchy_complete) ||
      !hierarchy_complete) {
    return 1;
  }
  auto effective = std::numeric_limits<std::int64_t>::max();
  for (;;) {
    char quota_path[4096]{};
    char period_path[4096]{};
    char quota_line[128]{};
    char period_line[128]{};
    const auto quota_written = std::snprintf(quota_path, sizeof(quota_path),
                                             "%s/cpu.cfs_quota_us", current);
    const auto period_written = std::snprintf(period_path, sizeof(period_path),
                                              "%s/cpu.cfs_period_us", current);
    if (quota_written <= 0 || period_written <= 0 ||
        static_cast<std::size_t>(quota_written) >= sizeof(quota_path) ||
        static_cast<std::size_t>(period_written) >= sizeof(period_path) ||
        !read_line(quota_path, quota_line, sizeof(quota_line)) ||
        !read_line(period_path, period_line, sizeof(period_line))) {
      return 1;
    }
    std::int64_t quota = 0;
    std::int64_t period = 0;
    if (!parse_v1_quota(std::string_view(quota_line), quota) ||
        !parse_positive(std::string_view(period_line), period)) {
      return 1;
    }
    if (quota > 0) {
      effective = std::min(effective, quota_capacity(quota, period));
    }
    if (std::strcmp(current, mountpoint) == 0) {
      break;
    }
    if (!cgroup_view_detail::parent_directory_in_place(current, mountpoint)) {
      return 1;
    }
  }
  return effective;
}

/// Parses a canonical cpuset list and returns its CPU count without allocating
/// storage proportional to the largest CPU identifier.
[[nodiscard]] inline bool cpuset_capacity(std::string_view text,
                                          std::int64_t &capacity) noexcept {
  while (!text.empty() && (text.back() == '\n' || text.back() == '\r' ||
                           text.back() == ' ' || text.back() == '\t')) {
    text.remove_suffix(1U);
  }
  if (text.empty()) {
    return false;
  }
  std::int64_t count = 0;
  std::int64_t previous_end = -1;
  while (!text.empty()) {
    const auto comma = text.find(',');
    const auto field = text.substr(0U, comma);
    const auto dash = field.find('-');
    const auto first = field.substr(0U, dash);
    const auto last =
        dash == std::string_view::npos ? first : field.substr(dash + 1U);
    std::int64_t begin = -1;
    std::int64_t end = -1;
    const auto begin_result =
        std::from_chars(first.data(), first.data() + first.size(), begin);
    const auto end_result =
        std::from_chars(last.data(), last.data() + last.size(), end);
    if (first.empty() || last.empty() || begin_result.ec != std::errc{} ||
        begin_result.ptr != first.data() + first.size() ||
        end_result.ec != std::errc{} ||
        end_result.ptr != last.data() + last.size() || begin < 0 ||
        end < begin || begin <= previous_end) {
      return false;
    }
    const auto width = static_cast<std::uint64_t>(end) -
                       static_cast<std::uint64_t>(begin) + 1U;
    if (width > static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max() - count)) {
      return false;
    }
    count += static_cast<std::int64_t>(width);
    previous_end = end;
    if (comma == std::string_view::npos) {
      break;
    }
    text.remove_prefix(comma + 1U);
  }
  capacity = count;
  return capacity > 0;
}

/// Reads the effective cpuset at every visible v2 or v1 ancestor.
[[nodiscard]] inline std::int64_t cgroup_cpuset_capacity() noexcept {
  char current[4096]{};
  char mountpoint[4096]{};
  bool unified = false;
  bool hierarchy_complete = false;
  if (!cgroup_view_detail::resolve_directory(
          "cpuset", current, sizeof(current), mountpoint, sizeof(mountpoint),
          nullptr, 0U, &unified, &hierarchy_complete) ||
      !hierarchy_complete) {
    return 1;
  }
  const char *filename = unified ? "cpuset.cpus.effective" : "cpuset.cpus";
  auto effective = std::numeric_limits<std::int64_t>::max();
  bool saw_value = false;
  for (;;) {
    char path[4096]{};
    char line[4096]{};
    const auto written =
        std::snprintf(path, sizeof(path), "%s/%s", current, filename);
    if (written <= 0 || static_cast<std::size_t>(written) >= sizeof(path) ||
        !read_line(path, line, sizeof(line))) {
      return 1;
    }
    std::string_view text(line);
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r' ||
                             text.back() == ' ' || text.back() == '\t')) {
      text.remove_suffix(1U);
    }
    if (!text.empty()) {
      std::int64_t parsed = 0;
      if (!cpuset_capacity(text, parsed)) {
        return 1;
      }
      effective = std::min(effective, parsed);
      saw_value = true;
    } else if (unified) {
      // v2 effective cpusets are never inherited through an empty value.
      return 1;
    }
    if (std::strcmp(current, mountpoint) == 0) {
      break;
    }
    if (!cgroup_view_detail::parent_directory_in_place(current, mountpoint)) {
      return 1;
    }
  }
  return saw_value ? effective : 1;
}

constexpr std::int64_t kCgroupCapacityRefreshPeriodNs = 250'000'000LL;

/// Returns the current steady-clock time in nanoseconds.
[[nodiscard]] inline std::int64_t monotonic_now_ns() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

/// Computes the next cgroup-capacity refresh deadline with
/// saturating arithmetic.
[[nodiscard]] constexpr std::int64_t
next_cgroup_refresh_after(std::int64_t now) noexcept {
  const auto maximum = std::numeric_limits<std::int64_t>::max();
  return now > maximum - kCgroupCapacityRefreshPeriodNs
             ? maximum
             : now + kCgroupCapacityRefreshPeriodNs;
}

/// Samples whichever cgroup CPU controller version constrains the process.
[[nodiscard]] inline std::int64_t sample_cgroup_capacity() noexcept {
  char cpu_before[4096]{};
  char cpuset_before[4096]{};
  bool cpu_unified_before = false;
  bool cpuset_unified_before = false;
  if (!cgroup_view_detail::current_membership(
          "cpu", cpu_before, sizeof(cpu_before), cpu_unified_before) ||
      !cgroup_view_detail::current_membership("cpuset", cpuset_before,
                                              sizeof(cpuset_before),
                                              cpuset_unified_before)) {
    return 1;
  }
  const auto version = cpu_unified_before ? 2 : 1;
  std::int64_t quota = 1;
  if (version == 2) {
    quota = cgroup_v2_capacity();
  } else if (version == 1) {
    quota = cgroup_v1_capacity();
  } else {
    // An unreadable or unrecognized Linux controller view is not proof that
    // the process is unrestricted. Keep discovery failures fail-closed.
    return 1;
  }
  const auto cpuset = cgroup_cpuset_capacity();
  char cpu_after[4096]{};
  char cpuset_after[4096]{};
  bool cpu_unified_after = false;
  bool cpuset_unified_after = false;
  if (!cgroup_view_detail::current_membership(
          "cpu", cpu_after, sizeof(cpu_after), cpu_unified_after) ||
      !cgroup_view_detail::current_membership(
          "cpuset", cpuset_after, sizeof(cpuset_after), cpuset_unified_after) ||
      cpu_unified_after != cpu_unified_before ||
      cpuset_unified_after != cpuset_unified_before ||
      std::strcmp(cpu_after, cpu_before) != 0 ||
      std::strcmp(cpuset_after, cpuset_before) != 0) {
    return 1;
  }
  return std::min(quota, cpuset);
}

struct CgroupCapacityCache final {
  std::atomic<std::int64_t> capacity{1};
  std::atomic<std::int64_t> next_refresh_ns{
      std::numeric_limits<std::int64_t>::min()};
  std::atomic<std::uint64_t> owner_pid{0U};
};
static_assert(std::atomic<std::int64_t>::is_always_lock_free &&
                  std::atomic<std::uint64_t>::is_always_lock_free,
              "the fork-safe CPU capacity cache requires lock-free atomics");

// Constant initialization avoids a function-local static guard that could be
// inherited in its locked state if another thread forks during first use.
inline constinit CgroupCapacityCache g_cgroup_capacity_cache{};

/// Returns a periodically refreshed cgroup CPU-capacity sample.
[[nodiscard]] inline std::int64_t cached_cgroup_capacity() noexcept {
  auto &cache = g_cgroup_capacity_cache;
  const auto now = monotonic_now_ns();
  const auto owner_pid = static_cast<std::uint64_t>(::getpid());
  if (cache.owner_pid.load(std::memory_order_acquire) != owner_pid) {
    // A post-fork child must never wait for a refresh claimed by a vanished
    // parent thread or trust the parent's cgroup membership. Multiple first
    // callers may sample concurrently, but all remain fail-closed on error and
    // publish the owner last so later readers observe a complete sample.
    const auto sampled = sample_cgroup_capacity();
    cache.capacity.store(sampled, std::memory_order_release);
    cache.next_refresh_ns.store(next_cgroup_refresh_after(now),
                                std::memory_order_release);
    cache.owner_pid.store(owner_pid, std::memory_order_release);
    return sampled;
  }

  auto next = cache.next_refresh_ns.load(std::memory_order_acquire);
  if (now < next) {
    return cache.capacity.load(std::memory_order_acquire);
  }

  const auto claimed_next = next_cgroup_refresh_after(now);
  if (!cache.next_refresh_ns.compare_exchange_strong(
          next, claimed_next, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    // Another caller owns this refresh. The last complete sample remains a
    // safe immutable fallback, and the refresher will publish within the same
    // bounded interval without making hot-path callers wait.
    return cache.capacity.load(std::memory_order_acquire);
  }

  const auto sampled = sample_cgroup_capacity();
  cache.capacity.store(sampled, std::memory_order_release);
  return sampled;
}

/// Combines hardware, affinity, and cgroup limits into visible
/// CPU capacity.
[[nodiscard]] inline std::int64_t platform_count() noexcept {
  return std::max<std::int64_t>(1, std::min({hardware_count(), affinity_count(),
                                             cached_cgroup_capacity()}));
}

#elif defined(_WIN32)

/// Combines Windows process affinity and processor-group counts into visible
/// CPU capacity.
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

/// Reads active macOS CPUs and falls back to the configured hardware count.
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

/// Uses the portable hardware-thread count when no platform probe is available.
[[nodiscard]] inline std::int64_t platform_count() noexcept {
  return hardware_count();
}

#endif

} // namespace cpu_capacity_detail

#if defined(SCHEMA_SANITIZER_TEST_CPU_CAPACITY_OVERRIDE)
/// Sets an in-process capacity only in explicitly compiled native tests.
inline void
set_available_cpu_capacity_for_testing(std::int64_t capacity) noexcept {
  cpu_capacity_detail::g_test_capacity_override.store(
      std::max<std::int64_t>(0, capacity), std::memory_order_release);
}
#endif

/// Returns the positive CPU capacity currently available to
/// native execution.
[[nodiscard]] inline std::int64_t available_cpu_capacity() noexcept {
#if defined(SCHEMA_SANITIZER_TEST_CPU_CAPACITY_OVERRIDE)
  const auto test_override = cpu_capacity_detail::g_test_capacity_override.load(
      std::memory_order_acquire);
  if (test_override > 0) {
    return test_override;
  }
#endif
  return cpu_capacity_detail::platform_count();
}

} // namespace sanitize::internal

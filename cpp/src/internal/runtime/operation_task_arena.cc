// Implements the operation-wide bounded native task arena.
#include "internal/runtime/operation_task_arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/runtime/atomic_worker_bitmap.hh"
#include "internal/runtime/cgroup_view.hh"
#include "internal/runtime/numa_locality.hh"
#include "internal/runtime/operation_task_arena_selection.hh"
#include "internal/runtime/process_cpu_governor.hh"
#include "internal/runtime/process_fd_governor.hh"
#include "internal/runtime/process_identity.hh"

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#if defined(__linux__)
#include <dirent.h>
#include <sys/resource.h>
#include <unistd.h>
#elif defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <tlhelp32.h>
#include <windows.h>
#elif defined(__APPLE__)
#include <libproc.h>
#include <mach/mach.h>
#include <sys/resource.h>
#include <unistd.h>
#endif
#include <exception>
#include <iterator>
#include <limits>
#include <list>
#include <memory_resource>
#include <mutex>
#include <new>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace sanitize::internal {

namespace {
std::atomic<std::size_t> g_live_arena_states{0U};
std::atomic<std::size_t> g_detached_workers{0U};
std::atomic<std::size_t> g_reaper_workers{0U};
std::atomic<std::size_t> g_reaper_thread_permits{0U};
std::atomic<std::size_t> g_reaper_thread_start_failures{0U};
std::atomic<std::size_t> g_native_counter_underflows{0U};
// One process-global physical-thread permit domain shared by native workers
// and Python governed Thread.start() calls through the ABI.
// One atomic admission authority commits the combined managed + external
// physical-thread envelope. Per-domain counters below are diagnostic/ownership
// subledgers only; they never independently decide capacity.
std::atomic<std::size_t> g_process_total_thread_permits{0U};
std::atomic<std::size_t> g_process_physical_thread_permits{0U};
std::atomic<std::size_t> g_process_external_runtime_thread_permits{0U};
// Amount-based native release APIs are retained for ABI compatibility, but
// any impossible debit poisons the whole permit domain.  This prevents a
// duplicate/stale release from turning corrupted accounting into reusable
// process headroom.  Exact Python/native RAII owners may still retire debt.
std::atomic<bool> g_process_thread_permit_corrupted{false};
// Runtime-reported resident external workers are observational credits, not
// active claims. They are kept distinct so a persistent pool is not mistaken
// for unrelated unmanaged process threads after an operation releases its
// claim.
std::atomic<std::size_t> g_process_external_runtime_resident_threads{0U};
// Memory debt is intentionally distinct from identity credit. If a resident
// probe becomes temporarily unavailable, CPU attribution may retract to zero
// while stack reservations remain charged until positive retirement evidence.
std::atomic<std::size_t> g_process_external_runtime_stack_debt_threads{0U};
std::atomic<std::size_t> g_external_runtime_resident_protocol_violations{0U};
// Residency identity and stack-debt form one invariant-bearing subledger.
// A dedicated allocation-free writer gate makes validation + publication a
// transaction; the general permit mutation epoch only protects readers from
// torn aggregate snapshots and intentionally does not serialize writers.
std::atomic_flag g_external_runtime_residency_writer = ATOMIC_FLAG_INIT;
std::atomic<bool> g_external_runtime_residency_corrupted{false};
// Number of writers between the combined permit commit and its per-domain
// subledger publication. Snapshots wait for zero instead of observing a
// transient non-conserving total. Writers remain mutually concurrent.
std::atomic<std::size_t> g_process_thread_ledger_mutations_inflight{0U};
std::atomic<std::uint64_t> g_process_thread_ledger_mutation_epoch{0U};
std::atomic<std::size_t> g_managed_running_threads{0U};
std::atomic<std::size_t> g_native_physical_thread_rejections{0U};
std::atomic<std::size_t> g_completion_memory_protocol_violations{0U};
std::atomic<std::size_t> g_process_file_descriptor_permits{0U};
std::atomic<std::size_t> g_process_file_descriptors_opened{0U};
std::atomic<std::size_t> g_process_file_descriptor_rejections{0U};
std::atomic<std::size_t> g_process_file_descriptor_protocol_violations{0U};
std::atomic<bool> g_process_file_descriptor_permit_corrupted{false};
std::atomic<std::size_t> g_process_file_descriptor_uncertain_close_debts{0U};
std::atomic<std::uint64_t> g_process_fd_epoch{0U};
std::atomic<std::size_t> g_process_fd_waiters{0U};
// Strict FIFO ticketing replaces Pass69's g_process_fd_fifo_mutex /
// std::timed_mutex scheduler-dependent ordering. A fixed cancellation ring
// keeps timeout retirement allocation-free and bounded.
constexpr std::size_t kProcessFdTicketSlots = 65536U;
std::atomic<std::uint64_t> g_process_fd_next_ticket{0U};
std::atomic<std::uint64_t> g_process_fd_serving_ticket{0U};
std::array<std::atomic<std::uint64_t>, kProcessFdTicketSlots>
    g_process_fd_cancelled_tickets{};
std::mutex g_process_fd_wait_mutex;
std::condition_variable g_process_fd_wait_cv;
constexpr std::size_t kReaperThreadPermitCapacity = 2U;

[[nodiscard]] std::size_t ConfiguredProcessFdCapacity() noexcept {
  constexpr std::size_t kAbsoluteCap = 65536U;
  std::size_t capacity = 4096U;
  const char *configured = std::getenv("SCHEMA_SANITIZER_MAX_OPEN_FILES");
  if (configured && *configured != '\0') {
    if (*configured == '-') {
      return 0U;
    }
    char *end = nullptr;
    const auto parsed = std::strtoull(configured, &end, 10);
    if (end != configured && (!end || *end == '\0')) {
      capacity =
          std::min<std::size_t>(kAbsoluteCap, static_cast<std::size_t>(parsed));
    }
  }
#if defined(__linux__) || defined(__APPLE__)
  struct rlimit limits{};
  if (::getrlimit(RLIMIT_NOFILE, &limits) == 0 &&
      limits.rlim_cur != RLIM_INFINITY) {
    const auto soft = static_cast<std::uint64_t>(limits.rlim_cur);
    const auto reserve =
        std::max<std::uint64_t>(16U, std::min<std::uint64_t>(256U, soft / 8U));
    const auto usable = soft > reserve ? soft - reserve : 0U;
    capacity = std::min<std::size_t>(
        capacity, static_cast<std::size_t>(
                      std::min<std::uint64_t>(usable, kAbsoluteCap)));
  }
#endif
  return capacity;
}

[[nodiscard]] std::optional<std::size_t> ProcessFileDescriptorCount() noexcept {
#if defined(__linux__)
  DIR *directory = ::opendir("/proc/self/fd");
  if (!directory) {
    return std::nullopt;
  }
  std::size_t count = 0U;
  while (const auto *entry = ::readdir(directory)) {
    const char first = entry->d_name[0];
    if (first >= '0' && first <= '9') {
      ++count;
    }
  }
  // The directory handle itself appears in /proc/self/fd.
  if (count != 0U) {
    --count;
  }
  if (::closedir(directory) != 0) {
    return std::nullopt;
  }
  return count;
#elif defined(__APPLE__)
  proc_taskallinfo info{};
  const int bytes =
      ::proc_pidinfo(::getpid(), PROC_PIDTASKALLINFO, 0, &info, sizeof(info));
  if (bytes != static_cast<int>(sizeof(info))) {
    return std::nullopt;
  }
  return static_cast<std::size_t>(info.pbsd.pbi_nfiles);
#else
  return std::nullopt;
#endif
}

[[nodiscard]] std::size_t
TryAcquireProcessFdPermitsUpTo(std::size_t desired, std::size_t minimum,
                               bool queued_waiter = false) noexcept {
  if (!runtime_owner_process() || desired == 0U || minimum == 0U ||
      minimum > desired ||
      g_process_file_descriptor_permit_corrupted.load(
          std::memory_order_acquire)) {
    return 0U;
  }
  // Do not let opportunistic/native try-acquire traffic repeatedly overtake a
  // blocked cross-language waiter. Waiting callers bypass this check while
  // remaining bounded/cancellable at the Python layer.
  if (!queued_waiter &&
      g_process_fd_waiters.load(std::memory_order_acquire) != 0U) {
    return 0U;
  }
  auto current =
      g_process_file_descriptor_permits.load(std::memory_order_acquire);
  for (;;) {
    auto effective_capacity = ConfiguredProcessFdCapacity();
#if defined(__linux__) || defined(__APPLE__)
    const auto observed = ProcessFileDescriptorCount();
    if (!observed) {
      g_process_file_descriptor_rejections.fetch_add(1U,
                                                     std::memory_order_relaxed);
      return 0U;
    }
    // Reserved permits and physically-open governed descriptors are distinct.
    // Only the latter can be subtracted from /proc/self/fd.  Treating all
    // reservations as already-open would admit beyond the intended safety
    // margin when many threads reserve before calling open().
    const auto opened =
        g_process_file_descriptors_opened.load(std::memory_order_acquire);
    const auto external = *observed > opened ? *observed - opened : 0U;
    effective_capacity =
        external >= effective_capacity ? 0U : effective_capacity - external;
#endif
    if (current >= effective_capacity) {
      break;
    }
    const auto available = effective_capacity - current;
    const auto granted = std::min(desired, available);
    if (granted < minimum) {
      break;
    }
    if (g_process_file_descriptor_permit_corrupted.load(
            std::memory_order_acquire)) {
      return 0U;
    }
    if (g_process_file_descriptor_permits.compare_exchange_weak(
            current, current + granted, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      // Close the tiny check/CAS race. If quarantine linearized concurrently,
      // never expose the claim. Retain it as conservative terminal debt rather
      // than amount-releasing it and risking theft from another exact owner.
      if (g_process_file_descriptor_permit_corrupted.load(
              std::memory_order_acquire)) {
        return 0U;
      }
      return granted;
    }
  }
  return 0U;
}

[[nodiscard]] std::size_t ConfiguredProcessThreadCapacity() noexcept {
  constexpr std::size_t kAbsoluteCap = 512U;
  // Physical thread ownership is deliberately independent from runnable CPU
  // credits. Wide arenas may keep many parked workers while ProcessCpuGovernor
  // independently bounds how many execute simultaneously.
  constexpr std::size_t default_capacity = 256U;
  const char *configured = std::getenv("SCHEMA_SANITIZER_MAX_PROJECT_THREADS");
  if (!configured || *configured == '\0') {
    return default_capacity;
  }
  if (*configured == '-') {
    return 0U;
  }
  char *end = nullptr;
  const auto parsed = std::strtoull(configured, &end, 10);
  if (end == configured || (end && *end != '\0')) {
    return default_capacity;
  }
  return std::min<std::size_t>(kAbsoluteCap, static_cast<std::size_t>(parsed));
}

[[nodiscard]] std::optional<std::size_t> ProcessPhysicalThreadCount() noexcept {
#if defined(__linux__)
  DIR *directory = ::opendir("/proc/self/task");
  if (!directory) {
    return std::nullopt;
  }
  std::size_t count = 0U;
  while (const auto *entry = ::readdir(directory)) {
    const char first = entry->d_name[0];
    if (first >= '0' && first <= '9') {
      ++count;
    }
  }
  if (::closedir(directory) != 0) {
    return std::nullopt;
  }
  return count;
#elif defined(_WIN32)
  const DWORD process_id = ::GetCurrentProcessId();
  HANDLE snapshot = ::CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
  if (snapshot == INVALID_HANDLE_VALUE) {
    return std::nullopt;
  }
  THREADENTRY32 entry{};
  entry.dwSize = sizeof(entry);
  std::size_t count = 0U;
  if (::Thread32First(snapshot, &entry) != FALSE) {
    do {
      if (entry.th32OwnerProcessID == process_id) {
        ++count;
      }
      entry.dwSize = sizeof(entry);
    } while (::Thread32Next(snapshot, &entry) != FALSE);
  } else {
    ::CloseHandle(snapshot);
    return std::nullopt;
  }
  ::CloseHandle(snapshot);
  return count;
#elif defined(__APPLE__)
  thread_act_array_t threads = nullptr;
  mach_msg_type_number_t count = 0;
  if (task_threads(mach_task_self(), &threads, &count) != KERN_SUCCESS) {
    return std::nullopt;
  }
  if (threads != nullptr) {
    const auto bytes = static_cast<vm_size_t>(count) * sizeof(thread_t);
    (void)vm_deallocate(mach_task_self(),
                        reinterpret_cast<vm_address_t>(threads), bytes);
  }
  return static_cast<std::size_t>(count);
#else
  return std::nullopt;
#endif
}

[[nodiscard]] std::uint64_t ThreadStackReservationBytes() noexcept {
  constexpr std::uint64_t kDefault = 8ULL * 1024ULL * 1024ULL;
  if (const char *configured =
          std::getenv("SCHEMA_SANITIZER_THREAD_STACK_RESERVATION_BYTES");
      configured && *configured != '\0') {
    if (*configured == '-') {
      return kDefault;
    }
    char *end = nullptr;
    const auto parsed = std::strtoull(configured, &end, 10);
    if (end != configured && (!end || *end == '\0') && parsed != 0U) {
      return std::max<std::uint64_t>(kDefault,
                                     static_cast<std::uint64_t>(parsed));
    }
  }
#if defined(__linux__) || defined(__APPLE__)
  struct rlimit limits{};
  if (::getrlimit(RLIMIT_STACK, &limits) == 0 &&
      limits.rlim_cur != RLIM_INFINITY && limits.rlim_cur != 0U) {
    return std::max<std::uint64_t>(kDefault,
                                   static_cast<std::uint64_t>(limits.rlim_cur));
  }
#endif
  return kDefault;
}

[[nodiscard]] std::optional<std::size_t> ProcessPidThreadHeadroom() noexcept {
#if defined(__linux__)
  using cgroup_view_detail::ValueState;
  const auto version = cgroup_view_detail::current_version("pids");
  if (version == 1 || version == 2) {
    const auto sample = cgroup_view_detail::effective_headroom(
        "pids", "pids.max", "pids.current");
    if (sample.state == ValueState::kUnknown) {
      return 0U;
    }
    if (sample.state == ValueState::kValue) {
      constexpr std::size_t kPidReserve = 16U;
      const auto headroom = static_cast<std::size_t>(std::min<std::uint64_t>(
          sample.value, std::numeric_limits<std::size_t>::max()));
      return headroom > kPidReserve ? headroom - kPidReserve : 0U;
    }
  }
#endif
  return std::nullopt;
}

// Historical pass70 contract name: ProcessRlimitThreadCapacity. Pass71
// refines Linux semantics to per-UID headroom rather than a process-local cap.
[[nodiscard]] std::optional<std::size_t>
ProcessRlimitThreadHeadroom() noexcept {
#if defined(__linux__)
  struct rlimit limits{};
  if (::getrlimit(RLIMIT_NPROC, &limits) != 0 ||
      limits.rlim_cur == RLIM_INFINITY) {
    return std::nullopt;
  }
  // RLIMIT_NPROC is charged to the real UID on Linux, not exclusively to this
  // process. Count same-UID threads from /proc so the admission signal is
  // headroom rather than a misleading process-local absolute ceiling.
  DIR *dir = ::opendir("/proc");
  if (dir == nullptr) {
    return std::nullopt;
  }
  const auto real_uid = static_cast<unsigned long>(::getuid());
  std::uint64_t uid_threads = 0U;
  while (const dirent *entry = ::readdir(dir)) {
    const char *name = entry->d_name;
    if (name == nullptr || *name < '0' || *name > '9') {
      continue;
    }
    bool numeric = true;
    for (const char *cursor = name; *cursor != '\0'; ++cursor) {
      if (*cursor < '0' || *cursor > '9') {
        numeric = false;
        break;
      }
    }
    if (!numeric) {
      continue;
    }
    char path[64]{};
    if (std::snprintf(path, sizeof(path), "/proc/%s/status", name) <= 0) {
      continue;
    }
    FILE *status = std::fopen(path, "r");
    if (status == nullptr) {
      continue;
    }
    unsigned long observed_uid = std::numeric_limits<unsigned long>::max();
    std::uint64_t threads = 0U;
    char line[256]{};
    while (std::fgets(line, sizeof(line), status) != nullptr) {
      if (std::strncmp(line, "Uid:", 4U) == 0) {
        unsigned long parsed = 0U;
        if (std::sscanf(line + 4, "%lu", &parsed) == 1) {
          observed_uid = parsed;
        }
      } else if (std::strncmp(line, "Threads:", 8U) == 0) {
        unsigned long long parsed = 0U;
        if (std::sscanf(line + 8, "%llu", &parsed) == 1) {
          threads = static_cast<std::uint64_t>(parsed);
        }
      }
    }
    std::fclose(status);
    if (observed_uid == real_uid) {
      const auto max_u64 = std::numeric_limits<std::uint64_t>::max();
      uid_threads =
          threads > max_u64 - uid_threads ? max_u64 : uid_threads + threads;
    }
  }
  ::closedir(dir);
  constexpr std::uint64_t kReserve = 16U;
  const auto soft = static_cast<std::uint64_t>(limits.rlim_cur);
  const auto used_with_reserve =
      uid_threads > std::numeric_limits<std::uint64_t>::max() - kReserve
          ? std::numeric_limits<std::uint64_t>::max()
          : uid_threads + kReserve;
  const auto headroom =
      soft > used_with_reserve ? soft - used_with_reserve : 0U;
  return static_cast<std::size_t>(std::min<std::uint64_t>(
      headroom, std::numeric_limits<std::size_t>::max()));
#elif defined(__APPLE__)
  // macOS does not expose Linux /proc accounting. Keep RLIMIT_NPROC as a weak
  // ceiling signal there; kernel thread creation remains the final authority.
  struct rlimit limits{};
  if (::getrlimit(RLIMIT_NPROC, &limits) == 0 &&
      limits.rlim_cur != RLIM_INFINITY) {
    constexpr std::uint64_t kReserve = 16U;
    const auto soft = static_cast<std::uint64_t>(limits.rlim_cur);
    const auto bounded = soft > kReserve ? soft - kReserve : 0U;
    return static_cast<std::size_t>(std::min<std::uint64_t>(
        bounded, std::numeric_limits<std::size_t>::max()));
  }
#endif
  return std::nullopt;
}

[[nodiscard]] std::optional<std::size_t>
ManagedThreadMemoryCapacity(std::size_t current_permits,
                            std::size_t stack_reservations) noexcept {
  const std::uint64_t kStackReservation = ThreadStackReservationBytes();
  constexpr std::uint64_t kEmergencyReserve = 256ULL * 1024ULL * 1024ULL;
  std::uint64_t headroom = 0U;
#if defined(__linux__)
  using cgroup_view_detail::ValueState;
  cgroup_view_detail::UnsignedSample sample{};
  const auto version = cgroup_view_detail::current_version("memory");
  if (version == 2) {
    sample = cgroup_view_detail::effective_headroom("memory", "memory.max",
                                                    "memory.current");
  } else if (version == 1) {
    sample = cgroup_view_detail::effective_headroom(
        "memory", "memory.limit_in_bytes", "memory.usage_in_bytes");
  } else {
    return current_permits;
  }
  if (sample.state == ValueState::kUnknown) {
    return current_permits;
  }
  if (sample.state == ValueState::kUnbounded) {
    return std::nullopt;
  }
  headroom = sample.value;
#elif defined(_WIN32)
  MEMORYSTATUSEX status{};
  status.dwLength = sizeof(status);
  if (::GlobalMemoryStatusEx(&status) == FALSE) {
    return current_permits;
  }
  headroom = static_cast<std::uint64_t>(status.ullAvailPhys);
#elif defined(__APPLE__)
  mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
  vm_statistics64_data_t stats{};
  if (::host_statistics64(mach_host_self(), HOST_VM_INFO64,
                          reinterpret_cast<host_info64_t>(&stats),
                          &count) != KERN_SUCCESS) {
    return current_permits;
  }
  vm_size_t page_size = 0U;
  if (::host_page_size(mach_host_self(), &page_size) != KERN_SUCCESS ||
      page_size == 0U) {
    return current_permits;
  }
  const auto pages = static_cast<std::uint64_t>(stats.free_count) +
                     static_cast<std::uint64_t>(stats.inactive_count) +
                     static_cast<std::uint64_t>(stats.speculative_count);
  headroom = pages > std::numeric_limits<std::uint64_t>::max() / page_size
                 ? std::numeric_limits<std::uint64_t>::max()
                 : pages * static_cast<std::uint64_t>(page_size);
#else
  return std::nullopt;
#endif
  const auto usable =
      headroom > kEmergencyReserve ? headroom - kEmergencyReserve : 0ULL;
  const auto max_size =
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max());
  const auto bounded_permits =
      std::min<std::uint64_t>(current_permits, max_size);
  const auto bounded_stacks =
      std::min<std::uint64_t>(stack_reservations, max_size);
  const auto virtual_reserved =
      bounded_stacks >
              std::numeric_limits<std::uint64_t>::max() / kStackReservation
          ? std::numeric_limits<std::uint64_t>::max()
          : bounded_stacks * kStackReservation;
  const auto remaining =
      usable > virtual_reserved ? usable - virtual_reserved : 0ULL;
  const auto additional = remaining / kStackReservation;
  const auto total =
      std::min<std::uint64_t>(max_size, bounded_permits + additional);
  return static_cast<std::size_t>(total);
}

[[nodiscard]] std::size_t NativePhysicalThreadCapacity() noexcept {
  constexpr std::size_t kAbsoluteCap = 512U;
  const auto configured = ConfiguredProcessThreadCapacity();
  if (configured == 0U) {
    return 0U;
  }
  return std::min(configured, kAbsoluteCap);
}

class ExternalRuntimeResidencyWriterGuard final {
public:
  ExternalRuntimeResidencyWriterGuard() noexcept {
    while (g_external_runtime_residency_writer.test_and_set(
        std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }
  ExternalRuntimeResidencyWriterGuard(
      const ExternalRuntimeResidencyWriterGuard &) = delete;
  ExternalRuntimeResidencyWriterGuard &
  operator=(const ExternalRuntimeResidencyWriterGuard &) = delete;
  ~ExternalRuntimeResidencyWriterGuard() noexcept {
    g_external_runtime_residency_writer.clear(std::memory_order_release);
  }
};

[[nodiscard]] bool ExternalRuntimeResidencyHealthy() noexcept {
  return !g_external_runtime_residency_corrupted.load(
      std::memory_order_acquire);
}

void QuarantineExternalRuntimeResidency() noexcept {
  g_external_runtime_residency_corrupted.store(true, std::memory_order_release);
  g_external_runtime_resident_protocol_violations.fetch_add(
      1U, std::memory_order_relaxed);
}

void BeginThreadPermitLedgerMutation() noexcept {
  g_process_thread_ledger_mutations_inflight.fetch_add(
      1U, std::memory_order_acq_rel);
  g_process_thread_ledger_mutation_epoch.fetch_add(1U,
                                                   std::memory_order_acq_rel);
}

void EndThreadPermitLedgerMutation() noexcept {
  // Publish a generation change before dropping the in-flight marker. A reader
  // that missed the short-lived writer still observes epoch_before !=
  // epoch_after.
  g_process_thread_ledger_mutation_epoch.fetch_add(1U,
                                                   std::memory_order_release);
  g_process_thread_ledger_mutations_inflight.fetch_sub(
      1U, std::memory_order_release);
}

[[nodiscard]] std::size_t
ProcessThreadStackReservationCount(std::size_t total_reserved) noexcept {
  const auto managed_reserved =
      g_process_physical_thread_permits.load(std::memory_order_acquire);
  const auto external_active =
      g_process_external_runtime_thread_permits.load(std::memory_order_acquire);
  const auto resident_stack_debt =
      g_process_external_runtime_stack_debt_threads.load(
          std::memory_order_acquire);
  // Pass69 source-contract breadcrumb: std::max(external_active,
  // resident_external). Pass70 intentionally substitutes resident_stack_debt so
  // an unknown identity probe cannot forgive virtual stack memory that may
  // still be resident.
  const auto external_stack_width =
      std::max(external_active, resident_stack_debt);
  const auto max_size = std::numeric_limits<std::size_t>::max();
  const auto modelled = managed_reserved > max_size - external_stack_width
                            ? max_size
                            : managed_reserved + external_stack_width;
  // The combined total is the admission authority and can temporarily lead its
  // domain subledgers while a writer is publishing ownership. Never let that
  // normal publication window reduce active-stack memory protection.
  return std::max(total_reserved, modelled);
}

[[nodiscard]] std::size_t
EffectiveProcessThreadCapacity(std::size_t total_reserved) noexcept {
  const auto configured_process_capacity = ConfiguredProcessThreadCapacity();
  const auto physical_capacity = NativePhysicalThreadCapacity();
  if (configured_process_capacity == 0U || physical_capacity == 0U) {
    return 0U;
  }
  const auto process_threads = ProcessPhysicalThreadCount();
  if (!process_threads) {
    return 0U;
  }
  const auto managed_running =
      g_managed_running_threads.load(std::memory_order_acquire);
  const auto observed_unmanaged = *process_threads > managed_running
                                      ? *process_threads - managed_running
                                      : 0U;
  const auto resident_external =
      g_process_external_runtime_resident_threads.load(
          std::memory_order_acquire);
  // Only runtime-reported resident workers may offset OS-observed unmanaged
  // threads. Active claims are reservations, not identity evidence, and are
  // therefore never subtracted from the observation independently.
  // Historical source-contract breadcrumb: external_threads are represented by
  // resident identity evidence, never by active reservation claims.
  const auto attributed_external =
      std::min(observed_unmanaged, resident_external);
  const auto unaccounted_external = observed_unmanaged - attributed_external;
  // Historical pass53 name breadcrumb: process_managed_capacity. The effective
  // process capacity is now shared by managed and external active reservations.
  const auto process_capacity =
      unaccounted_external >= configured_process_capacity
          ? 0U
          : configured_process_capacity - unaccounted_external;
  auto effective_capacity = std::min(physical_capacity, process_capacity);
  const auto pid_headroom = ProcessPidThreadHeadroom();
  if (pid_headroom) {
    const auto max_size = std::numeric_limits<std::size_t>::max();
    const auto pid_capacity = *pid_headroom > max_size - total_reserved
                                  ? max_size
                                  : total_reserved + *pid_headroom;
    effective_capacity = std::min(effective_capacity, pid_capacity);
  } else if (const auto rlimit_headroom = ProcessRlimitThreadHeadroom()) {
    // cgroup pids is the hard authority when present. RLIMIT_NPROC is a weaker
    // per-UID fallback; on Linux its /proc scan is therefore paid only when the
    // cgroup controller cannot supply an authoritative headroom signal.
#if defined(__linux__)
    const auto max_size = std::numeric_limits<std::size_t>::max();
    const auto rlimit_capacity = *rlimit_headroom > max_size - total_reserved
                                     ? max_size
                                     : total_reserved + *rlimit_headroom;
    effective_capacity = std::min(effective_capacity, rlimit_capacity);
#else
    effective_capacity = std::min(effective_capacity, *rlimit_headroom);
#endif
  }
  const auto stack_reservations =
      ProcessThreadStackReservationCount(total_reserved);
  if (const auto memory_capacity =
          ManagedThreadMemoryCapacity(total_reserved, stack_reservations)) {
    effective_capacity = std::min(effective_capacity, *memory_capacity);
  }
  return effective_capacity;
}

template <typename CommitDomain>
[[nodiscard]] std::size_t
TryAcquireProcessThreadPermitsUpTo(std::size_t desired, std::size_t minimum,
                                   CommitDomain &&commit_domain) noexcept {
  if (!runtime_owner_process() || desired == 0U || minimum == 0U ||
      minimum > desired ||
      g_process_thread_permit_corrupted.load(std::memory_order_acquire)) {
    return 0U;
  }
  // Pass53 source-contract breadcrumb:
  // g_process_physical_thread_permits.compare_exchange_weak was replaced by the
  // pass68+ combined-total admission CAS; the managed counter is now only an
  // ownership subledger.
  auto total = g_process_total_thread_permits.load(std::memory_order_acquire);
  for (;;) {
    const auto effective_capacity = EffectiveProcessThreadCapacity(total);
    if (total >= effective_capacity) {
      break;
    }
    const auto available = effective_capacity - total;
    const auto granted = std::min(desired, available);
    if (granted < minimum) {
      break;
    }
    if (g_process_thread_permit_corrupted.load(std::memory_order_acquire)) {
      break;
    }
    BeginThreadPermitLedgerMutation();
    if (g_process_total_thread_permits.compare_exchange_weak(
            total, total + granted, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      // The combined reservation is the admission commit. Keep the mutation
      // marker live until the ownership subledger has published so diagnostics
      // can obtain a conserving snapshot without serializing writers.
      // Always publish the matching subledger before observing quarantine so
      // diagnostics remain conserving. A concurrently poisoned grant is kept as
      // terminal debt and never exposed; amount-based rollback could otherwise
      // debit an unrelated exact owner.
      commit_domain(granted);
      const bool poisoned =
          g_process_thread_permit_corrupted.load(std::memory_order_acquire);
      EndThreadPermitLedgerMutation();
      if (poisoned) {
        break;
      }
      return granted;
    }
    EndThreadPermitLedgerMutation();
  }
  g_native_physical_thread_rejections.fetch_add(1U, std::memory_order_relaxed);
  return 0U;
}

[[nodiscard]] std::size_t
TryAcquireProcessPhysicalThreadPermitsUpTo(std::size_t desired,
                                           std::size_t minimum) noexcept {
  if (!runtime_owner_process()) {
    return 0U;
  }
  // Preserve the public fail-closed observation boundary before delegating to
  // the shared atomic authority; the CAS loop re-observes after contention.
  do {
    const auto process_threads = ProcessPhysicalThreadCount();
    if (!process_threads) {
      break;
    }
    return TryAcquireProcessThreadPermitsUpTo(
        desired, minimum, [](std::size_t granted) noexcept {
          g_process_physical_thread_permits.fetch_add(
              granted, std::memory_order_acq_rel);
        });
  } while (false);
  g_native_physical_thread_rejections.fetch_add(1U, std::memory_order_relaxed);
  return 0U;
}

[[nodiscard]] std::size_t TryAcquireProcessExternalRuntimeThreadPermitsUpTo(
    std::size_t desired, std::size_t minimum) noexcept {
  if (!runtime_owner_process()) {
    return 0U;
  }
  return TryAcquireProcessThreadPermitsUpTo(
      desired, minimum, [](std::size_t granted) noexcept {
        g_process_external_runtime_thread_permits.fetch_add(
            granted, std::memory_order_acq_rel);
      });
}

template <class Function>
[[nodiscard]] std::thread StartGovernedNativeThread(Function &&function) {
  ProcessPhysicalThreadPermitLease permit(1U);
  if (!permit) {
    throw std::system_error(
        std::make_error_code(std::errc::resource_unavailable_try_again),
        "native physical thread capacity exhausted");
  }
  // The permit moves into the thread callable before std::thread can publish a
  // running worker. Constructor failure destroys the callable and therefore
  // returns the permit automatically; normal exit does the same exactly once.
  return std::thread([permit = std::move(permit),
                      function = std::forward<Function>(function)]() mutable {
    g_managed_running_threads.fetch_add(1U, std::memory_order_acq_rel);
    struct RunningGuard final {
      ~RunningGuard() {
        const auto previous =
            g_managed_running_threads.fetch_sub(1U, std::memory_order_acq_rel);
        if (previous == 0U) {
          g_managed_running_threads.store(0U, std::memory_order_release);
          g_native_counter_underflows.fetch_add(1U, std::memory_order_relaxed);
        }
      }
    } running_guard;
    std::invoke(function);
  });
}

bool TryAcquireReaperThreadPermit() noexcept {
  auto current = g_reaper_thread_permits.load(std::memory_order_acquire);
  while (current < kReaperThreadPermitCapacity) {
    if (g_reaper_thread_permits.compare_exchange_weak(
            current, current + 1U, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] std::size_t SaturatingAtomicTake(std::atomic<std::size_t> &target,
                                               std::size_t amount) noexcept {
  auto current = target.load(std::memory_order_acquire);
  while (true) {
    const auto removed = std::min(current, amount);
    const auto next = current - removed;
    if (current < amount) {
      g_native_counter_underflows.fetch_add(1U, std::memory_order_relaxed);
    }
    if (target.compare_exchange_weak(current, next, std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
      return removed;
    }
  }
}

[[nodiscard]] std::size_t TakePermitDomainOrQuarantine(
    std::atomic<std::size_t> &target, std::size_t amount,
    std::atomic<bool> &corrupted,
    std::atomic<std::size_t> *protocol_violations = nullptr) noexcept {
  auto current = target.load(std::memory_order_acquire);
  bool violation_recorded = false;
  for (;;) {
    const bool invalid = current < amount;
    if (invalid) {
      // Poison before attempting the debit. A concurrent acquisition that
      // already won its CAS performs a post-CAS poison check and retains that
      // tentative grant as conservative terminal debt, so an invalid release
      // can never be "made valid" by stealing capacity from a later owner.
      // Record each bad release once even if its CAS retries.
      corrupted.store(true, std::memory_order_release);
      if (!violation_recorded) {
        violation_recorded = true;
        g_native_counter_underflows.fetch_add(1U, std::memory_order_relaxed);
        if (protocol_violations != nullptr) {
          protocol_violations->fetch_add(1U, std::memory_order_relaxed);
        }
      }
    }
    const auto removed = std::min(current, amount);
    const auto next = current - removed;
    if (target.compare_exchange_weak(current, next, std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
      return removed;
    }
  }
}

void SaturatingAtomicSubtract(std::atomic<std::size_t> &target,
                              std::size_t amount) noexcept {
  static_cast<void>(SaturatingAtomicTake(target, amount));
}

void ReleaseReaperThreadPermit() noexcept {
  SaturatingAtomicSubtract(g_reaper_thread_permits, 1U);
}

struct ThreadPermitLedgerSnapshot final {
  std::size_t managed = 0U;
  std::size_t external = 0U;
  std::size_t total = 0U;
  bool stable = false;
};

[[nodiscard]] ThreadPermitLedgerSnapshot
ReadThreadPermitLedgerSnapshot() noexcept {
  ThreadPermitLedgerSnapshot out;
  for (std::size_t attempt = 0U; attempt < 4096U; ++attempt) {
    const auto epoch_before =
        g_process_thread_ledger_mutation_epoch.load(std::memory_order_acquire);
    if (g_process_thread_ledger_mutations_inflight.load(
            std::memory_order_acquire) != 0U) {
      std::this_thread::yield();
      continue;
    }
    out.managed =
        g_process_physical_thread_permits.load(std::memory_order_acquire);
    out.external = g_process_external_runtime_thread_permits.load(
        std::memory_order_acquire);
    out.total = g_process_total_thread_permits.load(std::memory_order_acquire);
    std::atomic_thread_fence(std::memory_order_acquire);
    const auto epoch_after =
        g_process_thread_ledger_mutation_epoch.load(std::memory_order_acquire);
    if (g_process_thread_ledger_mutations_inflight.load(
            std::memory_order_acquire) == 0U &&
        epoch_before == epoch_after &&
        out.managed <= std::numeric_limits<std::size_t>::max() - out.external &&
        out.total == out.managed + out.external) {
      out.stable = true;
      return out;
    }
    std::this_thread::yield();
  }
  return out;
}
} // namespace

std::uint64_t process_thread_stack_reservation_bytes() noexcept {
  return ThreadStackReservationBytes();
}

std::optional<std::size_t> process_physical_thread_count() noexcept {
  return ProcessPhysicalThreadCount();
}

std::size_t
acquire_process_physical_thread_permits(std::size_t desired,
                                        std::size_t minimum) noexcept {
  return TryAcquireProcessPhysicalThreadPermitsUpTo(desired, minimum);
}

void release_process_physical_thread_permits(std::size_t amount) noexcept {
  if (!runtime_owner_process() || amount == 0U) {
    return;
  }
  BeginThreadPermitLedgerMutation();
  const auto removed =
      TakePermitDomainOrQuarantine(g_process_physical_thread_permits, amount,
                                   g_process_thread_permit_corrupted);
  if (removed != 0U) {
    static_cast<void>(
        TakePermitDomainOrQuarantine(g_process_total_thread_permits, removed,
                                     g_process_thread_permit_corrupted));
  }
  EndThreadPermitLedgerMutation();
}

std::size_t
acquire_process_external_runtime_thread_permits(std::size_t desired,
                                                std::size_t minimum) noexcept {
  return TryAcquireProcessExternalRuntimeThreadPermitsUpTo(desired, minimum);
}

void release_process_external_runtime_thread_permits(
    std::size_t amount) noexcept {
  if (!runtime_owner_process() || amount == 0U) {
    return;
  }
  BeginThreadPermitLedgerMutation();
  const auto removed =
      TakePermitDomainOrQuarantine(g_process_external_runtime_thread_permits,
                                   amount, g_process_thread_permit_corrupted);
  if (removed != 0U) {
    static_cast<void>(
        TakePermitDomainOrQuarantine(g_process_total_thread_permits, removed,
                                     g_process_thread_permit_corrupted));
  }
  EndThreadPermitLedgerMutation();
}

void add_process_external_runtime_resident_threads(
    std::size_t amount) noexcept {
  if (!runtime_owner_process() || amount == 0U) {
    return;
  }
  constexpr std::size_t kSaneSingleObservation = 65536U;
  if (amount > kSaneSingleObservation || !ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  ExternalRuntimeResidencyWriterGuard writer;
  if (!ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  BeginThreadPermitLedgerMutation();
  auto current = g_process_external_runtime_resident_threads.load(
      std::memory_order_relaxed);
  const auto debt = g_process_external_runtime_stack_debt_threads.load(
      std::memory_order_relaxed);
  const auto max_size = std::numeric_limits<std::size_t>::max();
  if (amount > max_size - current) {
    QuarantineExternalRuntimeResidency();
    EndThreadPermitLedgerMutation();
    return;
  }
  const auto target = current + amount;
  if (target > debt) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    EndThreadPermitLedgerMutation();
    return;
  }
  // Retain a CAS publication even under the writer gate: source-contract tests
  // assert that resident identity never regressed to unchecked fetch_add.
  while (!g_process_external_runtime_resident_threads.compare_exchange_weak(
      current, target, std::memory_order_release, std::memory_order_relaxed)) {
    // The writer gate excludes competing stores; only a permitted spurious weak
    // CAS failure can retry here.
  }
  EndThreadPermitLedgerMutation();
}

void release_process_external_runtime_resident_threads(
    std::size_t amount) noexcept {
  if (!runtime_owner_process() || amount == 0U) {
    return;
  }
  if (!ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  ExternalRuntimeResidencyWriterGuard writer;
  if (!ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  BeginThreadPermitLedgerMutation();
  const auto current = g_process_external_runtime_resident_threads.load(
      std::memory_order_relaxed);
  if (amount > current) {
    QuarantineExternalRuntimeResidency();
    EndThreadPermitLedgerMutation();
    return;
  }
  g_process_external_runtime_resident_threads.store(current - amount,
                                                    std::memory_order_release);
  EndThreadPermitLedgerMutation();
}

void add_process_external_runtime_stack_debt_threads(
    std::size_t amount) noexcept {
  if (!runtime_owner_process() || amount == 0U) {
    return;
  }
  constexpr std::size_t kSaneSingleObservation = 65536U;
  if (amount > kSaneSingleObservation || !ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  ExternalRuntimeResidencyWriterGuard writer;
  if (!ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  BeginThreadPermitLedgerMutation();
  auto current = g_process_external_runtime_stack_debt_threads.load(
      std::memory_order_relaxed);
  const auto max_size = std::numeric_limits<std::size_t>::max();
  if (amount > max_size - current) {
    QuarantineExternalRuntimeResidency();
    EndThreadPermitLedgerMutation();
    return;
  }
  const auto target = current + amount;
  while (!g_process_external_runtime_stack_debt_threads.compare_exchange_weak(
      current, target, std::memory_order_release, std::memory_order_relaxed)) {
    // Writer exclusivity means a retry only handles a spurious weak-CAS
    // failure.
  }
  EndThreadPermitLedgerMutation();
}

void release_process_external_runtime_stack_debt_threads(
    std::size_t amount) noexcept {
  if (!runtime_owner_process() || amount == 0U) {
    return;
  }
  if (!ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  ExternalRuntimeResidencyWriterGuard writer;
  if (!ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  BeginThreadPermitLedgerMutation();
  const auto debt = g_process_external_runtime_stack_debt_threads.load(
      std::memory_order_relaxed);
  const auto identity = g_process_external_runtime_resident_threads.load(
      std::memory_order_relaxed);
  if (amount > debt || debt - amount < identity) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    EndThreadPermitLedgerMutation();
    return;
  }
  g_process_external_runtime_stack_debt_threads.store(
      debt - amount, std::memory_order_release);
  EndThreadPermitLedgerMutation();
}

void update_process_external_runtime_residency(
    std::int64_t identity_delta, std::int64_t stack_debt_delta) noexcept {
  if (!runtime_owner_process()) {
    return;
  }
  constexpr std::uint64_t kSaneDelta = 65536U;
  const auto magnitude = [](std::int64_t value) noexcept -> std::uint64_t {
    if (value >= 0) {
      return static_cast<std::uint64_t>(value);
    }
    return static_cast<std::uint64_t>(-(value + 1)) + 1U;
  };
  if (magnitude(identity_delta) > kSaneDelta ||
      magnitude(stack_debt_delta) > kSaneDelta ||
      !ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  const auto apply_delta = [](std::size_t current, std::int64_t delta,
                              std::size_t *target) noexcept -> bool {
    if (delta >= 0) {
      const auto amount = static_cast<std::uint64_t>(delta);
      if (amount > std::numeric_limits<std::size_t>::max() - current) {
        return false;
      }
      *target = current + static_cast<std::size_t>(amount);
      return true;
    }
    const auto amount = static_cast<std::uint64_t>(-(delta + 1)) + 1U;
    if (amount > current) {
      return false;
    }
    *target = current - static_cast<std::size_t>(amount);
    return true;
  };

  ExternalRuntimeResidencyWriterGuard writer;
  if (!ExternalRuntimeResidencyHealthy()) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    return;
  }
  BeginThreadPermitLedgerMutation();
  // Validation occurs after exclusive writer authority is held and therefore
  // applies to the exact state that will be published.
  const auto current_identity =
      g_process_external_runtime_resident_threads.load(
          std::memory_order_relaxed);
  const auto current_debt = g_process_external_runtime_stack_debt_threads.load(
      std::memory_order_relaxed);
  std::size_t target_identity = 0U;
  std::size_t target_debt = 0U;
  if (!apply_delta(current_identity, identity_delta, &target_identity) ||
      !apply_delta(current_debt, stack_debt_delta, &target_debt) ||
      target_debt < target_identity) {
    g_external_runtime_resident_protocol_violations.fetch_add(
        1U, std::memory_order_relaxed);
    EndThreadPermitLedgerMutation();
    return;
  }
  // Publish in invariant-preserving order so lock-free single-field readers are
  // conservative even during the short transaction window.
  if (target_debt > current_debt) {
    g_process_external_runtime_stack_debt_threads.store(
        target_debt, std::memory_order_release);
  }
  if (target_identity < current_identity) {
    g_process_external_runtime_resident_threads.store(
        target_identity, std::memory_order_release);
  }
  if (target_identity > current_identity) {
    g_process_external_runtime_resident_threads.store(
        target_identity, std::memory_order_release);
  }
  if (target_debt < current_debt) {
    g_process_external_runtime_stack_debt_threads.store(
        target_debt, std::memory_order_release);
  }
  EndThreadPermitLedgerMutation();
}

std::size_t
acquire_process_file_descriptor_permits(std::size_t desired,
                                        std::size_t minimum) noexcept {
  const auto granted = TryAcquireProcessFdPermitsUpTo(desired, minimum);
  if (granted == 0U) {
    g_process_file_descriptor_rejections.fetch_add(1U,
                                                   std::memory_order_relaxed);
  }
  return granted;
}

void SkipCancelledFdTicketsLocked() noexcept {
  for (;;) {
    const auto serving =
        g_process_fd_serving_ticket.load(std::memory_order_acquire);
    auto &slot = g_process_fd_cancelled_tickets[static_cast<std::size_t>(
        serving % kProcessFdTicketSlots)];
    const auto encoded = slot.load(std::memory_order_acquire);
    if (encoded != serving + 1U) {
      return;
    }
    slot.store(0U, std::memory_order_release);
    g_process_fd_serving_ticket.store(serving + 1U, std::memory_order_release);
  }
}

void RetireFdTicketLocked(std::uint64_t ticket) noexcept {
  const auto serving =
      g_process_fd_serving_ticket.load(std::memory_order_acquire);
  if (serving == ticket) {
    g_process_fd_serving_ticket.store(ticket + 1U, std::memory_order_release);
    SkipCancelledFdTicketsLocked();
    return;
  }
  g_process_fd_cancelled_tickets[static_cast<std::size_t>(
                                     ticket % kProcessFdTicketSlots)]
      .store(ticket + 1U, std::memory_order_release);
}

bool TryReserveFdTicket(std::uint64_t *ticket_out) noexcept {
  if (ticket_out == nullptr) {
    return false;
  }
  auto next = g_process_fd_next_ticket.load(std::memory_order_acquire);
  for (;;) {
    const auto serving =
        g_process_fd_serving_ticket.load(std::memory_order_acquire);
    if (next < serving) {
      // Ticket wrap is never allowed while a generation is live.
      return false;
    }
    const auto backlog = next - serving;
    if (backlog >= static_cast<std::uint64_t>(kProcessFdTicketSlots - 1U) ||
        next == std::numeric_limits<std::uint64_t>::max()) {
      return false;
    }
    if (g_process_fd_next_ticket.compare_exchange_weak(
            next, next + 1U, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      *ticket_out = next;
      return true;
    }
  }
}

std::size_t acquire_process_file_descriptor_permits_wait(
    std::size_t desired, std::size_t minimum,
    std::uint64_t timeout_millis) noexcept {
  if (!runtime_owner_process()) {
    g_process_file_descriptor_rejections.fetch_add(1U,
                                                   std::memory_order_relaxed);
    return 0U;
  }
  if (timeout_millis == 0U) {
    const auto granted = TryAcquireProcessFdPermitsUpTo(desired, minimum);
    if (granted == 0U) {
      g_process_file_descriptor_rejections.fetch_add(1U,
                                                     std::memory_order_relaxed);
    }
    return granted;
  }
  std::uint64_t ticket = 0U;
  // Admission is bounded by unresolved ticket distance, not by the number of
  // currently live waiter threads. Timed-out followers leave tombstones until
  // serving reaches them, so waiter-count admission could otherwise reuse a
  // ring slot and erase an unconsumed cancellation generation.
  if (!TryReserveFdTicket(&ticket)) {
    g_process_file_descriptor_rejections.fetch_add(1U,
                                                   std::memory_order_relaxed);
    return 0U;
  }
  struct WaiterGuard final {
    WaiterGuard() noexcept {
      g_process_fd_waiters.fetch_add(1U, std::memory_order_acq_rel);
    }
    ~WaiterGuard() {
      g_process_fd_waiters.fetch_sub(1U, std::memory_order_release);
    }
  } waiter_guard;
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_millis);
  constexpr auto kExternalObservationPoll = std::chrono::milliseconds(50);
  std::unique_lock<std::mutex> lock(g_process_fd_wait_mutex);
  for (;;) {
    SkipCancelledFdTicketsLocked();
    if (!runtime_owner_process() ||
        g_process_file_descriptor_permit_corrupted.load(
            std::memory_order_acquire)) {
      RetireFdTicketLocked(ticket);
      g_process_fd_wait_cv.notify_all();
      g_process_file_descriptor_rejections.fetch_add(1U,
                                                     std::memory_order_relaxed);
      return 0U;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      RetireFdTicketLocked(ticket);
      g_process_fd_wait_cv.notify_all();
      g_process_file_descriptor_rejections.fetch_add(1U,
                                                     std::memory_order_relaxed);
      return 0U;
    }
    if (g_process_fd_serving_ticket.load(std::memory_order_acquire) == ticket) {
      lock.unlock();
      const auto granted =
          TryAcquireProcessFdPermitsUpTo(desired, minimum, true);
      lock.lock();
      if (granted != 0U) {
        RetireFdTicketLocked(ticket);
        g_process_fd_wait_cv.notify_all();
        return granted;
      }
    }
    // Poll at a small bounded interval as well as on governor epochs so closing
    // an unrelated external FD (which cannot notify us) is still observed.
    const auto wake = std::min(deadline, std::chrono::steady_clock::now() +
                                             kExternalObservationPoll);
    const auto epoch = g_process_fd_epoch.load(std::memory_order_acquire);
    g_process_fd_wait_cv.wait_until(lock, wake, [&] {
      return g_process_fd_epoch.load(std::memory_order_acquire) != epoch ||
             g_process_fd_serving_ticket.load(std::memory_order_acquire) ==
                 ticket ||
             !runtime_owner_process() ||
             g_process_file_descriptor_permit_corrupted.load(
                 std::memory_order_acquire);
    });
  }
}

void release_process_file_descriptor_permits(std::size_t amount) noexcept {
  if (!runtime_owner_process() || amount == 0U) {
    return;
  }
  static_cast<void>(TakePermitDomainOrQuarantine(
      g_process_file_descriptor_permits, amount,
      g_process_file_descriptor_permit_corrupted,
      &g_process_file_descriptor_protocol_violations));
  {
    std::lock_guard<std::mutex> lock(g_process_fd_wait_mutex);
    g_process_fd_epoch.fetch_add(1U, std::memory_order_acq_rel);
  }
  g_process_fd_wait_cv.notify_all();
}

void mark_process_file_descriptors_opened(std::size_t amount) noexcept {
  if (!runtime_owner_process() || amount == 0U) {
    return;
  }
  const auto reserved =
      g_process_file_descriptor_permits.load(std::memory_order_acquire);
  auto opened =
      g_process_file_descriptors_opened.load(std::memory_order_acquire);
  while (opened < reserved) {
    const auto available = reserved - opened;
    const auto delta = std::min(amount, available);
    if (delta == 0U) {
      break;
    }
    if (g_process_file_descriptors_opened.compare_exchange_weak(
            opened, opened + delta, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      break;
    }
  }
}

void mark_process_file_descriptors_closed(std::size_t amount) noexcept {
  if (!runtime_owner_process() || amount == 0U) {
    return;
  }
  SaturatingAtomicSubtract(g_process_file_descriptors_opened, amount);
  {
    std::lock_guard<std::mutex> lock(g_process_fd_wait_mutex);
    g_process_fd_epoch.fetch_add(1U, std::memory_order_acq_rel);
  }
  g_process_fd_wait_cv.notify_all();
}

std::size_t process_file_descriptor_permits_in_use() noexcept {
  return g_process_file_descriptor_permits.load(std::memory_order_acquire);
}

std::size_t process_file_descriptors_opened() noexcept {
  return g_process_file_descriptors_opened.load(std::memory_order_acquire);
}

std::optional<std::size_t> process_file_descriptor_count() noexcept {
  return ProcessFileDescriptorCount();
}

std::size_t process_file_descriptor_rejections() noexcept {
  return g_process_file_descriptor_rejections.load(std::memory_order_acquire);
}

void record_process_file_descriptor_protocol_violation() noexcept {
  g_process_file_descriptor_protocol_violations.fetch_add(
      1U, std::memory_order_relaxed);
}

void record_process_file_descriptor_uncertain_close_debt(
    std::size_t amount) noexcept {
  if (amount != 0U) {
    g_process_file_descriptor_uncertain_close_debts.fetch_add(
        amount, std::memory_order_relaxed);
  }
}

std::size_t process_file_descriptor_protocol_violations() noexcept {
  return g_process_file_descriptor_protocol_violations.load(
      std::memory_order_acquire);
}

std::size_t process_file_descriptor_uncertain_close_debts() noexcept {
  return g_process_file_descriptor_uncertain_close_debts.load(
      std::memory_order_acquire);
}

std::size_t process_file_descriptor_capacity() noexcept {
  return ConfiguredProcessFdCapacity();
}

void mark_process_physical_thread_running() noexcept {
  g_managed_running_threads.fetch_add(1U, std::memory_order_acq_rel);
}

void mark_process_physical_thread_stopped() noexcept {
  SaturatingAtomicSubtract(g_managed_running_threads, 1U);
}

struct OperationTaskArena::DetachedMetrics final {
  explicit DetachedMetrics(std::size_t worker_count)
      : slot_count(std::max<std::size_t>(1U, worker_count)),
        live_since_ns(new std::atomic<std::int64_t>[slot_count]) {
    for (std::size_t index = 0; index < slot_count; ++index) {
      live_since_ns[index].store(0, std::memory_order_relaxed);
    }
  }

  const std::size_t slot_count;
  std::unique_ptr<std::atomic<std::int64_t>[]> live_since_ns;
  std::atomic<std::size_t> total{0U};
  std::atomic<std::size_t> reaper_queued_states{0U};
  std::atomic<std::size_t> reaper_active_states{0U};
  std::atomic<std::size_t> reaper_queued_bytes{0U};
  std::atomic<std::size_t> reaper_active_bytes{0U};

  [[nodiscard]] std::size_t Register(std::int64_t since_ns) noexcept {
    const auto normalized = std::max<std::int64_t>(1, since_ns);
    for (std::size_t index = 0; index < slot_count; ++index) {
      auto empty = std::int64_t{0};
      if (live_since_ns[index].compare_exchange_strong(
              empty, normalized, std::memory_order_acq_rel,
              std::memory_order_relaxed)) {
        total.fetch_add(1U, std::memory_order_relaxed);
        g_detached_workers.fetch_add(1U, std::memory_order_relaxed);
        return index + 1U;
      }
    }
    return 0U;
  }
  void Complete(std::size_t id) noexcept {
    if (id == 0U || id > slot_count) {
      return;
    }
    const auto previous =
        live_since_ns[id - 1U].exchange(0, std::memory_order_acq_rel);
    if (previous != 0) {
      SaturatingAtomicSubtract(g_detached_workers, 1U);
    }
  }
  [[nodiscard]] std::size_t Current() const noexcept {
    std::size_t current = 0U;
    for (std::size_t index = 0; index < slot_count; ++index) {
      current += static_cast<std::size_t>(
          live_since_ns[index].load(std::memory_order_acquire) != 0);
    }
    return current;
  }
  [[nodiscard]] std::int64_t OldestSinceNs() const noexcept {
    std::int64_t oldest = 0;
    for (std::size_t index = 0; index < slot_count; ++index) {
      const auto since = live_since_ns[index].load(std::memory_order_acquire);
      if (since != 0 && (oldest == 0 || since < oldest)) {
        oldest = since;
      }
    }
    return oldest;
  }
};
namespace {
constexpr auto kArenaShutdownDrain = std::chrono::seconds(2);
std::atomic<std::uint64_t> g_arena_generation{0};

[[nodiscard]] std::uint64_t NextArenaGeneration() noexcept {
  auto current = g_arena_generation.load(std::memory_order_relaxed);
  for (;;) {
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      return 0U;
    }
    if (g_arena_generation.compare_exchange_weak(current, current + 1U,
                                                 std::memory_order_relaxed,
                                                 std::memory_order_relaxed)) {
      return current + 1U;
    }
  }
}

class ArenaWorkerThread final {
public:
  struct Completion final {
    std::mutex mutex;
    std::condition_variable ready;
    std::condition_variable stopped_ready;
    bool done = false;
    std::shared_ptr<OperationTaskArena::DetachedMetrics> detached_metrics;
    std::size_t detached_id = 0U;
  };

  template <class Function>
  explicit ArenaWorkerThread(Function &&function)
      : stop_source_(), completion_(std::make_shared<Completion>()) {
    thread_ = StartGovernedNativeThread(
        [token = stop_source_.get_token(), completion = completion_,
         function = std::forward<Function>(function)]() mutable {
          try {
            std::invoke(function, token);
          } catch (...) {
            std::terminate();
          }
          std::shared_ptr<OperationTaskArena::DetachedMetrics> metrics;
          std::size_t detached_id = 0U;
          {
            std::lock_guard lock(completion->mutex);
            completion->done = true;
            metrics = std::move(completion->detached_metrics);
            detached_id = completion->detached_id;
            completion->detached_id = 0U;
          }
          completion->ready.notify_all();
          if (metrics) {
            metrics->Complete(detached_id);
          }
        });
  }

  ArenaWorkerThread(const ArenaWorkerThread &) = delete;
  ArenaWorkerThread &operator=(const ArenaWorkerThread &) = delete;

  ~ArenaWorkerThread() {
    if (thread_.joinable()) {
      stop_source_.request_stop();
      thread_.detach();
    }
  }

  bool request_stop() noexcept { return stop_source_.request_stop(); }

  [[nodiscard]] bool
  wait_until(std::chrono::steady_clock::time_point deadline) const noexcept {
    std::unique_lock lock(completion_->mutex);
    return completion_->ready.wait_until(lock, deadline,
                                         [this] { return completion_->done; });
  }

  void mark_detached(
      const std::shared_ptr<OperationTaskArena::DetachedMetrics> &metrics,
      std::size_t detached_id) noexcept {
    bool already_done = false;
    {
      std::lock_guard lock(completion_->mutex);
      already_done = completion_->done;
      if (!already_done) {
        completion_->detached_metrics = metrics;
        completion_->detached_id = detached_id;
      }
    }
    if (already_done && metrics) {
      metrics->Complete(detached_id);
    }
  }

  void join() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  void detach() noexcept {
    if (thread_.joinable()) {
      thread_.detach();
    }
  }

private:
  StopSource stop_source_;
  std::shared_ptr<Completion> completion_;
  std::thread thread_;
};
} // namespace

struct OperationTaskArena::State final {
  struct QueuedTask final {
    Task task;
    std::size_t lane_begin = 0;
    std::size_t lane_end = 1;
    TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther;
    std::int64_t queued_at_ns = 0;
    std::size_t retained_bytes = 256U;
  };
  static std::size_t QueueCapacity(
      std::size_t count,
      const std::shared_ptr<PerformanceTelemetry> &telemetry) noexcept {
    constexpr std::size_t kTasksPerWorker = 256U;
    const auto worker_bound =
        count >= 32U
            ? 8192U
            : (count > std::numeric_limits<std::size_t>::max() / kTasksPerWorker
                   ? std::numeric_limits<std::size_t>::max()
                   : count * kTasksPerWorker);
    if (!telemetry || telemetry->memory_limit_bytes() <= 0) {
      return worker_bound;
    }
    // The deque allocator is charged to the operation pool, but retain an
    // explicit conservative metadata ceiling as a final admission invariant.
    constexpr std::size_t kEstimatedQueuedTaskBytes = 256U;
    const auto memory_bound = std::max<std::size_t>(
        1U, static_cast<std::size_t>(telemetry->memory_limit_bytes()) /
                kEstimatedQueuedTaskBytes);
    return std::min(worker_bound, memory_bound);
  }
  static std::size_t QueueByteCapacity(
      std::size_t task_capacity,
      const std::shared_ptr<PerformanceTelemetry> &telemetry) noexcept {
    constexpr std::size_t kDefaultCharge = 256U;
    // Uncharged submissions are rejected at half this byte ceiling so the
    // other half remains available for active work. Account for that safety
    // split when deriving the byte capacity from the task-count capacity;
    // otherwise an otherwise valid queue is rejected halfway through.
    constexpr std::size_t kUnknownChargeHeadroom = 2U;
    constexpr std::size_t kBytesPerTask =
        kDefaultCharge * kUnknownChargeHeadroom;
    const auto task_bound =
        task_capacity > std::numeric_limits<std::size_t>::max() / kBytesPerTask
            ? std::numeric_limits<std::size_t>::max()
            : task_capacity * kBytesPerTask;
    if (!telemetry || telemetry->memory_limit_bytes() <= 0) {
      return task_bound;
    }
    const auto memory_limit =
        static_cast<std::size_t>(telemetry->memory_limit_bytes());
    if (memory_limit <= kDefaultCharge) {
      return std::min(task_bound, memory_limit);
    }
    // Queued closures are only one consumer of the operation budget. Retain
    // headroom for active tasks, result reordering, parser buffers, and output.
    // Task-count capacity and retained-byte capacity are independent bounds:
    // explicitly charged packets can be hundreds of KiB even when only two
    // workers are active. Deriving the byte ceiling from the 256-byte default
    // metadata charge makes two legitimate explicit packets mutually exclusive
    // (for two workers it capped the whole arena at 256 KiB). The operation
    // memory budget remains the authoritative physical ceiling, while this
    // independent quarter-budget is a conservative retained-ownership
    // backpressure envelope.
    return std::max<std::size_t>(kDefaultCharge, memory_limit / 4U);
  }
  static std::size_t ProducerWaiterCapacity(
      std::size_t count,
      const std::shared_ptr<PerformanceTelemetry> &telemetry) noexcept {
    constexpr std::size_t kMinWaiters = 64U;
    constexpr std::size_t kMaxWaiters = 2048U;
    const auto scaled = count > kMaxWaiters / 32U
                            ? kMaxWaiters
                            : std::max(kMinWaiters, count * 32U);
    if (!telemetry || telemetry->memory_limit_bytes() <= 0) {
      return std::min(kMaxWaiters, scaled);
    }
    constexpr std::size_t kEstimatedTicketBytes = 64U;
    const auto memory_bound = std::max<std::size_t>(
        1U, static_cast<std::size_t>(telemetry->memory_limit_bytes()) /
                (64U * kEstimatedTicketBytes));
    return std::max<std::size_t>(1U,
                                 std::min({kMaxWaiters, scaled, memory_bound}));
  }
  struct BackpressureWaitTicket final {
    bool active = false;
    std::uint64_t sequence = 0U;
    std::size_t requested_bytes = 0U;
    std::size_t bypasses = 0U;
    std::int64_t waiting_since_ns = 0;
  };
  struct alignas(64) QueueVisibilityShard final {
    // Global worker bits are split into bounded eight-worker publication
    // domains above eight workers. Narrow lanes therefore avoid contending on
    // one operation-wide queue-visibility cache line.
    std::atomic<std::uint64_t> nonempty_mask{0};
  };
  struct WorkerSlot final {
    QueueVisibilityShard *visibility = nullptr;
    std::mutex mutex;
    std::condition_variable_any ready;
    // Targeted wake generation. Producers only mutate the epoch of a worker
    // that must leave its park state, avoiding one operation-global cache line.
    alignas(64) std::atomic<std::uint64_t> wake_epoch{0};
    // Align the queue control block as well as the epoch itself. Member
    // alignment protects the bytes before wake_epoch, but without a second
    // boundary the deque could begin in the unused tail of the epoch line.
    // Producers/workers mutate deque control state independently from helper
    // and park-boundary epoch traffic, so keep both ownership domains apart.
    alignas(64) std::pmr::deque<QueuedTask> tasks;
    // Preallocated shutdown target. Swapping into this deque is
    // allocation-free.
    std::pmr::deque<QueuedTask> abandoned_tasks;
    // Exact mutex-owned counters avoid atomic read-modify-write operations on
    // the queue's hot cache line. Atomic snapshots preserve lock-free public
    // diagnostics and worker-selection reads.
    std::size_t queued_local = 0;
    std::atomic<std::size_t> queued{0};
    std::size_t submitted_local = 0;
    std::atomic<std::size_t> submitted{0};
    // The owning worker is the sole writer. Keep the exact value privately and
    // publish it with one relaxed store for lock-free bounded diagnostics.
    std::size_t stolen_local = 0;
    std::atomic<std::size_t> stolen{0};
    // Producers read running while the owning worker toggles it across dequeue
    // and activity streaks. Publishing before dequeue closes the empty-queue
    // window where a task is claimed but its worker still appears idle.
    // Keep that independently contended publication off the queue snapshot
    // line.
    alignas(64) std::atomic<bool> running{false};
    std::atomic<bool> first_task_pending{false};
    // Arenas wider than the compact mask path publish lifecycle state per
    // worker. This removes any fixed worker-count ceiling while preserving the
    // cache-efficient bitset scheduler for arenas up to 32 workers.
    std::atomic<bool> admitted{false};
    std::atomic<bool> started{false};
    std::atomic<bool> initialized{false};
    // Sampled once when the native worker starts. Wide-arena stealing first
    // searches workers from the same NUMA node and only then crosses nodes.
    std::atomic<int> locality_domain{-1};
    // Protected by mutex. Avoids searching queues that contain no dedicated
    // output work when bounded low-core preference is enabled.
    std::size_t dedicated_output_queued = 0;
    bool shallow_output_preference = false;
    std::mutex start_mutex;
    std::unique_ptr<ArenaWorkerThread> worker;

    explicit WorkerSlot(std::pmr::memory_resource *resource)
        : tasks(resource), abandoned_tasks(resource) {}
  };
  explicit State(std::size_t count,
                 std::shared_ptr<PerformanceTelemetry> telemetry_owner,
                 std::uint64_t generation_value)
      : generation(generation_value), worker_count(count),
        scalable_scan(count > 32U),
        queue_capacity(QueueCapacity(count, telemetry_owner)),
        queue_byte_capacity(QueueByteCapacity(queue_capacity, telemetry_owner)),
        producer_waiter_capacity(
            ProducerWaiterCapacity(count, telemetry_owner)),
        backpressure_tickets(producer_waiter_capacity),
        cpu_registration(process_cpu_governor().MakeRegistration(count)),
        telemetry(std::move(telemetry_owner)),
        operation_resource(std::make_shared<PoolResource>(
            telemetry ? std::static_pointer_cast<void>(telemetry->memory_pool())
                      : std::shared_ptr<void>{})),
        queue_resource(count > 1U ? operation_resource
                                  : std::make_shared<PoolResource>(
                                        std::shared_ptr<void>{})),
        admitted_dynamic(count), started_dynamic(count),
        initialized_dynamic(count), nonempty_dynamic(count) {}

  const std::uint64_t generation;
  const std::size_t worker_count;
  const bool scalable_scan;
  const std::size_t queue_capacity;
  const std::size_t queue_byte_capacity;
  const std::size_t producer_waiter_capacity;
  std::vector<BackpressureWaitTicket> backpressure_tickets;
  ProcessCpuGovernor::Registration cpu_registration;
  // The historical publication domain remains the sole 1-8-worker line and
  // the first high-core shard. Three additional aligned shards cover workers
  // 8-31.
  QueueVisibilityShard primary_queue_visibility;
  std::array<QueueVisibilityShard, 3> queue_visibility;
  std::shared_ptr<PerformanceTelemetry> telemetry;
  std::shared_ptr<PoolResource> operation_resource;
  std::shared_ptr<PoolResource> queue_resource;
  std::vector<std::unique_ptr<WorkerSlot>> slots;
  AtomicWorkerBitmap admitted_dynamic;
  AtomicWorkerBitmap started_dynamic;
  AtomicWorkerBitmap initialized_dynamic;
  AtomicWorkerBitmap nonempty_dynamic;
  // Stage producers reserve independent tickets while workers publish activity.
  // Keep each hot writer domain on its own bounded cache line so upstream,
  // output, all-lane, and worker activity traffic cannot invalidate unrelated
  // atomics. The cursor names and operations remain unchanged; this is purely
  // an internal layout optimization.
  alignas(64) std::atomic<std::size_t> upstream_cursor{0};
  alignas(64) std::atomic<std::size_t> output_cursor{0};
  alignas(64) std::atomic<std::size_t> all_cursor{0};
  alignas(64) std::atomic<bool> stopping{false};
  std::atomic<bool> cancel_requested{false};
  // Per-saturation timeout and optional absolute operation deadline. Both are
  // allocation-free atomics; every wait recomputes their effective minimum so
  // runtime shortening takes effect after the setter wakes waiters.
  std::atomic<std::int64_t> backpressure_timeout_ns{
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::seconds(30))
          .count()};
  std::atomic<std::int64_t> backpressure_deadline_ns{0};
  std::mutex inline_mutex;
  std::condition_variable inline_ready;
  std::atomic<std::size_t> inline_active{0};
  std::atomic<std::uint64_t> admitted_mask{0};
  std::atomic<std::uint64_t> started_mask{0};
  std::atomic<std::uint64_t> initialized_mask{0};
  alignas(64) std::atomic<std::size_t> active{0};
  std::atomic<std::size_t> peak_active{0};
  alignas(64) std::atomic<std::size_t> queued_total{0};
  std::atomic<std::size_t> peak_queued{0};
  std::atomic<std::size_t> rejected_submissions{0};
  std::atomic<std::size_t> output_preference_bypasses{0};
  std::atomic<std::size_t> queued_bytes{0};
  std::atomic<std::size_t> active_bytes{0};
  std::atomic<std::size_t> retained_bytes_total{0};
  // Versioned retained-byte availability plus a preallocated condition variable
  // provide deadline-bounded producer backpressure. Every release/shutdown
  // advances the epoch before notifying both legacy atomic observers and timed
  // waiters; no waiter holds a worker queue mutex while blocked.
  std::atomic<std::uint64_t> retained_epoch{0};
  std::mutex retained_wait_mutex;
  std::condition_variable retained_ready;
  void NotifyRetainedAvailability(bool wake_all = false) noexcept {
    // Serialize the epoch transition with the CV waiter's final recheck so a
    // release cannot land between that recheck and the atomic unlock+sleep.
    // This closes the classic lost-wakeup window while keeping queue mutexes
    // completely out of the backpressure wait path.
    {
      std::lock_guard retained_lock(retained_wait_mutex);
      retained_epoch.fetch_add(1U, std::memory_order_release);
    }
    (void)wake_all;
    // Waiters can request different byte charges. A single arbitrary wake can
    // select a request that still does not fit while a smaller request remains
    // asleep despite available credit. Wake the bounded waiter set and let the
    // authoritative CAS choose requests that fit.
    // Pass58 compatibility breadcrumb: retained_ready.notify_one() was used
    // here.
    retained_epoch.notify_all();
    retained_ready.notify_all();
  }
  std::atomic<std::size_t> peak_queued_bytes{0};
  std::atomic<std::size_t> peak_active_bytes{0};
  std::atomic<std::size_t> peak_retained_bytes_total{0};
  std::atomic<std::size_t> rejected_byte_submissions{0};
  std::atomic<std::size_t> backpressure_timeouts{0};
  std::atomic<std::size_t> logical_backpressure_timeouts{0};
  std::atomic<std::size_t> backpressure_waiters{0};
  std::atomic<std::size_t> peak_backpressure_waiters{0};
  std::atomic<std::size_t> rejected_backpressure_waiters{0};
  std::atomic<std::size_t> backpressure_bypasses{0};
  std::atomic<std::size_t> starvation_preventions{0};
  std::uint64_t backpressure_ticket_sequence = 0U; // retained_wait_mutex owned
  std::atomic<std::size_t> unknown_charge_submissions{0};
  std::atomic<std::size_t> inline_submitted{0};
  std::atomic<std::size_t> abandoned_queued_tasks{0};
  std::atomic<std::size_t> abandoned_queued_bytes{0};
  // Intrusive process-wide reaper node. It is allocated with State at Make(),
  // so Shutdown() never needs to allocate a cleanup container.
  State *reaper_next = nullptr;
  std::shared_ptr<State> reaper_self;
  std::shared_ptr<DetachedMetrics> reaper_metrics;
  std::size_t reaper_bytes = 0U;
  std::atomic<std::size_t> reaper_reserved_bytes{0U};
  bool reaper_reserved = false;
  std::int64_t reaper_parked_since_ns = 0;
};

namespace {

inline void UpdateEarlyPeak(std::atomic<std::size_t> &peak,
                            std::size_t value) noexcept {
  auto current = peak.load(std::memory_order_relaxed);
  while (current < value &&
         !peak.compare_exchange_weak(current, value, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
  }
}

sanitize::Status AcquireRetainedSubmitCredit(
    const std::shared_ptr<OperationTaskArena::State> &state,
    std::size_t retained_bytes) {
  constexpr auto kRetainedBackpressureDeadline = std::chrono::seconds(30);
  constexpr std::size_t kMaxOldestBypasses = 4U;
  constexpr std::size_t kNoTicket = std::numeric_limits<std::size_t>::max();
  const auto backpressure_started_at = std::chrono::steady_clock::now();
  const auto hard_wait_deadline =
      backpressure_started_at + kRetainedBackpressureDeadline;
  std::size_t ticket_index = kNoTicket;

  const auto retire_ticket_locked = [&]() noexcept {
    if (ticket_index == kNoTicket ||
        ticket_index >= state->backpressure_tickets.size()) {
      return;
    }
    auto &ticket = state->backpressure_tickets[ticket_index];
    if (!ticket.active) {
      ticket_index = kNoTicket;
      return;
    }
    ticket = OperationTaskArena::State::BackpressureWaitTicket{};
    SaturatingAtomicSubtract(state->backpressure_waiters, 1U);
    ticket_index = kNoTicket;
    state->retained_epoch.fetch_add(1U, std::memory_order_release);
  };

  const auto retire_ticket = [&]() noexcept {
    bool wake = false;
    {
      std::lock_guard lock(state->retained_wait_mutex);
      wake = ticket_index != kNoTicket;
      retire_ticket_locked();
    }
    if (wake) {
      state->retained_epoch.notify_all();
      state->retained_ready.notify_all();
    }
  };

  const auto register_ticket = [&]() -> bool {
    std::lock_guard lock(state->retained_wait_mutex);
    if (ticket_index != kNoTicket) {
      return true;
    }
    std::size_t free_index = kNoTicket;
    for (std::size_t index = 0U; index < state->backpressure_tickets.size();
         ++index) {
      if (!state->backpressure_tickets[index].active) {
        free_index = index;
        break;
      }
    }
    if (free_index == kNoTicket) {
      state->rejected_backpressure_waiters.fetch_add(1U,
                                                     std::memory_order_relaxed);
      return false;
    }
    auto next_sequence = state->backpressure_ticket_sequence + 1U;
    if (next_sequence == 0U) {
      next_sequence = 1U;
    }
    state->backpressure_ticket_sequence = next_sequence;
    auto &ticket = state->backpressure_tickets[free_index];
    ticket.active = true;
    ticket.sequence = next_sequence;
    ticket.requested_bytes = retained_bytes;
    ticket.bypasses = 0U;
    ticket.waiting_since_ns = PerformanceTelemetry::NowNs();
    ticket_index = free_index;
    const auto waiters =
        state->backpressure_waiters.fetch_add(1U, std::memory_order_acq_rel) +
        1U;
    UpdateEarlyPeak(state->peak_backpressure_waiters, waiters);
    return true;
  };

  const auto try_admit = [&]() -> bool {
    std::lock_guard lock(state->retained_wait_mutex);
    if (ticket_index == kNoTicket) {
      // A new producer never leapfrogs an existing waiter. This is what turns
      // the bounded-bypass rule into a persistent fairness guarantee.
      if (state->backpressure_waiters.load(std::memory_order_acquire) != 0U) {
        return false;
      }
      auto retained_total =
          state->retained_bytes_total.load(std::memory_order_acquire);
      while (retained_total <= state->queue_byte_capacity - retained_bytes) {
        if (state->retained_bytes_total.compare_exchange_weak(
                retained_total, retained_total + retained_bytes,
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
          UpdateEarlyPeak(state->peak_retained_bytes_total,
                          retained_total + retained_bytes);
          return true;
        }
      }
      return false;
    }

    std::size_t oldest_index = kNoTicket;
    std::uint64_t oldest_sequence = std::numeric_limits<std::uint64_t>::max();
    for (std::size_t index = 0U; index < state->backpressure_tickets.size();
         ++index) {
      const auto &candidate = state->backpressure_tickets[index];
      if (candidate.active && candidate.sequence < oldest_sequence) {
        oldest_sequence = candidate.sequence;
        oldest_index = index;
      }
    }
    if (oldest_index == kNoTicket ||
        ticket_index >= state->backpressure_tickets.size() ||
        !state->backpressure_tickets[ticket_index].active) {
      return false;
    }
    auto &oldest = state->backpressure_tickets[oldest_index];
    if (ticket_index != oldest_index && oldest.bypasses >= kMaxOldestBypasses) {
      state->starvation_preventions.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }

    auto retained_total =
        state->retained_bytes_total.load(std::memory_order_acquire);
    while (retained_total <= state->queue_byte_capacity - retained_bytes) {
      if (state->retained_bytes_total.compare_exchange_weak(
              retained_total, retained_total + retained_bytes,
              std::memory_order_acq_rel, std::memory_order_relaxed)) {
        if (ticket_index != oldest_index) {
          ++oldest.bypasses;
          state->backpressure_bypasses.fetch_add(1U, std::memory_order_relaxed);
        }
        UpdateEarlyPeak(state->peak_retained_bytes_total,
                        retained_total + retained_bytes);
        retire_ticket_locked();
        return true;
      }
    }
    return false;
  };

  while (true) {
    if (state->stopping.load(std::memory_order_acquire) ||
        state->cancel_requested.load(std::memory_order_acquire)) {
      retire_ticket();
      return sanitize::Status::Cancelled(
          "OperationTaskArena::Submit: cancelled during byte backpressure");
    }
    if (try_admit()) {
      if (ticket_index == kNoTicket) {
        state->retained_ready.notify_all();
      }
      return sanitize::Status::OK();
    }

    const auto retained_total =
        state->retained_bytes_total.load(std::memory_order_acquire);
    const auto queued_bytes =
        state->queued_bytes.load(std::memory_order_acquire);
    const auto active_bytes =
        state->active_bytes.load(std::memory_order_acquire);
    if (queued_bytes == 0U && active_bytes == 0U) {
      retire_ticket();
      state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Submit: retained byte capacity exhausted by "
          "completion ownership (retained=",
          retained_total, ", requested=", retained_bytes,
          ", capacity=", state->queue_byte_capacity, ")");
    }

    if (!register_ticket()) {
      state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Submit: producer backpressure waiter capacity "
          "exhausted");
    }

    bool availability_changed = false;
    bool logical_deadline_is_effective = false;
    std::uint64_t epoch = 0U;
    {
      auto retained_wait_deadline = hard_wait_deadline;
      const auto timeout_ns =
          state->backpressure_timeout_ns.load(std::memory_order_acquire);
      if (timeout_ns > 0) {
        retained_wait_deadline = std::min(
            retained_wait_deadline,
            backpressure_started_at + std::chrono::nanoseconds(timeout_ns));
      }
      const auto logical_deadline_ns =
          state->backpressure_deadline_ns.load(std::memory_order_acquire);
      if (logical_deadline_ns > 0) {
        const auto logical_deadline = std::chrono::steady_clock::time_point(
            std::chrono::nanoseconds(logical_deadline_ns));
        if (logical_deadline < retained_wait_deadline) {
          retained_wait_deadline = logical_deadline;
          logical_deadline_is_effective = true;
        }
      }
      std::unique_lock retained_lock(state->retained_wait_mutex);
      epoch = state->retained_epoch.load(std::memory_order_acquire);
      if (state->stopping.load(std::memory_order_acquire) ||
          state->cancel_requested.load(std::memory_order_acquire)) {
        availability_changed = true;
      } else {
        const auto wait_status = state->retained_ready.wait_until(
            retained_lock, retained_wait_deadline);
        availability_changed =
            wait_status != std::cv_status::timeout ||
            state->stopping.load(std::memory_order_acquire) ||
            state->cancel_requested.load(std::memory_order_acquire) ||
            state->retained_epoch.load(std::memory_order_acquire) != epoch;
      }
    }

    if (state->stopping.load(std::memory_order_acquire) ||
        state->cancel_requested.load(std::memory_order_acquire)) {
      retire_ticket();
      return sanitize::Status::Cancelled(
          "OperationTaskArena::Submit: cancelled during byte backpressure");
    }
    if (!availability_changed &&
        state->retained_epoch.load(std::memory_order_acquire) == epoch) {
      retire_ticket();
      state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
      state->backpressure_timeouts.fetch_add(1U, std::memory_order_relaxed);
      if (logical_deadline_is_effective) {
        state->logical_backpressure_timeouts.fetch_add(
            1U, std::memory_order_relaxed);
        return sanitize::Status::Cancelled(
            "OperationTaskArena::Submit: operation backpressure deadline "
            "exceeded");
      }
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Submit: retained byte backpressure hard "
          "deadline "
          "exceeded");
    }
  }
}

class ArenaCleanupReaper final {
public:
  static ArenaCleanupReaper &Instance() noexcept {
    static ArenaCleanupReaper *instance =
        new (std::nothrow) ArenaCleanupReaper();
    static ArenaCleanupReaper fallback(false);
    static const bool registered = []() noexcept {
      if (instance != nullptr) {
        std::atexit([]() noexcept {
          (void)ArenaCleanupReaper::Instance().ShutdownFor(100U);
        });
      }
      return true;
    }();
    (void)registered;
    return instance != nullptr ? *instance : fallback;
  }

  [[nodiscard]] bool
  Reserve(const std::shared_ptr<OperationTaskArena::State> &state,
          std::size_t /*maximum_bytes*/) noexcept {
    if (!enabled_ || !state) {
      over_capacity_.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
    for (std::size_t index = 0; index < kLaneCount; ++index) {
      PromoteTerminal(index);
      if (parked_by_lane_[index].load(std::memory_order_acquire) != 0U &&
          EnsureLaneStarted(index)) {
        lanes_[index].ready.notify_one();
      }
    }
    if (parked_states_.load(std::memory_order_acquire) != 0U ||
        terminal_states_.load(std::memory_order_acquire) != 0U) {
      over_capacity_.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
    const auto index = LaneIndex(state.get());
    auto &lane = lanes_[index];
    {
      std::lock_guard lock(lane.mutex);
      if (lane.stopping || lane.reserved_states >= kMaxQueuedStates) {
        over_capacity_.fetch_add(1U, std::memory_order_relaxed);
        return false;
      }
      ++lane.reserved_states;
      state->reaper_reserved = true;
      state->reaper_reserved_bytes.store(0U, std::memory_order_release);
    }
    // Reservation is deliberately passive. Most arenas shut down cleanly and
    // never need a reaper; starting its lanes here would add process threads to
    // every single-threaded operation. Enqueue/Park start a lane only after a
    // bounded shutdown actually leaves work behind.
    return true;
  }

  [[nodiscard]] bool
  ReserveQueuedBytes(const std::shared_ptr<OperationTaskArena::State> &state,
                     std::size_t bytes) noexcept {
    if (!enabled_ || !state || !state->reaper_reserved || bytes == 0U) {
      return bytes == 0U;
    }
    auto &reserved = lanes_[LaneIndex(state.get())].reserved_bytes;
    auto current = reserved.load(std::memory_order_acquire);
    for (;;) {
      if (bytes > kMaxQueuedBytes || current > kMaxQueuedBytes - bytes) {
        over_capacity_.fetch_add(1U, std::memory_order_relaxed);
        return false;
      }
      if (reserved.compare_exchange_weak(current, current + bytes,
                                         std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
        state->reaper_reserved_bytes.fetch_add(bytes,
                                               std::memory_order_acq_rel);
        return true;
      }
    }
  }

  void
  ReleaseQueuedBytes(const std::shared_ptr<OperationTaskArena::State> &state,
                     std::size_t bytes) noexcept {
    if (!state || bytes == 0U) {
      return;
    }
    SaturatingAtomicSubtract(lanes_[LaneIndex(state.get())].reserved_bytes,
                             bytes);
    SaturatingAtomicSubtract(state->reaper_reserved_bytes, bytes);
  }

  void ReleaseReservation(
      const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
    if (!state || !state->reaper_reserved) {
      return;
    }
    const auto index = LaneIndex(state.get());
    auto &lane = lanes_[index];
    const auto bytes =
        state->reaper_reserved_bytes.exchange(0U, std::memory_order_acq_rel);
    if (bytes != 0U) {
      SaturatingAtomicSubtract(lane.reserved_bytes, bytes);
    }
    std::lock_guard lock(lane.mutex);
    lane.reserved_states =
        lane.reserved_states > 0U ? lane.reserved_states - 1U : 0U;
    state->reaper_reserved = false;
  }

  [[nodiscard]] bool EnsureLaneStarted(std::size_t index) noexcept {
    if (!enabled_ || index >= kLaneCount) {
      return false;
    }
    auto &lane = lanes_[index];
    std::lock_guard lock(lane.mutex);
    if (lane.started) {
      return true;
    }
    if (lane.stopping || !TryAcquireReaperThreadPermit()) {
      return false;
    }
    lane.thread_permit = true;
    try {
      lane.stopped = false;
      lane.worker = StartGovernedNativeThread([this, index] { Run(index); });
      lane.started = true;
      g_reaper_workers.fetch_add(1U, std::memory_order_relaxed);
      return true;
    } catch (...) {
      lane.stopped = true;
      lane.thread_permit = false;
      ReleaseReaperThreadPermit();
      g_reaper_thread_start_failures.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
  }

  [[nodiscard]] bool
  Enqueue(const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
    if (!state) {
      return true;
    }
    const auto reserved_bytes =
        state->reaper_reserved_bytes.load(std::memory_order_acquire);
    if (!state->reaper_reserved || state->reaper_bytes > reserved_bytes ||
        !EnsureLaneStarted(LaneIndex(state.get()))) {
      return false;
    }
    const auto index = LaneIndex(state.get());
    auto &lane = lanes_[index];
    {
      std::lock_guard lock(lane.mutex);
      if (lane.stopping || lane.reserved_states == 0U) {
        return false;
      }
      --lane.reserved_states;
      if (reserved_bytes != 0U) {
        SaturatingAtomicSubtract(lane.reserved_bytes, reserved_bytes);
      }
      state->reaper_reserved = false;
      state->reaper_reserved_bytes.store(0U, std::memory_order_release);
      state->reaper_self = state;
      state->reaper_next = nullptr;
      if (state->reaper_metrics) {
        state->reaper_metrics->reaper_queued_states.fetch_add(
            1U, std::memory_order_relaxed);
        state->reaper_metrics->reaper_queued_bytes.fetch_add(
            state->reaper_bytes, std::memory_order_relaxed);
      }
      if (lane.tail != nullptr) {
        lane.tail->reaper_next = state.get();
      } else {
        lane.head = state.get();
      }
      lane.tail = state.get();
      ++lane.queued_states;
      lane.queued_bytes += state->reaper_bytes;
    }
    lane.ready.notify_one();
    return true;
  }

  [[nodiscard]] bool
  Park(const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
    if (!state || !state->reaper_reserved) {
      return false;
    }
    const auto index = LaneIndex(state.get());
    bool parked = false;
    {
      std::lock_guard lock(parked_mutex_);
      for (auto &slot : parked_) {
        if (!slot) {
          slot = state;
          state->reaper_parked_since_ns = PerformanceTelemetry::NowNs();
          parked_states_.fetch_add(1U, std::memory_order_relaxed);
          parked_bytes_.fetch_add(state->reaper_bytes,
                                  std::memory_order_relaxed);
          parked_by_lane_[index].fetch_add(1U, std::memory_order_relaxed);
          parked = true;
          break;
        }
      }
    }
    if (parked && EnsureLaneStarted(index)) {
      lanes_[index].ready.notify_one();
    }
    return parked;
  }

  [[nodiscard]] bool Terminalize(
      const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
    if (!state || !state->reaper_reserved) {
      return false;
    }
    std::lock_guard lock(terminal_mutex_);
    for (auto &slot : terminal_) {
      if (!slot) {
        slot = state;
        state->reaper_parked_since_ns = PerformanceTelemetry::NowNs();
        terminal_states_.fetch_add(1U, std::memory_order_relaxed);
        terminal_bytes_.fetch_add(state->reaper_bytes,
                                  std::memory_order_relaxed);
        return true;
      }
    }
    over_capacity_.fetch_add(1U, std::memory_order_relaxed);
    return false;
  }

  [[nodiscard]] std::size_t ParkedStates() const noexcept {
    return parked_states_.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool ShutdownFor(std::uint64_t timeout_millis) noexcept {
    if (!enabled_) {
      return true;
    }
    const auto timeout = std::chrono::milliseconds(timeout_millis);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (auto &lane : lanes_) {
      {
        std::lock_guard lock(lane.mutex);
        lane.stopping = true;
      }
      lane.ready.notify_all();
    }
    bool all_stopped = true;
    for (auto &lane : lanes_) {
      std::unique_lock lock(lane.mutex);
      if (lane.started && !lane.stopped) {
        if (!lane.stopped_ready.wait_until(lock, deadline,
                                           [&lane] { return lane.stopped; })) {
          all_stopped = false;
        }
      }
      const bool can_join = lane.stopped;
      lock.unlock();
      if (can_join && lane.worker.joinable() &&
          lane.worker.get_id() != std::this_thread::get_id()) {
        try {
          lane.worker.join();
        } catch (...) {
          all_stopped = false;
        }
      }
    }
    return all_stopped;
  }

  [[nodiscard]] bool
  DrainAndShutdownFor(std::uint64_t timeout_millis) noexcept {
    if (!enabled_) {
      return true;
    }
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_millis);
    while (std::chrono::steady_clock::now() < deadline) {
      for (std::size_t index = 0; index < kLaneCount; ++index) {
        if (parked_by_lane_[index].load(std::memory_order_acquire) != 0U) {
          if (EnsureLaneStarted(index)) {
            lanes_[index].ready.notify_one();
          }
        }
        PromoteTerminal(index);
      }
      const auto snapshot = Snapshot();
      const bool producers_quiescent = snapshot.live_arenas == 0U &&
                                       snapshot.detached_workers == 0U &&
                                       snapshot.reaper_reserved_states == 0U;
      const bool consumers_drained = snapshot.reaper_queued_states == 0U &&
                                     snapshot.reaper_active_states == 0U &&
                                     snapshot.reaper_parked_states == 0U &&
                                     snapshot.reaper_terminal_states == 0U;
      if (producers_quiescent && consumers_drained) {
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
        return ShutdownFor(static_cast<std::uint64_t>(
            std::max<std::int64_t>(0, remaining.count())));
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // Keep consumers alive after a timed-out attempt. A later arena shutdown
    // must still have a reaper capable of draining its reserved destination.
    return false;
  }

  void Shutdown() noexcept {
    // Process teardown must never wait indefinitely for arbitrary capture
    // destructors.  The intentionally leaked singleton and OS process cleanup
    // own any states that outlive this bounded terminal attempt.
    (void)ShutdownFor(100U);
  }

  [[nodiscard]] OperationTaskArenaRuntimeSnapshot Snapshot() noexcept {
    OperationTaskArenaRuntimeSnapshot out;
    out.live_arenas = g_live_arena_states.load(std::memory_order_acquire);
    out.detached_workers = g_detached_workers.load(std::memory_order_acquire);
    out.reaper_workers = g_reaper_workers.load(std::memory_order_acquire);
    out.reaper_parked_states = parked_states_.load(std::memory_order_acquire);
    out.counter_underflows =
        g_native_counter_underflows.load(std::memory_order_acquire);
    out.reaper_parked_bytes = parked_bytes_.load(std::memory_order_acquire);
    out.reaper_thread_permits =
        g_reaper_thread_permits.load(std::memory_order_acquire);
    out.reaper_thread_start_failures =
        g_reaper_thread_start_failures.load(std::memory_order_acquire);
    const auto permit_ledger = ReadThreadPermitLedgerSnapshot();
    out.native_physical_threads = permit_ledger.managed;
    out.native_physical_thread_capacity = NativePhysicalThreadCapacity();
    out.native_physical_thread_rejections =
        g_native_physical_thread_rejections.load(std::memory_order_acquire);
    out.external_runtime_thread_permits = permit_ledger.external;
    out.total_physical_thread_permits = permit_ledger.total;
    out.thread_permit_snapshot_stable = permit_ledger.stable;
    {
      ExternalRuntimeResidencyWriterGuard residency_reader;
      out.external_runtime_resident_threads =
          g_process_external_runtime_resident_threads.load(
              std::memory_order_relaxed);
      out.external_runtime_stack_debt_threads =
          g_process_external_runtime_stack_debt_threads.load(
              std::memory_order_relaxed);
    }
    out.external_runtime_resident_protocol_violations =
        g_external_runtime_resident_protocol_violations.load(
            std::memory_order_acquire);
    out.completion_memory_protocol_violations =
        g_completion_memory_protocol_violations.load(std::memory_order_acquire);
    out.reaper_over_capacity = over_capacity_.load(std::memory_order_acquire);
    out.reaper_terminal_states =
        terminal_states_.load(std::memory_order_acquire);
    out.reaper_terminal_bytes = terminal_bytes_.load(std::memory_order_acquire);
    {
      std::lock_guard lock(parked_mutex_);
      for (const auto &state : parked_) {
        if (state && state->reaper_parked_since_ns != 0 &&
            (out.oldest_parked_since_ns == 0 ||
             state->reaper_parked_since_ns < out.oldest_parked_since_ns)) {
          out.oldest_parked_since_ns = state->reaper_parked_since_ns;
        }
      }
    }
    {
      std::lock_guard lock(terminal_mutex_);
      for (const auto &state : terminal_) {
        if (state && state->reaper_parked_since_ns != 0 &&
            (out.oldest_terminal_since_ns == 0 ||
             state->reaper_parked_since_ns < out.oldest_terminal_since_ns)) {
          out.oldest_terminal_since_ns = state->reaper_parked_since_ns;
        }
      }
    }
    for (auto &lane : lanes_) {
      std::lock_guard lock(lane.mutex);
      out.reaper_queued_states += lane.queued_states;
      out.reaper_reserved_states += lane.reserved_states;
      out.reaper_active_states += lane.active_states;
      out.reaper_queued_bytes += lane.queued_bytes;
      out.reaper_active_bytes += lane.active_bytes;
      out.reaper_reserved_bytes +=
          lane.reserved_bytes.load(std::memory_order_acquire);
      out.reaper_stopping_lanes += static_cast<std::size_t>(lane.stopping);
    }
    return out;
  }

private:
  static constexpr std::size_t kLaneCount = 2U;
  static constexpr std::size_t kMaxQueuedStates = 1024U;
  static constexpr std::size_t kMaxQueuedBytes = 1024U * 1024U * 1024U;

  static std::size_t LaneIndex(OperationTaskArena::State *state) noexcept {
    return (reinterpret_cast<std::uintptr_t>(state) >> 6U) % kLaneCount;
  }

  struct Lane final {
    std::mutex mutex;
    std::condition_variable ready;
    std::condition_variable stopped_ready;
    OperationTaskArena::State *head = nullptr;
    OperationTaskArena::State *tail = nullptr;
    std::size_t queued_states = 0U;
    std::size_t queued_bytes = 0U;
    std::size_t reserved_states = 0U;
    std::atomic<std::size_t> reserved_bytes{0U};
    std::size_t active_states = 0U;
    std::size_t active_bytes = 0U;
    bool started = false;
    bool thread_permit = false;
    bool stopping = false;
    bool stopped = true;
    std::thread worker;
  };

  explicit ArenaCleanupReaper(bool enabled = true) noexcept
      : enabled_(enabled) {}

  static void
  SaturatingSubtract(std::atomic<std::size_t> &target, std::size_t amount,
                     std::atomic<std::size_t> &violations) noexcept {
    auto current = target.load(std::memory_order_acquire);
    while (true) {
      const auto next = current >= amount ? current - amount : 0U;
      if (current < amount) {
        violations.fetch_add(1U, std::memory_order_relaxed);
      }
      if (target.compare_exchange_weak(current, next, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
        return;
      }
    }
  }

  void Run(std::size_t index) noexcept {
    auto &lane = lanes_[index];
    while (true) {
      OperationTaskArena::State *raw = nullptr;
      {
        std::unique_lock lock(lane.mutex);
        lane.ready.wait(lock, [this, &lane, index] {
          return lane.stopping || lane.head != nullptr ||
                 parked_by_lane_[index].load(std::memory_order_acquire) != 0U;
        });
        if (lane.head == nullptr && !lane.stopping &&
            parked_by_lane_[index].load(std::memory_order_acquire) != 0U) {
          lock.unlock();
          PromoteParked(index);
          PromoteTerminal(index);
          continue;
        }
        if (lane.head == nullptr && lane.stopping) {
          lane.stopped = true;
          lock.unlock();
          SaturatingAtomicSubtract(g_reaper_workers, 1U);
          if (lane.thread_permit) {
            lane.thread_permit = false;
            ReleaseReaperThreadPermit();
          }
          lane.stopped_ready.notify_all();
          return;
        }
        raw = lane.head;
        lane.head = raw->reaper_next;
        if (lane.head == nullptr) {
          lane.tail = nullptr;
        }
        raw->reaper_next = nullptr;
        lane.queued_states =
            lane.queued_states > 0U ? lane.queued_states - 1U : 0U;
        lane.queued_bytes = lane.queued_bytes >= raw->reaper_bytes
                                ? lane.queued_bytes - raw->reaper_bytes
                                : 0U;
        ++lane.active_states;
        lane.active_bytes += raw->reaper_bytes;
      }
      auto state = std::move(raw->reaper_self);
      const auto bytes = state->reaper_bytes;
      const auto metrics = state->reaper_metrics;
      if (metrics) {
        SaturatingAtomicSubtract(metrics->reaper_queued_states, 1U);
        SaturatingAtomicSubtract(metrics->reaper_queued_bytes, bytes);
        metrics->reaper_active_states.fetch_add(1U, std::memory_order_relaxed);
        metrics->reaper_active_bytes.fetch_add(bytes,
                                               std::memory_order_relaxed);
      }
      for (auto &slot : state->slots) {
        slot->abandoned_tasks.clear();
      }
      if (bytes > 0U) {
        SaturatingSubtract(state->retained_bytes_total, bytes,
                           retained_underflows_);
        state->NotifyRetainedAvailability();
      }
      state->reaper_bytes = 0U;
      if (metrics) {
        SaturatingAtomicSubtract(metrics->reaper_active_states, 1U);
        SaturatingAtomicSubtract(metrics->reaper_active_bytes, bytes);
      }
      {
        std::lock_guard lock(lane.mutex);
        lane.active_states =
            lane.active_states > 0U ? lane.active_states - 1U : 0U;
        lane.active_bytes =
            lane.active_bytes >= bytes ? lane.active_bytes - bytes : 0U;
      }
      state.reset();
      PromoteParked(index);
      PromoteTerminal(index);
    }
  }

  void PromoteParked(std::size_t index) noexcept {
    std::shared_ptr<OperationTaskArena::State> candidate;
    std::size_t slot_index = parked_.size();
    {
      std::lock_guard lock(parked_mutex_);
      for (std::size_t i = 0; i < parked_.size(); ++i) {
        if (parked_[i] && LaneIndex(parked_[i].get()) == index) {
          candidate = parked_[i];
          slot_index = i;
          parked_[i].reset();
          break;
        }
      }
    }
    if (!candidate) {
      return;
    }
    SaturatingAtomicSubtract(parked_states_, 1U);
    SaturatingAtomicSubtract(parked_bytes_, candidate->reaper_bytes);
    SaturatingAtomicSubtract(parked_by_lane_[index], 1U);
    candidate->reaper_parked_since_ns = 0;
    if (!Enqueue(candidate)) {
      bool reinserted = false;
      {
        std::lock_guard lock(parked_mutex_);
        std::size_t target = parked_.size();
        if (slot_index < parked_.size() && !parked_[slot_index]) {
          target = slot_index;
        } else {
          for (std::size_t i = 0; i < parked_.size(); ++i) {
            if (!parked_[i]) {
              target = i;
              break;
            }
          }
        }
        candidate->reaper_parked_since_ns = PerformanceTelemetry::NowNs();
        if (target < parked_.size()) {
          parked_[target] = std::move(candidate);
          parked_states_.fetch_add(1U, std::memory_order_relaxed);
          parked_bytes_.fetch_add(parked_[target]->reaper_bytes,
                                  std::memory_order_relaxed);
          parked_by_lane_[index].fetch_add(1U, std::memory_order_relaxed);
          reinserted = true;
        }
      }
      if (!reinserted && !Terminalize(candidate)) {
        // Capacity is reserved at arena admission, so reaching this branch is
        // a hard invariant violation. Keep the process fail-closed rather than
        // running arbitrary destructors on the reaper thread. This replaces
        // the legacy hidden cycle: candidate->reaper_self = candidate.
        std::terminate();
      }
    }
  }

  void PromoteTerminal(std::size_t index) noexcept {
    std::shared_ptr<OperationTaskArena::State> candidate;
    {
      std::lock_guard lock(terminal_mutex_);
      for (auto &slot : terminal_) {
        if (slot && LaneIndex(slot.get()) == index) {
          candidate = std::move(slot);
          slot.reset();
          break;
        }
      }
    }
    if (!candidate) {
      return;
    }
    SaturatingAtomicSubtract(terminal_states_, 1U);
    SaturatingAtomicSubtract(terminal_bytes_, candidate->reaper_bytes);
    candidate->reaper_parked_since_ns = 0;
    if (Enqueue(candidate) || Park(candidate)) {
      return;
    }
    (void)Terminalize(candidate);
  }

  const bool enabled_;
  std::array<Lane, kLaneCount> lanes_;
  mutable std::mutex parked_mutex_;
  std::array<std::shared_ptr<OperationTaskArena::State>,
             kLaneCount * kMaxQueuedStates>
      parked_{};
  std::array<std::atomic<std::size_t>, kLaneCount> parked_by_lane_{};
  std::atomic<std::size_t> parked_states_{0U};
  std::atomic<std::size_t> parked_bytes_{0U};
  mutable std::mutex terminal_mutex_;
  std::array<std::shared_ptr<OperationTaskArena::State>,
             kLaneCount * kMaxQueuedStates>
      terminal_{};
  std::atomic<std::size_t> terminal_states_{0U};
  std::atomic<std::size_t> terminal_bytes_{0U};
  std::atomic<std::size_t> over_capacity_{0U};
  std::atomic<std::size_t> retained_underflows_{0U};
};

class TeardownReservationGuard final {
public:
  explicit TeardownReservationGuard(
      std::shared_ptr<OperationTaskArena::State> state) noexcept
      : state_(std::move(state)) {}
  TeardownReservationGuard(const TeardownReservationGuard &) = delete;
  TeardownReservationGuard &
  operator=(const TeardownReservationGuard &) = delete;
  ~TeardownReservationGuard() noexcept {
    if (state_) {
      ArenaCleanupReaper::Instance().ReleaseReservation(state_);
    }
  }
  void Commit() noexcept { state_.reset(); }

private:
  std::shared_ptr<OperationTaskArena::State> state_;
};

#include "internal/runtime/operation_task_arena_runtime.cc.inc"
} // namespace

bool OperationTaskArena::ShutdownCleanupReaper(
    std::uint64_t timeout_millis) noexcept {
  auto &reaper = ArenaCleanupReaper::Instance();
  const bool reaper_stopped = reaper.DrainAndShutdownFor(timeout_millis);
  const auto snapshot = reaper.Snapshot();
  return reaper_stopped && snapshot.live_arenas == 0U &&
         snapshot.detached_workers == 0U &&
         snapshot.reaper_queued_states == 0U &&
         snapshot.reaper_active_states == 0U &&
         snapshot.reaper_reserved_states == 0U &&
         snapshot.reaper_parked_states == 0U &&
         snapshot.reaper_terminal_states == 0U;
}

OperationTaskArenaRuntimeSnapshot
OperationTaskArena::RuntimeSnapshot() noexcept {
  return ArenaCleanupReaper::Instance().Snapshot();
}

OperationTaskArena::OperationTaskArena(
    std::shared_ptr<State> state,
    std::shared_ptr<DetachedMetrics> metrics) noexcept
    : state_(std::move(state)), detached_metrics_(std::move(metrics)) {}

sanitize::Result<std::shared_ptr<OperationTaskArena>>
OperationTaskArena::Make(std::size_t worker_count,
                         std::shared_ptr<PerformanceTelemetry> telemetry) {
  const auto normalized = std::max<std::size_t>(1, worker_count);
  const auto arena_generation = NextArenaGeneration();
  if (arena_generation == 0U) {
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Make: generation namespace exhausted");
  }
  std::shared_ptr<State> state;
  std::shared_ptr<DetachedMetrics> metrics;
  try {
    metrics = std::make_shared<DetachedMetrics>(normalized);
    state = std::make_shared<State>(normalized, std::move(telemetry),
                                    arena_generation);
    state->reaper_metrics = metrics;
    state->slots.reserve(normalized);
    for (std::size_t index = 0; index < normalized; ++index) {
      auto slot =
          std::make_unique<State::WorkerSlot>(state->queue_resource.get());
      if (!state->scalable_scan) {
        slot->visibility = index < 8U
                               ? &state->primary_queue_visibility
                               : &state->queue_visibility[(index >> 3U) - 1U];
      }
      state->slots.push_back(std::move(slot));
    }
    if (!ArenaCleanupReaper::Instance().Reserve(state,
                                                state->queue_byte_capacity)) {
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Make: teardown capacity exhausted");
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Make: allocation failed");
  } catch (const std::exception &error) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::Make: startup failed: ", error.what());
  } catch (...) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::Make: startup failed");
  }
  TeardownReservationGuard reservation_guard(state);
  try {
    auto owner = std::shared_ptr<OperationTaskArena>(
        new OperationTaskArena(state, std::move(metrics)));
    reservation_guard.Commit();
    g_live_arena_states.fetch_add(1U, std::memory_order_relaxed);
    return owner;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Make: owner allocation failed");
  }
}
OperationTaskArena::~OperationTaskArena() noexcept { Shutdown(); }

bool OperationTaskArena::ValidPlan(
    const State &state, const TaskArenaSubmissionPlan &plan) noexcept {
  const auto valid_lane = plan.cursor_lane_ == TaskArenaLane::kUpstream ||
                          plan.cursor_lane_ == TaskArenaLane::kOutputCompact ||
                          plan.cursor_lane_ == TaskArenaLane::kOutput ||
                          plan.cursor_lane_ == TaskArenaLane::kAll;
  if (!valid_lane || plan.generation_ != state.generation ||
      plan.width_ == 0U || plan.width_ > state.worker_count ||
      plan.lane_begin_ >= plan.lane_end_ ||
      plan.lane_end_ > state.worker_count ||
      plan.lane_end_ - plan.lane_begin_ != plan.width_ ||
      plan.alternative_offset_ == 0U ||
      plan.alternative_offset_ > plan.width_ ||
      plan.scalable_scan_ != state.scalable_scan) {
    return false;
  }
  std::size_t expected_begin = 0U;
  if (plan.cursor_lane_ == TaskArenaLane::kOutput) {
    expected_begin = state.worker_count - plan.width_;
  } else if (plan.cursor_lane_ == TaskArenaLane::kOutputCompact) {
    expected_begin = (state.worker_count - plan.width_) / 2U;
  }
  if (plan.lane_begin_ != expected_begin) {
    return false;
  }
  if (state.scalable_scan) {
    return plan.allowed_mask_ == 0U && plan.visibility_shard_begin_ == 0U &&
           plan.visibility_shard_end_ == 1U;
  }
  const auto expected_mask = lane_mask(plan.lane_begin_, plan.lane_end_);
  const auto expected_shard_begin = plan.lane_begin_ >> 3U;
  const auto expected_shard_end =
      std::min<std::size_t>(4U, (plan.lane_end_ + 7U) >> 3U);
  return plan.allowed_mask_ == expected_mask &&
         plan.visibility_shard_begin_ == expected_shard_begin &&
         plan.visibility_shard_end_ == expected_shard_end;
}

TaskArenaSubmissionPlan
OperationTaskArena::PrepareSubmissionPlan(std::size_t lane_width,
                                          TaskArenaLane lane) noexcept {
  TaskArenaSubmissionPlan plan;
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return plan;
  }
  plan.generation_ = state->generation;
  plan.width_ =
      std::max<std::size_t>(1, std::min(lane_width, state->worker_count));
  if (lane == TaskArenaLane::kOutput) {
    plan.lane_begin_ = state->worker_count - plan.width_;
  } else if (lane == TaskArenaLane::kOutputCompact) {
    plan.lane_begin_ = (state->worker_count - plan.width_) / 2U;
  }
  plan.lane_end_ = plan.lane_begin_ + plan.width_;
  plan.alternative_offset_ = std::max<std::size_t>(1, plan.width_ / 2U);
  plan.scalable_scan_ = state->scalable_scan;
  if (plan.scalable_scan_) {
    plan.allowed_mask_ = 0;
  } else {
    plan.allowed_mask_ = lane_mask(plan.lane_begin_, plan.lane_end_);
  }
  if (!plan.scalable_scan_) {
    plan.visibility_shard_begin_ =
        static_cast<std::uint8_t>(plan.lane_begin_ >> 3U);
    plan.visibility_shard_end_ = static_cast<std::uint8_t>(
        std::min<std::size_t>(4U, (plan.lane_end_ + 7U) >> 3U));
  }
  plan.cursor_lane_ = lane;
  return plan;
}

sanitize::Status OperationTaskArena::Submit(Task task, std::size_t lane_width,
                                            TaskArenaLane lane,
                                            TaskTelemetryKind telemetry_kind) {
  return SubmitCharged(std::move(task), lane_width, lane, TaskMemoryCharge{},
                       telemetry_kind);
}

sanitize::Status
OperationTaskArena::SubmitCharged(Task task, std::size_t lane_width,
                                  TaskArenaLane lane, TaskMemoryCharge charge,
                                  TaskTelemetryKind telemetry_kind) {
  const auto plan = PrepareSubmissionPlan(lane_width, lane);
  return SubmitCharged(std::move(task), plan, charge, telemetry_kind);
}

sanitize::Status
OperationTaskArena::SubmitLeased(Task task, std::size_t lane_width,
                                 TaskArenaLane lane, TaskMemoryLease lease,
                                 TaskTelemetryKind telemetry_kind) {
  const auto plan = PrepareSubmissionPlan(lane_width, lane);
  return SubmitLeased(std::move(task), plan, std::move(lease), telemetry_kind);
}

std::size_t OperationTaskArena::ReserveSubmissionTicket(
    const TaskArenaSubmissionPlan &plan) noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state || !ValidPlan(*state, plan)) {
    return 0U;
  }
  auto *cursor = &state->all_cursor;
  if (plan.cursor_lane_ == TaskArenaLane::kUpstream) {
    cursor = &state->upstream_cursor;
  } else if (plan.cursor_lane_ == TaskArenaLane::kOutput ||
             plan.cursor_lane_ == TaskArenaLane::kOutputCompact) {
    cursor = &state->output_cursor;
  }
  return cursor->fetch_add(1, std::memory_order_relaxed);
}

sanitize::Status OperationTaskArena::Submit(Task task,
                                            const TaskArenaSubmissionPlan &plan,
                                            TaskTelemetryKind telemetry_kind) {
  return SubmitCharged(std::move(task), plan, TaskMemoryCharge{},
                       telemetry_kind);
}

sanitize::Status OperationTaskArena::SubmitCharged(
    Task task, const TaskArenaSubmissionPlan &plan, TaskMemoryCharge charge,
    TaskTelemetryKind telemetry_kind) {
  // Preserve the v91 direct-submit ordering: invalid, closed, inline, and
  // already-stopping arenas do not advance the shared lane cursor.
  const auto state = state_.load(std::memory_order_acquire);
  if (!task || !state || state->worker_count <= 1U ||
      state->stopping.load(std::memory_order_acquire) ||
      state->cancel_requested.load(std::memory_order_acquire)) {
    return SubmitCharged(std::move(task), plan, 0U, charge, telemetry_kind);
  }
  const auto ticket = ReserveSubmissionTicket(plan);
  return SubmitCharged(std::move(task), plan, ticket, charge, telemetry_kind);
}

sanitize::Status
OperationTaskArena::SubmitLeased(Task task, const TaskArenaSubmissionPlan &plan,
                                 TaskMemoryLease lease,
                                 TaskTelemetryKind telemetry_kind) {
  // Validate the user task before wrapping it.  An empty std::function becomes
  // apparently non-empty once captured by the ownership wrapper and would
  // otherwise throw std::bad_function_call on a worker after admission.
  if (!task) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::SubmitLeased: empty task");
  }
  if (!lease) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::SubmitLeased: empty memory lease");
  }
  const auto retained_bytes = lease.retained_bytes_;
  auto owner = std::move(lease.owner_);
  try {
    Task wrapped([task = std::move(task), owner = std::move(owner)](
                     std::size_t worker, StopToken stop) mutable {
      (void)owner;
      task(worker, stop);
    });
    return SubmitCharged(std::move(wrapped), plan,
                         TaskMemoryCharge(retained_bytes), telemetry_kind);
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::SubmitLeased: wrapper allocation failed");
  } catch (const std::exception &error) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::SubmitLeased: wrapper construction failed: ",
        error.what());
  } catch (...) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::SubmitLeased: wrapper construction failed");
  }
}

sanitize::Status OperationTaskArena::Submit(Task task,
                                            const TaskArenaSubmissionPlan &plan,
                                            std::size_t submission_ticket,
                                            TaskTelemetryKind telemetry_kind) {
  return SubmitCharged(std::move(task), plan, submission_ticket,
                       TaskMemoryCharge{}, telemetry_kind);
}

sanitize::Status OperationTaskArena::SubmitCharged(
    Task task, const TaskArenaSubmissionPlan &plan,
    std::size_t submission_ticket, TaskMemoryCharge charge,
    TaskTelemetryKind telemetry_kind) {
  const auto state = state_.load(std::memory_order_acquire);
  const auto retained_bytes = std::max<std::size_t>(1U, charge.retained_bytes);
  if (!task) {
    return sanitize::Status::Invalid("OperationTaskArena::Submit: empty task");
  }
  if (!state) {
    return sanitize::Status::Cancelled(
        "OperationTaskArena::Submit: arena is closed");
  }
  if (!ValidPlan(*state, plan)) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::Submit: invalid or stale submission plan");
  }
  if (state->cancel_requested.load(std::memory_order_acquire)) {
    return sanitize::Status::Cancelled(
        "OperationTaskArena::Submit: operation cancellation requested");
  }
  if (!charge.explicit_charge) {
    state->unknown_charge_submissions.fetch_add(1U, std::memory_order_relaxed);
    const auto retained =
        state->retained_bytes_total.load(std::memory_order_acquire);
    if (retained >= state->queue_byte_capacity / 2U) {
      state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
      state->rejected_submissions.fetch_add(1U, std::memory_order_relaxed);
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Submit: unknown task charge rejected under "
          "pressure");
    }
  }
  if (state->worker_count <= 1U) {
    if (retained_bytes > state->queue_byte_capacity) {
      state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Submit: task memory charge exceeds active "
          "capacity");
    }
    if (state->stopping.load(std::memory_order_acquire) ||
        state->cancel_requested.load(std::memory_order_acquire)) {
      return sanitize::Status::Cancelled(
          "OperationTaskArena::Submit: arena is stopping or cancelled");
    }
    auto retained_total =
        state->retained_bytes_total.load(std::memory_order_relaxed);
    while (true) {
      if (retained_total > state->queue_byte_capacity - retained_bytes) {
        state->rejected_byte_submissions.fetch_add(1U,
                                                   std::memory_order_relaxed);
        return sanitize::Status::OutOfMemory(
            "OperationTaskArena::Submit: active byte capacity exhausted");
      }
      if (state->retained_bytes_total.compare_exchange_weak(
              retained_total, retained_total + retained_bytes,
              std::memory_order_acq_rel, std::memory_order_relaxed)) {
        (void)update_peak(&state->peak_retained_bytes_total,
                          retained_total + retained_bytes);
        break;
      }
    }
    // Retained total was admitted above. The scope publishes active-byte
    // diagnostics and can atomically transfer that credit to a completion.
    ActiveRetainedCharge retained_charge(state, retained_bytes, &task);
    state->inline_active.fetch_add(1U, std::memory_order_acq_rel);
    if (state->stopping.load(std::memory_order_acquire) ||
        state->cancel_requested.load(std::memory_order_acquire)) {
      SaturatingAtomicSubtract(state->inline_active, 1U);
      state->inline_ready.notify_all();
      return sanitize::Status::Cancelled(
          "OperationTaskArena::Submit: arena is stopping or cancelled");
    }
    const auto started_ns =
        state->telemetry ? PerformanceTelemetry::NowNs() : std::int64_t{0};
    if (state->telemetry) {
      state->telemetry->RecordTaskSubmitted(telemetry_kind, 1);
      state->telemetry->RecordTaskStarted(telemetry_kind, 0);
      state->telemetry->ObserveActiveTasks(1);
    }
    try {
      task(0, {});
    } catch (...) {
      SaturatingAtomicSubtract(state->inline_active, 1U);
      state->inline_ready.notify_all();
      throw;
    }
    if (state->telemetry) {
      state->telemetry->RecordTaskFinished(
          telemetry_kind, PerformanceTelemetry::NowNs() - started_ns);
    }
    state->inline_submitted.fetch_add(1, std::memory_order_relaxed);
    SaturatingAtomicSubtract(state->inline_active, 1U);
    state->inline_ready.notify_all();
    return sanitize::Status::OK();
  }
  if (state->stopping.load(std::memory_order_acquire) ||
      state->cancel_requested.load(std::memory_order_acquire)) {
    return sanitize::Status::Cancelled(
        "OperationTaskArena::Submit: arena is stopping or cancelled");
  }
  if (retained_bytes > state->queue_byte_capacity) {
    state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Submit: task memory charge exceeds queue "
        "capacity");
  }
  // Memory is the first scarce resource for multi-worker submission. Producers
  // waiting on retained-byte credit consume only the separately bounded waiter
  // bank: they do not occupy queue slots and do not start physical workers.
  const auto retained_status =
      AcquireRetainedSubmitCredit(state, retained_bytes);
  if (!retained_status.ok()) {
    return retained_status;
  }
  bool retained_credit_owned = true;
  const auto release_retained_credit = [&]() noexcept {
    if (!retained_credit_owned) {
      return;
    }
    retained_credit_owned = false;
    SaturatingAtomicSubtract(state->retained_bytes_total, retained_bytes);
    state->NotifyRetainedAvailability();
  };
  const auto lane_begin = plan.lane_begin_;
  const auto lane_end = plan.lane_end_;
  const auto width = plan.width_;
  const auto ticket = submission_ticket;
  // Normalize the lane ticket once per admission. Startup reservation,
  // saturated placement, the precompiled alternative, and the optional helper
  // all reuse this origin instead of repeating integer division.
  const auto lane_origin = ticket % width;
  const auto initialized_snapshot =
      state->scalable_scan
          ? std::uint64_t{0}
          : state->initialized_mask.load(std::memory_order_acquire);
  auto physical = idle_started_worker(state, lane_begin, lane_end, width,
                                      plan.allowed_mask_, lane_origin,
                                      initialized_snapshot, plan);
  bool reserved_worker = false;
  const auto compact_lane_fully_initialized =
      !state->scalable_scan &&
      (initialized_snapshot & plan.allowed_mask_) == plan.allowed_mask_;
  if (physical == lane_end &&
      (state->scalable_scan || !compact_lane_fully_initialized)) {
    // If every allowed bit is initialized, every worker is already admitted and
    // started. A stale snapshot can only take this conservative reservation
    // path; it can never skip a worker that still needs startup.
    physical = reserve_unstarted_worker(state, lane_begin, lane_end,
                                        plan.allowed_mask_, lane_origin);
    reserved_worker = physical != lane_end;
    if (reserved_worker) {
      state->slots[physical]->first_task_pending.store(
          true, std::memory_order_release);
    }
  }
  if (physical == lane_end) {
    physical = lane_begin + lane_origin;
    if (width > 1) {
      const auto alternative_origin =
          task_arena_detail::advance_normalized_lane_origin(
              ticket, lane_origin, plan.alternative_offset_, width);
      const auto alternative = lane_begin + alternative_origin;
      const auto load = [&state](std::size_t index) noexcept {
        const auto &candidate = *state->slots[index];
        return candidate.queued.load(std::memory_order_relaxed) +
               (candidate.running.load(std::memory_order_relaxed) ? 1U : 0U);
      };
      if (load(alternative) < load(physical)) {
        physical = alternative;
      }
    }
  }
  const auto worker_initialized =
      state->scalable_scan
          ? state->initialized_dynamic.Test(physical)
          : (initialized_snapshot & worker_bit(physical)) != 0U;
  const auto startup_status =
      worker_initialized || worker_already_started_fast_path(state, physical)
          ? sanitize::Status::OK()
          : ensure_worker_started(state, physical, reserved_worker);
  if (!startup_status.ok()) {
    if (reserved_worker) {
      state->slots[physical]->first_task_pending.store(
          false, std::memory_order_release);
    }
    release_retained_credit();
    return startup_status;
  }

  auto &slot = *state->slots[physical];
  std::size_t queued_before = 0;
  bool target_running = false;
  bool queue_slot_reserved = false;
  bool reaper_bytes_reserved = false;
  bool queued_bytes_published = false;
  const auto rollback_publication = [&]() noexcept {
    if (queue_slot_reserved) {
      SaturatingAtomicSubtract(state->queued_total, 1U);
      queue_slot_reserved = false;
    }
    if (queued_bytes_published) {
      SaturatingAtomicSubtract(state->queued_bytes, retained_bytes);
      queued_bytes_published = false;
    }
    if (retained_credit_owned) {
      release_retained_credit();
    } else if (reaper_bytes_reserved) {
      SaturatingAtomicSubtract(state->retained_bytes_total, retained_bytes);
      state->NotifyRetainedAvailability();
    }
    if (reaper_bytes_reserved) {
      ArenaCleanupReaper::Instance().ReleaseQueuedBytes(state, retained_bytes);
      reaper_bytes_reserved = false;
    }
  };
  try {
    std::unique_lock lock(slot.mutex);
    if (state->stopping.load(std::memory_order_acquire)) {
      if (reserved_worker) {
        slot.first_task_pending.store(false, std::memory_order_release);
        slot.initialized.store(true, std::memory_order_release);
        if (state->scalable_scan) {
          state->initialized_dynamic.Set(physical);
        } else {
          state->initialized_mask.fetch_or(worker_bit(physical),
                                           std::memory_order_release);
        }
        slot.wake_epoch.fetch_add(1, std::memory_order_release);
        slot.ready.notify_one();
      }
      release_retained_credit();
      return sanitize::Status::Cancelled(
          "OperationTaskArena::Submit: arena is stopping or cancelled");
    }
    target_running = slot.running.load(std::memory_order_acquire);
    auto queued_total = state->queued_total.load(std::memory_order_relaxed);
    while (true) {
      if (queued_total >= state->queue_capacity) {
        state->rejected_submissions.fetch_add(1, std::memory_order_relaxed);
        if (reserved_worker) {
          slot.first_task_pending.store(false, std::memory_order_release);
          slot.initialized.store(true, std::memory_order_release);
          if (state->scalable_scan) {
            state->initialized_dynamic.Set(physical);
          } else {
            state->initialized_mask.fetch_or(worker_bit(physical),
                                             std::memory_order_release);
          }
          slot.wake_epoch.fetch_add(1, std::memory_order_release);
          slot.ready.notify_one();
        }
        release_retained_credit();
        return sanitize::Status::OutOfMemory(
            "OperationTaskArena::Submit: bounded queue capacity exhausted");
      }
      if (state->queued_total.compare_exchange_weak(
              queued_total, queued_total + 1U, std::memory_order_acq_rel,
              std::memory_order_relaxed)) {
        (void)update_peak(&state->peak_queued, queued_total + 1U);
        queue_slot_reserved = true;
        break;
      }
    }
    if (!ArenaCleanupReaper::Instance().ReserveQueuedBytes(state,
                                                           retained_bytes)) {
      SaturatingAtomicSubtract(state->queued_total, 1U);
      release_retained_credit();
      state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
      if (reserved_worker) {
        slot.first_task_pending.store(false, std::memory_order_release);
        slot.initialized.store(true, std::memory_order_release);
        if (state->scalable_scan) {
          state->initialized_dynamic.Set(physical);
        } else {
          state->initialized_mask.fetch_or(worker_bit(physical),
                                           std::memory_order_release);
        }
        slot.wake_epoch.fetch_add(1, std::memory_order_release);
        slot.ready.notify_one();
      }
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Submit: teardown byte capacity exhausted");
    }
    reaper_bytes_reserved = true;
    // Queue/reaper ownership now carries the admitted retained-byte credit.
    retained_credit_owned = false;
    const auto queued_bytes = state->queued_bytes.fetch_add(
                                  retained_bytes, std::memory_order_acq_rel) +
                              retained_bytes;
    queued_bytes_published = true;
    (void)update_peak(&state->peak_queued_bytes, queued_bytes);
    slot.tasks.push_back(State::QueuedTask{
        .task = std::move(task),
        .lane_begin = lane_begin,
        .lane_end = lane_end,
        .telemetry_kind = telemetry_kind,
        .queued_at_ns =
            state->telemetry ? PerformanceTelemetry::NowNs() : std::int64_t{0},
        .retained_bytes = retained_bytes,
    });
    if (state->worker_count >= 4U && state->worker_count <= 5U &&
        (slot.dedicated_output_queued > 0U ||
         bounded_low_core_output(slot.tasks.back(), state))) {
      if (bounded_low_core_output(slot.tasks.back(), state)) {
        ++slot.dedicated_output_queued;
      }
      refresh_shallow_output_preference(state, slot);
    }
    queued_before = slot.queued_local;
    ++slot.queued_local;
    slot.queued.store(slot.queued_local, std::memory_order_relaxed);
    ++slot.submitted_local;
    slot.submitted.store(slot.submitted_local, std::memory_order_relaxed);
    if (state->telemetry) {
      state->telemetry->RecordWorkerTaskSubmitted(physical, telemetry_kind,
                                                  queued_before + 1U);
    }
    // Publish queue visibility only on the empty-to-nonempty transition. The
    // worker-specific queue mutex already orders appended packets, so repeating
    // the operation-global mask RMW for an already-visible queue adds cache
    // contention without changing steal eligibility.
    if (queued_before == 0U) {
      mark_nonempty(state, physical);
    }
  } catch (const std::bad_alloc &) {
    rollback_publication();
    if (reserved_worker) {
      slot.first_task_pending.store(false, std::memory_order_release);
      slot.initialized.store(true, std::memory_order_release);
      if (state->scalable_scan) {
        state->initialized_dynamic.Set(physical);
      } else {
        state->initialized_mask.fetch_or(worker_bit(physical),
                                         std::memory_order_release);
      }
      slot.wake_epoch.fetch_add(1, std::memory_order_release);
      slot.ready.notify_one();
    }
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Submit: queue allocation failed");
  } catch (const std::exception &error) {
    rollback_publication();
    if (reserved_worker) {
      slot.first_task_pending.store(false, std::memory_order_release);
      slot.initialized.store(true, std::memory_order_release);
      if (state->scalable_scan) {
        state->initialized_dynamic.Set(physical);
      } else {
        state->initialized_mask.fetch_or(worker_bit(physical),
                                         std::memory_order_release);
      }
      slot.wake_epoch.fetch_add(1, std::memory_order_release);
      slot.ready.notify_one();
    }
    return sanitize::Status::Invalid(
        "OperationTaskArena::Submit: queue publication failed: ", error.what());
  } catch (...) {
    rollback_publication();
    if (reserved_worker) {
      slot.first_task_pending.store(false, std::memory_order_release);
      slot.initialized.store(true, std::memory_order_release);
      if (state->scalable_scan) {
        state->initialized_dynamic.Set(physical);
      } else {
        state->initialized_mask.fetch_or(worker_bit(physical),
                                         std::memory_order_release);
      }
      slot.wake_epoch.fetch_add(1, std::memory_order_release);
      slot.ready.notify_one();
    }
    return sanitize::Status::Invalid(
        "OperationTaskArena::Submit: queue publication failed");
  }
  // v82: a submission only publishes a wake generation when a physical
  // worker must actually leave its park state. v81's under-mutex local recheck
  // guarantees that a running target cannot sleep past work appended here.
  auto helper = lane_end;
  if (target_running || queued_before > 0) {
    const auto helper_initialized_snapshot =
        state->initialized_mask.load(std::memory_order_acquire);
    const auto helper_origin =
        task_arena_detail::advance_normalized_lane_origin(ticket, lane_origin,
                                                          1U, width);
    helper = idle_started_worker(state, lane_begin, lane_end, width,
                                 plan.allowed_mask_, helper_origin,
                                 helper_initialized_snapshot, plan);
  }
  const auto wake_target = !target_running;
  const auto wake_helper = helper != lane_end && helper != physical;
  if (wake_target) {
    slot.wake_epoch.fetch_add(1, std::memory_order_release);
    slot.ready.notify_one();
  }
  if (wake_helper) {
    auto &helper_slot = *state->slots[helper];
    helper_slot.wake_epoch.fetch_add(1, std::memory_order_release);
    helper_slot.ready.notify_one();
  }
  return sanitize::Status::OK();
}

std::size_t OperationTaskArena::worker_count() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->worker_count : 1U;
}
bool OperationTaskArena::inline_mode() const noexcept {
  return worker_count() <= 1U;
}
std::size_t OperationTaskArena::peak_active_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->peak_active.load(std::memory_order_relaxed) : 0U;
}
std::size_t OperationTaskArena::active_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->active.load(std::memory_order_acquire) : 0U;
}
std::size_t OperationTaskArena::submitted_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return 0U;
  }
  auto total = state->inline_submitted.load(std::memory_order_relaxed);
  for (const auto &slot : state->slots) {
    total += slot->submitted.load(std::memory_order_relaxed);
  }
  return total;
}
std::size_t OperationTaskArena::stolen_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return 0U;
  }
  std::size_t total = 0U;
  for (const auto &slot : state->slots) {
    total += slot->stolen.load(std::memory_order_relaxed);
  }
  return total;
}
std::size_t OperationTaskArena::output_preference_bypasses() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state
             ? state->output_preference_bypasses.load(std::memory_order_relaxed)
             : 0U;
}
std::size_t OperationTaskArena::queued_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->queued_total.load(std::memory_order_acquire) : 0U;
}
std::size_t OperationTaskArena::peak_queued_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->peak_queued.load(std::memory_order_relaxed) : 0U;
}
std::size_t OperationTaskArena::queue_capacity() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->queue_capacity : 0U;
}
std::size_t OperationTaskArena::queued_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->queued_bytes.load(std::memory_order_acquire) : 0U;
}
std::size_t OperationTaskArena::active_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->active_bytes.load(std::memory_order_acquire) : 0U;
}
std::size_t OperationTaskArena::retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->retained_bytes_total.load(std::memory_order_acquire)
               : post_shutdown_retained_bytes();
}
std::size_t OperationTaskArena::peak_queued_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->peak_queued_bytes.load(std::memory_order_relaxed) : 0U;
}
std::size_t OperationTaskArena::peak_active_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->peak_active_bytes.load(std::memory_order_relaxed) : 0U;
}
std::size_t OperationTaskArena::peak_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state
             ? state->peak_retained_bytes_total.load(std::memory_order_relaxed)
             : 0U;
}
std::size_t OperationTaskArena::queue_byte_capacity() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->queue_byte_capacity : 0U;
}
// Compatibility note: OperationTaskArena::RetainCompletionBytes was removed
// because it could bypass queue_byte_capacity; all ownership transfer now uses
// the single transactional method below.
bool OperationTaskArena::TryTransferActiveToCompletion(
    std::size_t active_credit, std::size_t completion_bytes,
    CompletionMemoryLease *completion_lease) noexcept {
  if (completion_lease == nullptr || static_cast<bool>(*completion_lease)) {
    return false;
  }
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return completion_bytes == 0U;
  }
  auto *scope = g_active_retained_charge;
  if (scope == nullptr ||
      !scope->TryTransfer(state.get(), active_credit, completion_bytes)) {
    return false;
  }
  if (completion_bytes != 0U) {
    *completion_lease = CompletionMemoryLease(state, completion_bytes);
  }
  return true;
}

CompletionMemoryLease::CompletionMemoryLease(
    CompletionMemoryLease &&other) noexcept
    : state_(std::move(other.state_)),
      retained_bytes_(std::exchange(other.retained_bytes_, 0U)) {}

CompletionMemoryLease &
CompletionMemoryLease::operator=(CompletionMemoryLease &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  reset();
  state_ = std::move(other.state_);
  retained_bytes_ = std::exchange(other.retained_bytes_, 0U);
  return *this;
}

CompletionMemoryLease::~CompletionMemoryLease() noexcept { reset(); }

void CompletionMemoryLease::reset() noexcept {
  auto state = std::move(state_);
  const auto retained = std::exchange(retained_bytes_, 0U);
  if (!state || retained == 0U) {
    return;
  }
  auto current = state->retained_bytes_total.load(std::memory_order_acquire);
  for (;;) {
    if (current < retained) {
      // CompletionMemoryLease is move-only/exactly-once. Reaching this branch
      // therefore means protocol corruption rather than a benign duplicate
      // quantity release. Preserve noexcept cleanup while surfacing the fault.
      g_completion_memory_protocol_violations.fetch_add(
          1U, std::memory_order_relaxed);
      if (state->retained_bytes_total.compare_exchange_weak(
              current, 0U, std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        break;
      }
      continue;
    }
    if (state->retained_bytes_total.compare_exchange_weak(
            current, current - retained, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      break;
    }
  }
  state->NotifyRetainedAvailability();
}
std::size_t OperationTaskArena::rejected_submissions() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->rejected_submissions.load(std::memory_order_relaxed)
               : 0U;
}
std::size_t OperationTaskArena::rejected_byte_submissions() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state
             ? state->rejected_byte_submissions.load(std::memory_order_relaxed)
             : 0U;
}
std::size_t OperationTaskArena::backpressure_timeouts() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->backpressure_timeouts.load(std::memory_order_relaxed)
               : 0U;
}
std::size_t OperationTaskArena::logical_backpressure_timeouts() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->logical_backpressure_timeouts.load(
                     std::memory_order_relaxed)
               : 0U;
}
std::size_t OperationTaskArena::backpressure_waiters() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->backpressure_waiters.load(std::memory_order_relaxed)
               : 0U;
}

std::size_t OperationTaskArena::producer_waiter_capacity() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->producer_waiter_capacity : 0U;
}

std::size_t OperationTaskArena::peak_backpressure_waiters() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state
             ? state->peak_backpressure_waiters.load(std::memory_order_relaxed)
             : 0U;
}

std::size_t OperationTaskArena::rejected_backpressure_waiters() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->rejected_backpressure_waiters.load(
                     std::memory_order_relaxed)
               : 0U;
}

std::size_t OperationTaskArena::backpressure_bypasses() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->backpressure_bypasses.load(std::memory_order_relaxed)
               : 0U;
}

std::size_t OperationTaskArena::starvation_preventions() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->starvation_preventions.load(std::memory_order_relaxed)
               : 0U;
}

std::uint64_t
OperationTaskArena::oldest_backpressure_waiter_age_millis() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return 0U;
  }
  std::int64_t oldest = 0;
  {
    std::lock_guard lock(state->retained_wait_mutex);
    for (const auto &ticket : state->backpressure_tickets) {
      if (ticket.active && ticket.waiting_since_ns > 0 &&
          (oldest == 0 || ticket.waiting_since_ns < oldest)) {
        oldest = ticket.waiting_since_ns;
      }
    }
  }
  if (oldest <= 0) {
    return 0U;
  }
  const auto now = PerformanceTelemetry::NowNs();
  return now > oldest ? static_cast<std::uint64_t>((now - oldest) / 1'000'000)
                      : 0U;
}

std::size_t OperationTaskArena::unknown_charge_submissions() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state
             ? state->unknown_charge_submissions.load(std::memory_order_relaxed)
             : 0U;
}
std::size_t OperationTaskArena::detached_workers() const noexcept {
  return detached_metrics_ ? detached_metrics_->Current() : 0U;
}
std::size_t OperationTaskArena::total_detached_workers() const noexcept {
  return detached_metrics_
             ? detached_metrics_->total.load(std::memory_order_acquire)
             : 0U;
}
std::uint64_t OperationTaskArena::detached_worker_age_millis() const noexcept {
  const auto since =
      detached_metrics_ ? detached_metrics_->OldestSinceNs() : std::int64_t{0};
  if (since <= 0) {
    return 0U;
  }
  const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();
  return static_cast<std::uint64_t>(std::max<std::int64_t>(0, now - since) /
                                    1'000'000);
}
std::size_t OperationTaskArena::shutdown_timeouts() const noexcept {
  return shutdown_timeouts_.load(std::memory_order_acquire);
}
std::size_t OperationTaskArena::abandoned_queued_tasks() const noexcept {
  return abandoned_queued_tasks_.load(std::memory_order_acquire);
}
std::size_t OperationTaskArena::abandoned_queued_bytes() const noexcept {
  return abandoned_queued_bytes_.load(std::memory_order_acquire);
}
std::size_t OperationTaskArena::reaper_queued_states() const noexcept {
  return detached_metrics_ ? detached_metrics_->reaper_queued_states.load(
                                 std::memory_order_acquire)
                           : 0U;
}
std::size_t OperationTaskArena::reaper_active_states() const noexcept {
  return detached_metrics_ ? detached_metrics_->reaper_active_states.load(
                                 std::memory_order_acquire)
                           : 0U;
}
std::size_t OperationTaskArena::reaper_queued_bytes() const noexcept {
  return detached_metrics_ ? detached_metrics_->reaper_queued_bytes.load(
                                 std::memory_order_acquire)
                           : 0U;
}
std::size_t OperationTaskArena::reaper_active_bytes() const noexcept {
  return detached_metrics_ ? detached_metrics_->reaper_active_bytes.load(
                                 std::memory_order_acquire)
                           : 0U;
}
std::size_t OperationTaskArena::post_shutdown_retained_bytes() const noexcept {
  const auto queued = reaper_queued_bytes();
  const auto active = reaper_active_bytes();
  return active > std::numeric_limits<std::size_t>::max() - queued
             ? std::numeric_limits<std::size_t>::max()
             : queued + active;
}
std::size_t OperationTaskArena::started_workers() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return 0U;
  }
  if (!state->scalable_scan) {
    return static_cast<std::size_t>(
        std::popcount(state->started_mask.load(std::memory_order_acquire)));
  }
  return state->started_dynamic.Count();
}
std::uint64_t OperationTaskArena::wake_epoch_publishes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return 0U;
  }
  std::uint64_t total = 0U;
  for (const auto &slot : state->slots) {
    total += slot->wake_epoch.load(std::memory_order_relaxed);
  }
  return total;
}
std::shared_ptr<PerformanceTelemetry>
OperationTaskArena::telemetry() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->telemetry : nullptr;
}
std::shared_ptr<std::pmr::memory_resource>
OperationTaskArena::memory_resource() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? std::static_pointer_cast<std::pmr::memory_resource>(
                     state->operation_resource)
               : nullptr;
}
void OperationTaskArena::RequestCancellation() noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return;
  }
  state->cancel_requested.store(true, std::memory_order_release);
  state->NotifyRetainedAvailability(true);
  state->inline_ready.notify_all();
  for (auto &slot : state->slots) {
    if (slot && slot->worker) {
      slot->worker->request_stop();
    }
    if (slot) {
      slot->ready.notify_all();
    }
  }
}

void OperationTaskArena::SetBackpressureTimeoutMillis(
    std::uint64_t timeout_millis) noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return;
  }
  const auto bounded = std::min<std::uint64_t>(timeout_millis, 30'000U);
  const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::milliseconds(bounded))
                            .count();
  state->backpressure_timeout_ns.store(
      timeout_millis == 0U ? 0 : static_cast<std::int64_t>(duration),
      std::memory_order_release);
  state->NotifyRetainedAvailability(true);
}

void OperationTaskArena::SetBackpressureDeadlineMillis(
    std::uint64_t timeout_millis) noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return;
  }
  if (timeout_millis == 0U) {
    state->backpressure_deadline_ns.store(0, std::memory_order_release);
  } else {
    // Keep conversion well inside signed chrono nanoseconds even for an
    // accidentally enormous caller value. The arena's independent hard wait
    // ceiling remains 30 seconds per saturation episode.
    constexpr std::uint64_t kMaxLogicalDeadlineMillis = 86'400'000U;
    const auto bounded =
        std::min<std::uint64_t>(timeout_millis, kMaxLogicalDeadlineMillis);
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    const auto delta = std::chrono::milliseconds(bounded);
    const auto deadline =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now + delta)
            .count();
    state->backpressure_deadline_ns.store(static_cast<std::int64_t>(deadline),
                                          std::memory_order_release);
  }
  state->NotifyRetainedAvailability(true);
}

void OperationTaskArena::Shutdown() noexcept {
  auto state = state_.exchange(nullptr, std::memory_order_acq_rel);
  if (!state) {
    return;
  }
  SaturatingAtomicSubtract(g_live_arena_states, 1U);
  state->stopping.store(true, std::memory_order_release);
  state->NotifyRetainedAvailability(true);
  const auto deadline = std::chrono::steady_clock::now() + kArenaShutdownDrain;
  {
    std::unique_lock inline_lock(state->inline_mutex);
    if (!state->inline_ready.wait_until(inline_lock, deadline, [&state] {
          return state->inline_active.load(std::memory_order_acquire) == 0U;
        })) {
      shutdown_timeouts_.fetch_add(1U, std::memory_order_release);
    }
  }
  for (auto &slot : state->slots) {
    std::lock_guard start_lock(slot->start_mutex);
    if (slot->worker) {
      slot->worker->request_stop();
    }
  }
  std::size_t abandoned = 0U;
  std::size_t abandoned_bytes = 0U;
  bool all_queues_drained = true;
  for (auto &slot : state->slots) {
    try {
      std::lock_guard lock(slot->mutex);
      for (const auto &queued : slot->tasks) {
        abandoned_bytes =
            queued.retained_bytes >
                    std::numeric_limits<std::size_t>::max() - abandoned_bytes
                ? std::numeric_limits<std::size_t>::max()
                : abandoned_bytes + queued.retained_bytes;
      }
      abandoned += slot->tasks.size();
      {
        auto &drain = slot->abandoned_tasks;
        drain.swap(slot->tasks);
        slot->tasks.clear();
      }
      slot->queued_local = 0U;
      slot->queued.store(0U, std::memory_order_relaxed);
      slot->dedicated_output_queued = 0U;
      slot->shallow_output_preference = false;
    } catch (...) {
      all_queues_drained = false;
      shutdown_timeouts_.fetch_add(1U, std::memory_order_release);
    }
  }
  if (abandoned > 0U) {
    SaturatingAtomicSubtract(state->queued_total, abandoned);
    SaturatingAtomicSubtract(state->queued_bytes, abandoned_bytes);
    state->reaper_bytes = abandoned_bytes;
  }
  state->abandoned_queued_tasks.fetch_add(abandoned, std::memory_order_relaxed);
  state->abandoned_queued_bytes.fetch_add(abandoned_bytes,
                                          std::memory_order_relaxed);
  abandoned_queued_tasks_.fetch_add(abandoned, std::memory_order_release);
  abandoned_queued_bytes_.fetch_add(abandoned_bytes, std::memory_order_release);
  if (all_queues_drained) {
    state->primary_queue_visibility.nonempty_mask.store(
        0U, std::memory_order_release);
    for (auto &visibility : state->queue_visibility) {
      visibility.nonempty_mask.store(0U, std::memory_order_release);
    }
    if (state->scalable_scan) {
      state->nonempty_dynamic.Reset();
    }
  }
  if (abandoned == 0U) {
    ArenaCleanupReaper::Instance().ReleaseReservation(state);
  }
  if (abandoned > 0U) {
    // The shared reaper performs the old abandoned_queues->clear() operation
    // after shutdown returns, while retaining the arena allocator.
    if (!ArenaCleanupReaper::Instance().Enqueue(state)) {
      // Once teardown has begun, do not turn an exhausted deadline into an
      // unbounded synchronous run of arbitrary capture destructors.  A fixed
      // preallocated parking array retains the exact owner until process exit.
      if (!ArenaCleanupReaper::Instance().Park(state)) {
        // Every accepted arena reserved one of kLaneCount*kMaxQueuedStates
        // terminal destinations before publication, so this branch is an
        // invariant violation. Never run arbitrary capture destructors on the
        // shutdown thread: retain one deliberate self-owner for OS cleanup.
        if (!ArenaCleanupReaper::Instance().Terminalize(state)) {
          // Every admitted arena reserved a terminal destination. Failure here
          // means the ownership invariant is already corrupted; never execute
          // arbitrary capture destructors on this shutdown thread.
          std::terminate();
        }
        shutdown_timeouts_.fetch_add(1U, std::memory_order_release);
      }
    }
  }
  for (auto &slot : state->slots) {
    slot->wake_epoch.fetch_add(1, std::memory_order_release);
    slot->ready.notify_all();
  }

  bool detached = false;
  for (auto &slot : state->slots) {
    std::unique_ptr<ArenaWorkerThread> worker;
    {
      std::lock_guard start_lock(slot->start_mutex);
      worker = std::move(slot->worker);
    }
    if (!worker) {
      continue;
    }
    if (worker->wait_until(deadline)) {
      worker->join();
    } else {
      const auto detached_now =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now().time_since_epoch())
              .count();
      const auto detached_id = detached_metrics_->Register(detached_now);
      worker->mark_detached(detached_metrics_, detached_id);
      worker->detach();
      detached = true;
      shutdown_timeouts_.fetch_add(1U, std::memory_order_release);
    }
  }
  if (detached) {
    // Detached workers capture ``state`` strongly. They own queues, allocators,
    // telemetry, and cancellation state until their actual terminal return.
    return;
  }

  for (auto &slot : state->slots) {
    std::lock_guard lock(slot->mutex);
    slot->running.store(false, std::memory_order_relaxed);
    slot->first_task_pending.store(false, std::memory_order_relaxed);
    slot->admitted.store(false, std::memory_order_relaxed);
    slot->started.store(false, std::memory_order_relaxed);
    slot->initialized.store(false, std::memory_order_relaxed);
  }
  if (!state->scalable_scan) {
    state->admitted_mask.store(0U, std::memory_order_relaxed);
    state->started_mask.store(0U, std::memory_order_relaxed);
    state->initialized_mask.store(0U, std::memory_order_relaxed);
  } else {
    state->admitted_dynamic.Reset();
    state->started_dynamic.Reset();
    state->initialized_dynamic.Reset();
    state->nonempty_dynamic.Reset();
  }
}

} // namespace sanitize::internal

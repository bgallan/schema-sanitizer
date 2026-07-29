// Samples the NUMA node of the calling native worker.
#pragma once

#if defined(__linux__)
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace sanitize::internal {

[[nodiscard]] inline int current_locality_domain() noexcept {
#if defined(__linux__) && defined(SYS_getcpu)
  unsigned cpu = 0;
  unsigned node = 0;
  if (::syscall(SYS_getcpu, &cpu, &node, nullptr) == 0) {
    return static_cast<int>(node);
  }
#endif
  return 0;
}

} // namespace sanitize::internal

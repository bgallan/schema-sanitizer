// Process identity guard for runtime state that is unsafe to inherit across
// fork.
#pragma once

#include <cstdint>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace sanitize::internal {

using RuntimeProcessId = std::uint64_t;

[[nodiscard]] inline RuntimeProcessId current_runtime_process_id() noexcept {
#ifdef _WIN32
  return static_cast<RuntimeProcessId>(_getpid());
#else
  return static_cast<RuntimeProcessId>(getpid());
#endif
}

// Inline initialization occurs when the native image is loaded. A forked child
// therefore observes the parent's value and can avoid all inherited locks and
// C++ ownership graphs. A fresh exec loads a new image and receives a new
// owner.
inline const RuntimeProcessId kRuntimeOwnerProcessId =
    current_runtime_process_id();

[[nodiscard]] inline bool runtime_owner_process() noexcept {
  return current_runtime_process_id() == kRuntimeOwnerProcessId;
}

} // namespace sanitize::internal

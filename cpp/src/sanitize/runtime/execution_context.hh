// Declares execution context ownership and memory-pool access.

#pragma once

#include <functional>

#include "sanitize/core/status.hh"

namespace sanitize {

// A shared execution boundary for ingest operations.
//
// This owns runtime state for ingest operations.
class ExecutionContext {
public:
  using InterruptCheck = std::function<sanitize::Status()>;

  // Creates an ExecutionContext.
  ExecutionContext();
  // Destroys the ExecutionContext.
  ~ExecutionContext() = default;

  // Disables copying execution contexts.
  ExecutionContext(const ExecutionContext &) = delete;
  // Disables copy assignment.
  ExecutionContext &operator=(const ExecutionContext &) = delete;

  // Returns the runtime memory pool handle.
  static void *memory_pool_handle() noexcept;

  // Installs a cooperative interrupt check for long-running work.
  void set_interrupt_check(InterruptCheck check);
  // Returns cancellation when the installed interrupt check requests it.
  [[nodiscard]] sanitize::Status CheckInterrupt() const;

private:
  InterruptCheck interrupt_check_;
};

} // namespace sanitize

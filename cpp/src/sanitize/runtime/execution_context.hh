// Declares execution context ownership and memory-pool access.

#pragma once

#include <functional>
#include <memory>

#include "sanitize/core/status.hh"

namespace sanitize::internal {
class MemoryPool;
}

namespace sanitize {

// A shared execution boundary for ingest operations.
class ExecutionContext {
public:
  using InterruptCheck = std::function<sanitize::Status()>;

  ExecutionContext();
  ~ExecutionContext() = default;

  ExecutionContext(const ExecutionContext &) = delete;
  ExecutionContext &operator=(const ExecutionContext &) = delete;

  // Returns the context-scoped aggregate memory pool handle.
  [[nodiscard]] void *memory_pool_handle() const noexcept;

  // Creates an independent operation-scoped pool. A positive limit is a hard
  // quota for allocations routed through this pool; the context pool still
  // aggregates current and peak usage across concurrent operations.
  [[nodiscard]] std::shared_ptr<void>
  make_operation_memory_pool_handle(int64_t limit_bytes) const;

  void set_interrupt_check(InterruptCheck check);
  [[nodiscard]] sanitize::Status CheckInterrupt() const;

private:
  std::shared_ptr<sanitize::internal::MemoryPool> memory_pool_;
  InterruptCheck interrupt_check_;
};

} // namespace sanitize

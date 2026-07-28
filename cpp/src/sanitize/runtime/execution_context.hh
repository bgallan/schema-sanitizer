// Declares execution context ownership and memory-pool access.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

#include "sanitize/core/status.hh"

namespace sanitize::internal {
class MemoryPool;
class PerformanceTelemetry;
} // namespace sanitize::internal

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

  // Starts one operation-local telemetry record and makes it the context
  // snapshot returned by performance_stats(). The caller passes the same
  // operation pool used by the pipeline so peak capacity pressure is exact.
  [[nodiscard]] std::shared_ptr<internal::PerformanceTelemetry>
  begin_performance_telemetry(std::shared_ptr<void> operation_memory_pool,
                              int64_t memory_limit_bytes,
                              int64_t effective_workers, bool multi_mode);

  // Returns the latest operation-local telemetry record, if any.
  [[nodiscard]] std::shared_ptr<internal::PerformanceTelemetry>
  performance_telemetry() const;

  void set_interrupt_check(InterruptCheck check);
  [[nodiscard]] sanitize::Status CheckInterrupt() const;

private:
  std::shared_ptr<sanitize::internal::MemoryPool> memory_pool_;
  InterruptCheck interrupt_check_;
  mutable std::mutex telemetry_mutex_;
  std::shared_ptr<sanitize::internal::PerformanceTelemetry> telemetry_;
  std::uint64_t next_operation_id_ = 1;
};

} // namespace sanitize

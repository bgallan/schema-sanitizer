// Owns execution-scoped allocator access and context lifetime state.

#include "sanitize/runtime/execution_context.hh"

#include "internal/memory/memory_pool.hh"

#include <utility>

namespace sanitize {

ExecutionContext::ExecutionContext() = default;

void *ExecutionContext::memory_pool_handle() noexcept {
  return static_cast<void *>(sanitize::internal::default_memory_pool());
}

void ExecutionContext::set_interrupt_check(InterruptCheck check) {
  interrupt_check_ = std::move(check);
}

sanitize::Status ExecutionContext::CheckInterrupt() const {
  if (!interrupt_check_) {
    return sanitize::Status::OK();
  }
  return interrupt_check_();
}

} // namespace sanitize

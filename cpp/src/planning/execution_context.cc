// Owns execution-scoped allocator access and context lifetime state.

#include "sanitize/runtime/execution_context.hh"

#include "internal/memory/memory_budget.hh"
#include "internal/memory/memory_pool.hh"

#include <algorithm>
#include <limits>
#include <memory>
#include <string_view>
#include <utility>

namespace sanitize {

ExecutionContext::ExecutionContext()
    : memory_pool_(sanitize::internal::make_tracking_memory_pool(
          sanitize::internal::shared_default_memory_pool(), -1,
          "schema_sanitizer::DefaultMemoryPool")) {}

void *ExecutionContext::memory_pool_handle() const noexcept {
  return static_cast<void *>(memory_pool_.get());
}

std::shared_ptr<void>
ExecutionContext::make_operation_memory_pool_handle(int64_t limit_bytes) const {
  const auto budget = sanitize::internal::memory_budget_from_limit(limit_bytes);
  auto pool = sanitize::internal::make_tracking_memory_pool(
      memory_pool_, budget.total_bytes,
      "schema_sanitizer::OperationMemoryPool");
  return std::static_pointer_cast<void>(std::move(pool));
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

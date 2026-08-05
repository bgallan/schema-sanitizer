// Defines bounded worker-side JSONL validation and token capture.

#pragma once

#include "internal/materialization/ingest_stream/parallel_packets.hh"
#include "internal/runtime/execution_policy.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

#include "internal/runtime/thread_compat.hh"
#include <cstddef>
#include <memory>
#include <vector>

namespace sanitize::internal {

// One source-ordered validation packet. The token allowance is a disjoint
// share of the operation-wide JSON token budget.
struct JsonValidationTask {
  OwnedRowPacket owned;
  std::size_t max_token_fields = 0;
};

class ParallelJsonRowValidator final {
public:
  // Creates private parser state for each effective worker while retaining the
  // operation memory pool as the owner of packet token storage.
  [[nodiscard]] static sanitize::Result<
      std::shared_ptr<ParallelJsonRowValidator>>
  Make(std::shared_ptr<void> operation_memory_pool,
       std::shared_ptr<const sanitize::CompiledPlan> plan,
       sanitize::OnErrorPolicy on_error, const ExecutionPolicy &policy);

  // Validates every row before returning the packet. A failure is the first
  // scanner/parser error inside this contiguous packet.
  [[nodiscard]] sanitize::Result<OwnedRowPacket>
  Validate(JsonValidationTask &&task, std::size_t worker_index,
           sanitize::internal::StopToken stop);

private:
  struct WorkerState;

  ParallelJsonRowValidator(std::shared_ptr<void> operation_memory_pool,
                           std::shared_ptr<const sanitize::CompiledPlan> plan,
                           sanitize::OnErrorPolicy on_error) noexcept;

  std::shared_ptr<void> operation_memory_pool_;
  std::shared_ptr<const sanitize::CompiledPlan> plan_;
  sanitize::OnErrorPolicy on_error_ = sanitize::OnErrorPolicy::kStop;
  bool plan_order_candidate_ = false;
  std::vector<std::unique_ptr<WorkerState>> workers_;
};

} // namespace sanitize::internal

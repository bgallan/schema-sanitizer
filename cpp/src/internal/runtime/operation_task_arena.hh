// Owns the bounded native worker set shared by one public operation.
#pragma once

#include "internal/runtime/performance_telemetry.hh"
#include "sanitize/core/status.hh"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stop_token>

namespace sanitize::internal {

// Selects a stable subset of physical workers for one pipeline stage. Upstream
// stages use the low worker indices and sinks use the high worker indices so
// two narrow stages can overlap without creating more than N native workers.
enum class TaskArenaLane : unsigned char {
  kUpstream,
  kOutputCompact,
  kOutput,
  kAll,
};

struct TaskArenaSubmissionPlan final {
  std::size_t lane_begin = 0;
  std::size_t lane_end = 1;
  std::size_t width = 1;
  std::size_t alternative_offset = 1;
  std::uint64_t allowed_mask = 1;
  std::array<std::atomic<std::uint64_t> *, 4> visibility_masks{};
  std::size_t visibility_count = 0;
  std::atomic<std::size_t> *cursor = nullptr;
};

class OperationTaskArena final {
public:
  using Task = std::move_only_function<void(std::size_t, std::stop_token)>;

  static sanitize::Result<std::shared_ptr<OperationTaskArena>>
  Make(std::size_t worker_count,
       std::shared_ptr<PerformanceTelemetry> telemetry = nullptr);

  OperationTaskArena(const OperationTaskArena &) = delete;
  OperationTaskArena &operator=(const OperationTaskArena &) = delete;
  ~OperationTaskArena();

  // Schedules one task on a stable worker inside the selected lane. The task's
  // worker index is relative to the lane, not the physical arena index.
  sanitize::Status
  Submit(Task task, std::size_t lane_width, TaskArenaLane lane,
         TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);

  [[nodiscard]] TaskArenaSubmissionPlan
  PrepareSubmissionPlan(std::size_t lane_width, TaskArenaLane lane) noexcept;

  sanitize::Status
  Submit(Task task, const TaskArenaSubmissionPlan &plan,
         TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);

  // Reserve a round-robin seed from the arena-wide lane cursor. Ordered
  // executors call this once and then advance a mutex-owned local ticket,
  // avoiding one shared atomic RMW per packet.
  [[nodiscard]] std::size_t
  ReserveSubmissionTicket(const TaskArenaSubmissionPlan &plan) noexcept;

  sanitize::Status
  Submit(Task task, const TaskArenaSubmissionPlan &plan,
         std::size_t submission_ticket,
         TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther);

  [[nodiscard]] std::size_t worker_count() const noexcept;
  [[nodiscard]] bool inline_mode() const noexcept;
  [[nodiscard]] std::size_t peak_active_tasks() const noexcept;
  [[nodiscard]] std::size_t active_tasks() const noexcept;
  [[nodiscard]] std::size_t submitted_tasks() const noexcept;
  [[nodiscard]] std::size_t stolen_tasks() const noexcept;
  [[nodiscard]] std::size_t queued_tasks() const noexcept;
  [[nodiscard]] std::size_t started_workers() const noexcept;
  // Sum of targeted park generations published to physical workers. This is
  // observable for scheduler diagnostics without adding a hot-path counter.
  [[nodiscard]] std::uint64_t wake_epoch_publishes() const noexcept;
  [[nodiscard]] std::shared_ptr<PerformanceTelemetry>
  telemetry() const noexcept;

  void Shutdown() noexcept;

public:
  struct State;

private:
  explicit OperationTaskArena(std::shared_ptr<State> state) noexcept;

  std::shared_ptr<State> state_;
};

} // namespace sanitize::internal

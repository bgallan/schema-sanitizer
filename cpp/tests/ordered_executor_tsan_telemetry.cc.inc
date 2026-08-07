// High-core task-telemetry batching race and exactness probe.
[[nodiscard]] bool retryable_telemetry_probe_backpressure(
    const sanitize::Status &status) noexcept {
  return status.code() == sanitize::StatusCode::kOutOfMemory &&
         status.message().find("capacity exhausted") != std::string::npos;
}

template <typename Callable>
sanitize::Status submit_telemetry_probe_task_until(
    sanitize::internal::OperationTaskArena &arena, const Callable &task,
    const sanitize::internal::TaskArenaSubmissionPlan &plan,
    sanitize::internal::TaskTelemetryKind telemetry_kind,
    std::chrono::steady_clock::time_point deadline) {
  constexpr sanitize::internal::TaskMemoryCharge charge{256U};
  for (;;) {
    auto status =
        arena.SubmitCharged(sanitize::internal::OperationTaskArena::Task(task),
                            plan, charge, telemetry_kind);
    if (status.ok() || !retryable_telemetry_probe_backpressure(status) ||
        std::chrono::steady_clock::now() >= deadline) {
      return status;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
}

bool run_high_core_telemetry_batch_round() {
  constexpr std::size_t worker_count = 16U;
  constexpr std::size_t task_count = 513U;
  auto telemetry = std::make_shared<sanitize::internal::PerformanceTelemetry>(
      1U, nullptr, -1, worker_count, true);
  auto arena_result =
      sanitize::internal::OperationTaskArena::Make(worker_count, telemetry);
  if (!arena_result.ok()) {
    return false;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  auto completed = std::make_shared<std::atomic<std::size_t>>(0U);
  const auto plan = arena->PrepareSubmissionPlan(
      worker_count, sanitize::internal::TaskArenaLane::kAll);
  const auto admission_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  for (std::size_t ordinal = 0; ordinal < task_count; ++ordinal) {
    const auto task = [completed](std::size_t,
                                  sanitize::internal::StopToken stop) {
      if (!stop.stop_requested()) {
        completed->fetch_add(1, std::memory_order_release);
      }
    };
    const auto status = submit_telemetry_probe_task_until(
        *arena, task, plan,
        sanitize::internal::TaskTelemetryKind::kMaterialization,
        admission_deadline);
    if (!status.ok()) {
      return false;
    }
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while ((completed->load(std::memory_order_acquire) != task_count ||
          arena->active_tasks() != 0U || arena->queued_tasks() != 0U) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  telemetry->Finish();
  const auto json = telemetry->ToJson();
  const auto exact = std::string{"\"materialization\":{\"submitted\":513,"
                                 "\"started\":513,\"finished\":513"};
  const bool valid = completed->load(std::memory_order_acquire) == task_count &&
                     arena->active_tasks() == 0U &&
                     arena->queued_tasks() == 0U &&
                     json.find(exact) != std::string::npos;
  arena->Shutdown();
  return valid;
}

// Concurrent producers exercise queue-mutex-authorized submission shards.
bool run_worker_submission_telemetry_round() {
  constexpr std::size_t worker_count = 4U;
  constexpr std::size_t producer_count = 4U;
  constexpr std::size_t tasks_per_producer = 1000U;
  constexpr std::size_t task_count = producer_count * tasks_per_producer;
  auto telemetry = std::make_shared<sanitize::internal::PerformanceTelemetry>(
      2U, nullptr, -1, worker_count, true);
  auto arena_result =
      sanitize::internal::OperationTaskArena::Make(worker_count, telemetry);
  if (!arena_result.ok()) {
    return false;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(
      worker_count, sanitize::internal::TaskArenaLane::kAll);
  std::atomic<std::size_t> ready{0};
  auto completed = std::make_shared<std::atomic<std::size_t>>(0U);
  std::atomic<bool> release{false};
  std::atomic<bool> failed{false};
  std::vector<sanitize::internal::JThread> producers;
  producers.reserve(producer_count);
  for (std::size_t producer = 0; producer < producer_count; ++producer) {
    producers.emplace_back([arena, plan, &ready, completed, &release, &failed](
                               sanitize::internal::StopToken stop) mutable {
      ready.fetch_add(1, std::memory_order_release);
      if (!wait_gate_or_stop(&release, stop)) {
        return;
      }
      const auto admission_deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(15);
      for (std::size_t ordinal = 0; ordinal < tasks_per_producer; ++ordinal) {
        const auto task = [completed](std::size_t,
                                      sanitize::internal::StopToken task_stop) {
          if (!task_stop.stop_requested()) {
            completed->fetch_add(1, std::memory_order_release);
          }
        };
        const auto status = submit_telemetry_probe_task_until(
            *arena, task, plan, sanitize::internal::TaskTelemetryKind::kOther,
            admission_deadline);
        if (!status.ok()) {
          failed.store(true, std::memory_order_release);
          return;
        }
      }
    });
  }
  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (ready.load(std::memory_order_acquire) != producer_count &&
         std::chrono::steady_clock::now() < startup_deadline) {
    std::this_thread::yield();
  }
  if (ready.load(std::memory_order_acquire) != producer_count) {
    release_gate(&release);
    return false;
  }
  release_gate(&release);
  for (auto &producer : producers) {
    producer.join();
  }
  if (failed.load(std::memory_order_acquire)) {
    return false;
  }
  const auto drain_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while ((completed->load(std::memory_order_acquire) != task_count ||
          arena->active_tasks() != 0U || arena->queued_tasks() != 0U) &&
         std::chrono::steady_clock::now() < drain_deadline) {
    std::this_thread::yield();
  }
  telemetry->Finish();
  const auto json = telemetry->ToJson();
  const auto exact = std::string{"\"other\":{\"submitted\":4000,"
                                 "\"started\":4000,\"finished\":4000"};
  const bool valid = completed->load(std::memory_order_acquire) == task_count &&
                     arena->active_tasks() == 0U &&
                     arena->queued_tasks() == 0U &&
                     json.find(exact) != std::string::npos;
  arena->Shutdown();
  return valid;
}

// Serializes exact operation telemetry, including worker-local shards.
std::string PerformanceTelemetry::ToJson() const {
  const auto finished_ns = finished_ns_.load(std::memory_order_acquire);
  const auto observed_end = finished_ns > 0 ? finished_ns : NowNs();
  const auto elapsed_ns = nonnegative(observed_end - started_ns_);
  const auto pool_current =
      operation_pool_ ? operation_pool_->bytes_allocated() : std::int64_t{0};
  const auto pool_peak =
      operation_pool_ ? operation_pool_->max_memory() : std::int64_t{0};
  const auto pool_limit =
      operation_pool_ ? operation_pool_->limit_bytes() : memory_limit_bytes_;

  const auto phase_value = [this](PerformancePhase phase) noexcept {
    return phase_ns_[static_cast<std::size_t>(phase)].load(
        std::memory_order_relaxed);
  };
  const auto task_sum = [this](const auto &global_values,
                               auto WorkerTaskTelemetryShard::*member,
                               std::size_t kind_index) noexcept {
    auto total = global_values[kind_index].load(std::memory_order_relaxed);
    for (const auto &shard : worker_task_shards_) {
      total += (shard.*member)[kind_index].load(std::memory_order_relaxed);
    }
    return total;
  };
  const auto task_submitted_sum = [this](std::size_t kind_index) noexcept {
    auto total = task_submitted_[kind_index].load(std::memory_order_relaxed);
    for (const auto &shard : worker_submission_shards_) {
      total += shard.submitted[kind_index].load(std::memory_order_relaxed);
    }
    return total;
  };
  const auto peak_queue_depth = [this]() noexcept {
    auto maximum = peak_queue_depth_.load(std::memory_order_relaxed);
    for (const auto &shard : worker_submission_shards_) {
      maximum = std::max(
          maximum, shard.peak_queue_depth.load(std::memory_order_relaxed));
    }
    return maximum;
  };
  const auto task_maximum = [this](const auto &global_values,
                                   auto WorkerTaskTelemetryShard::*member,
                                   std::size_t kind_index) noexcept {
    auto maximum = global_values[kind_index].load(std::memory_order_relaxed);
    for (const auto &shard : worker_task_shards_) {
      maximum = std::max(
          maximum, (shard.*member)[kind_index].load(std::memory_order_relaxed));
    }
    return maximum;
  };
  const auto task_run = [&](TaskTelemetryKind kind) noexcept {
    return task_sum(task_run_ns_, &WorkerTaskTelemetryShard::run_ns,
                    static_cast<std::size_t>(kind));
  };
  const auto telemetry_batch_count = [this]() noexcept {
    auto total =
        counters_[static_cast<std::size_t>(
                      PerformanceCounter::kLowCoreTaskTelemetryBatches)]
            .load(std::memory_order_relaxed);
    for (const auto &shard : worker_task_shards_) {
      total += shard.batches.load(std::memory_order_relaxed);
    }
    return total;
  };
  const auto stolen_task_count = [this]() noexcept {
    auto total = stolen_tasks_.load(std::memory_order_relaxed);
    for (const auto &shard : worker_task_shards_) {
      total += shard.stolen.load(std::memory_order_relaxed);
    }
    return total;
  };
  const auto worker_active_streak_count = [this]() noexcept {
    auto total = counters_[static_cast<std::size_t>(
                               PerformanceCounter::kWorkerActiveStreaks)]
                     .load(std::memory_order_relaxed);
    for (const auto &shard : worker_task_shards_) {
      total += shard.active_streaks.load(std::memory_order_relaxed);
    }
    return total;
  };
  const auto stream_ns = phase_value(PerformancePhase::kStreamGetNext);
  const auto frontend_ns = phase_value(PerformancePhase::kFrontendRead);
  const auto coordinator_wait_ns =
      phase_value(PerformancePhase::kCoordinatorWait);
  const auto arrow_terminal_ns = phase_value(PerformancePhase::kArrowMerge) +
                                 phase_value(PerformancePhase::kArrowFinalize);
  const auto materialization_run_ns =
      task_run(TaskTelemetryKind::kMaterialization);
  const auto validation_run_ns = task_run(TaskTelemetryKind::kJsonValidation);
  const auto worker_run_ns = materialization_run_ns + validation_run_ns;
  const auto peak_active_tasks =
      peak_active_tasks_.load(std::memory_order_relaxed);
  const auto materialization_tasks = task_submitted_sum(
      static_cast<std::size_t>(TaskTelemetryKind::kMaterialization));
  const auto validation_tasks = task_submitted_sum(
      static_cast<std::size_t>(TaskTelemetryKind::kJsonValidation));
  const auto diagnosis =
      classify(stream_ns, frontend_ns, coordinator_wait_ns, arrow_terminal_ns,
               worker_run_ns, pool_peak, pool_limit, effective_workers_,
               peak_active_tasks, materialization_tasks + validation_tasks);

  std::string json;
  json.reserve(4096);
  json.push_back('{');
  bool first = true;
  append_int_field(json, first, "schema_version", 1);
  append_int_field(
      json, first, "operation_id",
      static_cast<std::int64_t>(std::min<std::uint64_t>(
          operation_id_, static_cast<std::uint64_t>(
                             std::numeric_limits<std::int64_t>::max()))));
  append_bool_field(json, first, "finished", finished_ns > 0);
  append_string_field(json, first, "threading_mode",
                      multi_mode_ ? "multi" : "single");
  append_int_field(json, first, "effective_workers", effective_workers_);
  append_int_field(json, first, "elapsed_ns", elapsed_ns);

  append_key(json, first, "memory");
  json.push_back('{');
  bool memory_first = true;
  append_int_field(json, memory_first, "current_bytes", pool_current);
  append_int_field(json, memory_first, "peak_bytes", pool_peak);
  append_int_field(json, memory_first, "limit_bytes", pool_limit);
  append_double_field(json, memory_first, "peak_to_limit_ratio",
                      ratio(pool_peak, pool_limit));
  append_bool_field(json, memory_first, "capacity_pressure",
                    diagnosis.memory_capacity_pressure);
  append_bool_field(json, memory_first, "bandwidth_proven",
                    diagnosis.memory_bandwidth_proven);
  json.push_back('}');

  append_key(json, first, "phases");
  json.push_back('{');
  bool phases_first = true;
  for (std::size_t index = 0; index < kPhaseCount; ++index) {
    append_key(json, phases_first, kPhaseNames[index]);
    json.push_back('{');
    bool phase_first = true;
    const auto elapsed = phase_ns_[index].load(std::memory_order_relaxed);
    append_int_field(json, phase_first, "elapsed_ns", elapsed);
    append_int_field(json, phase_first, "calls",
                     phase_calls_[index].load(std::memory_order_relaxed));
    append_double_field(json, phase_first, "operation_share",
                        ratio(elapsed, elapsed_ns));
    json.push_back('}');
  }
  json.push_back('}');

  append_key(json, first, "tasks");
  json.push_back('{');
  bool tasks_first = true;
  for (std::size_t index = 0; index < kTaskKindCount; ++index) {
    append_key(json, tasks_first, kTaskKindNames[index]);
    json.push_back('{');
    bool task_first = true;
    const auto submitted = task_submitted_sum(index);
    const auto started =
        task_sum(task_started_, &WorkerTaskTelemetryShard::started, index);
    const auto finished =
        task_sum(task_finished_, &WorkerTaskTelemetryShard::finished, index);
    const auto run_ns =
        task_sum(task_run_ns_, &WorkerTaskTelemetryShard::run_ns, index);
    const auto queue_wait_ns = task_sum(
        task_queue_wait_ns_, &WorkerTaskTelemetryShard::queue_wait_ns, index);
    append_int_field(json, task_first, "submitted", submitted);
    append_int_field(json, task_first, "started", started);
    append_int_field(json, task_first, "finished", finished);
    append_int_field(json, task_first, "run_ns", run_ns);
    append_int_field(json, task_first, "queue_wait_ns", queue_wait_ns);
    append_int_field(json, task_first, "max_run_ns",
                     task_maximum(task_max_run_ns_,
                                  &WorkerTaskTelemetryShard::max_run_ns,
                                  index));
    append_int_field(json, task_first, "max_queue_wait_ns",
                     task_maximum(task_max_queue_wait_ns_,
                                  &WorkerTaskTelemetryShard::max_queue_wait_ns,
                                  index));
    append_double_field(json, task_first, "average_run_ns",
                        ratio(run_ns, finished));
    append_double_field(json, task_first, "average_queue_wait_ns",
                        ratio(queue_wait_ns, started));
    json.push_back('}');
  }
  json.push_back('}');

  append_key(json, first, "counters");
  json.push_back('{');
  bool counters_first = true;
  for (std::size_t index = 0; index < kCounterCount; ++index) {
    const auto value =
        index == static_cast<std::size_t>(
                     PerformanceCounter::kLowCoreTaskTelemetryBatches)
            ? telemetry_batch_count()
        : index == static_cast<std::size_t>(
                       PerformanceCounter::kWorkerActiveStreaks)
            ? worker_active_streak_count()
            : counters_[index].load(std::memory_order_relaxed);
    append_int_field(json, counters_first, kCounterNames[index], value);
  }
  append_int_field(json, counters_first, "peak_queue_depth",
                   peak_queue_depth());
  append_int_field(json, counters_first, "peak_active_tasks",
                   peak_active_tasks_.load(std::memory_order_relaxed));
  append_int_field(json, counters_first, "stolen_tasks", stolen_task_count());
  append_int_field(json, counters_first, "started_workers",
                   started_workers_.load(std::memory_order_relaxed));
  json.push_back('}');

  append_key(json, first, "diagnosis");
  json.push_back('{');
  bool diagnosis_first = true;
  append_string_field(json, diagnosis_first, "primary", diagnosis.primary);
  append_string_field(json, diagnosis_first, "confidence",
                      diagnosis.confidence);
  append_bool_field(json, diagnosis_first, "memory_capacity_pressure",
                    diagnosis.memory_capacity_pressure);
  append_string_field(
      json, diagnosis_first, "memory_capacity_status",
      diagnosis.memory_capacity_pressure
          ? "near_operation_budget_not_proof_of_capacity_bottleneck"
          : "no_operation_budget_pressure_observed");
  append_bool_field(json, diagnosis_first, "memory_bandwidth_proven",
                    diagnosis.memory_bandwidth_proven);
  append_string_field(
      json, diagnosis_first, "memory_bandwidth_status",
      "requires_hardware_counters_and_sustainable_bandwidth_baseline");
  append_double_field(json, diagnosis_first, "coordinator_wait_share",
                      ratio(coordinator_wait_ns, stream_ns));
  append_double_field(json, diagnosis_first, "frontend_share",
                      ratio(frontend_ns, stream_ns));
  append_double_field(json, diagnosis_first, "arrow_terminal_share",
                      ratio(arrow_terminal_ns, stream_ns));
  append_double_field(
      json, diagnosis_first, "materialization_worker_parallelism",
      ratio(materialization_run_ns,
            stream_ns * std::max<std::int64_t>(1, effective_workers_)));
  append_double_field(
      json, diagnosis_first, "json_validation_worker_parallelism",
      ratio(validation_run_ns,
            stream_ns * std::max<std::int64_t>(1, effective_workers_)));
  append_double_field(
      json, diagnosis_first, "combined_worker_parallelism",
      ratio(worker_run_ns,
            stream_ns * std::max<std::int64_t>(1, effective_workers_)));
  json.push_back('}');
  json.push_back('}');
  return json;
}

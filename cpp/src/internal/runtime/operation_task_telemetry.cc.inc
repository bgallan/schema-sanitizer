// Batches task telemetry without sharing completion cache lines between
// workers.
class TaskTelemetryBatch final {
public:
  TaskTelemetryBatch(PerformanceTelemetry *telemetry, std::size_t worker_index,
                     std::size_t worker_count) noexcept
      : telemetry_(telemetry), worker_index_(worker_index),
        flush_tasks_(worker_count > 8U ? 32U : 8U) {}
  ~TaskTelemetryBatch() { Flush(); }

  void Record(TaskTelemetryKind kind, std::int64_t queue_wait_ns,
              std::int64_t run_ns) noexcept {
    if (!telemetry_) {
      return;
    }
    const auto index = static_cast<std::size_t>(kind);
    if (index >= totals_.size()) {
      return;
    }
    const auto queue_wait = std::max<std::int64_t>(0, queue_wait_ns);
    const auto run = std::max<std::int64_t>(0, run_ns);
    auto &total = totals_[index];
    ++total.count;
    total.queue_wait_ns += queue_wait;
    total.run_ns += run;
    total.max_queue_wait_ns = std::max(total.max_queue_wait_ns, queue_wait);
    total.max_run_ns = std::max(total.max_run_ns, run);
    if (++pending_ >= flush_tasks_) {
      Flush();
    }
  }

  void Flush() noexcept {
    if (!telemetry_ || pending_ == 0) {
      return;
    }
    for (std::size_t index = 0; index < totals_.size(); ++index) {
      auto &total = totals_[index];
      if (total.count == 0) {
        continue;
      }
      telemetry_->RecordWorkerTaskBatch(
          worker_index_, static_cast<TaskTelemetryKind>(index), total.count,
          total.queue_wait_ns, total.run_ns, total.max_queue_wait_ns,
          total.max_run_ns);
      total = {};
    }
    pending_ = 0;
  }

private:
  struct Totals final {
    std::int64_t count = 0;
    std::int64_t queue_wait_ns = 0;
    std::int64_t run_ns = 0;
    std::int64_t max_queue_wait_ns = 0;
    std::int64_t max_run_ns = 0;
  };
  static constexpr std::size_t kTaskKindCount =
      static_cast<std::size_t>(TaskTelemetryKind::kCount);
  PerformanceTelemetry *telemetry_ = nullptr;
  std::size_t worker_index_ = 0;
  std::array<Totals, kTaskKindCount> totals_{};
  std::size_t pending_ = 0;
  std::size_t flush_tasks_ = 8;
};

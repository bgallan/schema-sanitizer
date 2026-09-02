// Implements native runtime probes for Python integration and concurrency
// tests. The probes expose deterministic admission and completion behavior to
// the Python test suite.

#include "internal/abi/python_abi3/methods.hh"
#include "internal/output/output_worker_admission.hh"
#include "internal/runtime/execution_policy.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor.hh"
#include "internal/runtime/process_cpu_governor.hh"
#include "internal/runtime/thread_compat.hh"
#include "sanitize/options/options.hh"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// ---- arena_scheduler_probe ----
namespace core_abi3_internal {
namespace {

using sanitize::internal::OperationTaskArena;
using sanitize::internal::TaskArenaLane;
using sanitize::internal::TaskMemoryCharge;
using sanitize::internal::TaskTelemetryKind;

constexpr TaskMemoryCharge kProbeTaskCharge{256U};

/// Classifies whether probe backpressure permits a bounded retry.
[[nodiscard]] bool
retryable_probe_backpressure(const sanitize::Status &status) noexcept {
  return status.code() == sanitize::StatusCode::kOutOfMemory &&
         status.message().find("capacity exhausted") != std::string::npos;
}

/// Retries probe-task admission while backpressure remains transient and before
/// the deadline.
template <typename Callable>
sanitize::Status
submit_probe_task_until(OperationTaskArena &arena, const Callable &task,
                        const sanitize::internal::TaskArenaSubmissionPlan &plan,
                        TaskTelemetryKind telemetry_kind,
                        std::chrono::steady_clock::time_point deadline) {
  for (;;) {
    auto status = arena.SubmitCharged(OperationTaskArena::Task(task), plan,
                                      kProbeTaskCharge, telemetry_kind);
    if (status.ok() || !retryable_probe_backpressure(status) ||
        std::chrono::steady_clock::now() >= deadline) {
      return status;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
}

/// Retries probe-task admission while backpressure remains transient and before
/// the deadline.
template <typename Callable>
sanitize::Status submit_probe_task_until(
    OperationTaskArena &arena, const Callable &task, std::size_t lane_width,
    TaskArenaLane lane, std::chrono::steady_clock::time_point deadline,
    TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther) {
  for (;;) {
    auto status =
        arena.SubmitCharged(OperationTaskArena::Task(task), lane_width, lane,
                            kProbeTaskCharge, telemetry_kind);
    if (status.ok() || !retryable_probe_backpressure(status) ||
        std::chrono::steady_clock::now() >= deadline) {
      return status;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
}

/// Opens the probe gate and wakes every waiting task.
void release_gate(std::atomic<bool> *gate) noexcept {
  gate->store(true, std::memory_order_release);
  gate->notify_all();
}

/// Waits until the probe gate opens or task cancellation is requested.
bool wait_gate_or_stop(std::atomic<bool> *gate,
                       sanitize::internal::StopToken stop) {
  auto release_on_stop = [gate] { release_gate(gate); };
  sanitize::internal::StopCallback<decltype(release_on_stop)> stop_gate(
      stop, std::move(release_on_stop));
  while (!gate->load(std::memory_order_acquire) && !stop.stop_requested()) {
    sanitize::internal::WaitOnAtomic(*gate, false, std::memory_order_acquire);
  }
  return !stop.stop_requested();
}

/// Polls the supplied predicate until it succeeds or the deadline expires.
[[nodiscard]] bool wait_until(const std::atomic<std::size_t> &value,
                              std::size_t target,
                              std::chrono::steady_clock::time_point deadline) {
  while (value.load(std::memory_order_acquire) < target &&
         std::chrono::steady_clock::now() < deadline) {
    // SwitchToThread may immediately return to this polling thread on
    // oversubscribed Windows runners. A bounded sleep gives arena workers a
    // deterministic opportunity to drain their queues.
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  return value.load(std::memory_order_acquire) >= target;
}

} // namespace

/// Exercises concurrent task submission and reports completion invariants.
PyObject *py_operation_task_arena_concurrent_submit_probe(PyObject *,
                                                          PyObject *args) {
  int requested_workers = 4;
  int requested_producers = 2;
  int tasks_per_producer = 1000;
  if (!PyArg_ParseTuple(
          args, "iii:operation_task_arena_concurrent_submit_probe",
          &requested_workers, &requested_producers, &tasks_per_producer)) {
    return nullptr;
  }
  if (requested_workers < 2 || requested_workers > 32 ||
      requested_producers < 2 || requested_producers > 16 ||
      tasks_per_producer < 1 || tasks_per_producer > 100000) {
    PyErr_SetString(PyExc_ValueError,
                    "workers must be within [2, 32], producers within [2, 16], "
                    "and tasks_per_producer within [1, 100000]");
    return nullptr;
  }

  const auto workers = static_cast<std::size_t>(requested_workers);
  const auto producers = static_cast<std::size_t>(requested_producers);
  const auto per_producer = static_cast<std::size_t>(tasks_per_producer);
  const auto task_count = producers * per_producer;
  auto arena_result = OperationTaskArena::Make(workers);
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(workers, TaskArenaLane::kAll);
  std::atomic<std::size_t> producers_ready{0};
  std::atomic<std::size_t> tasks_finished{0};
  std::atomic<bool> release_producers{false};
  std::atomic<bool> submit_failed{false};
  std::vector<sanitize::internal::JThread> producer_threads;
  producer_threads.reserve(producers);

  for (std::size_t producer = 0; producer < producers; ++producer) {
    producer_threads.emplace_back(
        [arena, plan, per_producer, &producers_ready, &tasks_finished,
         &release_producers,
         &submit_failed](sanitize::internal::StopToken stop) mutable {
          producers_ready.fetch_add(1, std::memory_order_release);
          if (!wait_gate_or_stop(&release_producers, stop)) {
            return;
          }
          const auto admission_deadline =
              std::chrono::steady_clock::now() + std::chrono::seconds(15);
          for (std::size_t task_index = 0; task_index < per_producer;
               ++task_index) {
            const auto task =
                [&tasks_finished](std::size_t,
                                  sanitize::internal::StopToken task_stop) {
                  if (!task_stop.stop_requested()) {
                    tasks_finished.fetch_add(1, std::memory_order_release);
                  }
                };
            auto status = submit_probe_task_until(*arena, task, plan,
                                                  TaskTelemetryKind::kOther,
                                                  admission_deadline);
            if (!status.ok()) {
              submit_failed.store(true, std::memory_order_release);
              return;
            }
          }
        });
  }

  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  if (!wait_until(producers_ready, producers, startup_deadline)) {
    release_gate(&release_producers);
    PyErr_SetString(PyExc_RuntimeError,
                    "concurrent-submit probe producers did not start");
    return nullptr;
  }
  const auto started_at = std::chrono::steady_clock::now();
  release_gate(&release_producers);
  for (auto &producer : producer_threads) {
    producer.join();
  }
  if (submit_failed.load(std::memory_order_acquire)) {
    PyErr_SetString(PyExc_RuntimeError,
                    "concurrent-submit probe admission failed");
    return nullptr;
  }
  const auto drain_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(15);
  if (!wait_until(tasks_finished, task_count, drain_deadline)) {
    PyErr_SetString(PyExc_RuntimeError,
                    "concurrent-submit probe did not drain all tasks");
    return nullptr;
  }
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - started_at);

  PyObject *result = PyTuple_New(6);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(
          result, 0,
          PyLong_FromLongLong(static_cast<long long>(elapsed.count()))) ||
      !tuple_set_item_steal(result, 1,
                            PyLong_FromSize_t(arena->submitted_tasks())) ||
      !tuple_set_item_steal(
          result, 2,
          PyLong_FromSize_t(tasks_finished.load(std::memory_order_acquire))) ||
      !tuple_set_item_steal(result, 3,
                            PyLong_FromSize_t(arena->queued_tasks())) ||
      !tuple_set_item_steal(result, 4,
                            PyLong_FromSize_t(arena->started_workers())) ||
      !tuple_set_item_steal(result, 5,
                            PyLong_FromSize_t(arena->peak_active_tasks()))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

/// Runs mixed-lane arena traffic and reports per-lane completion behavior.
PyObject *py_operation_task_arena_mixed_lane_probe(PyObject *, PyObject *args) {
  int requested_workers = 16;
  int rounds = 1000;
  if (!PyArg_ParseTuple(args, "ii:operation_task_arena_mixed_lane_probe",
                        &requested_workers, &rounds)) {
    return nullptr;
  }
  if (requested_workers < 4 || requested_workers > 32 || rounds < 1 ||
      rounds > 100000) {
    PyErr_SetString(PyExc_ValueError,
                    "workers must be within [4, 32] and rounds within "
                    "[1, 100000]");
    return nullptr;
  }

  auto arena_result =
      OperationTaskArena::Make(static_cast<std::size_t>(requested_workers));
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  const auto workers = static_cast<std::size_t>(requested_workers);
  const auto half = workers / 2U;
  const auto blockers_per_lane = std::max<std::size_t>(1U, half / 2U);
  const auto blocker_count = blockers_per_lane * 2U;
  const auto work_count = static_cast<std::size_t>(rounds) * 3U;
  std::atomic<std::size_t> blockers_started{0};
  std::atomic<std::size_t> blockers_finished{0};
  std::atomic<std::size_t> work_finished{0};
  std::atomic<bool> release_blockers{false};

  const auto blocker = [&](std::size_t, sanitize::internal::StopToken stop) {
    blockers_started.fetch_add(1, std::memory_order_release);
    (void)wait_gate_or_stop(&release_blockers, stop);
    blockers_finished.fetch_add(1, std::memory_order_release);
  };
  const auto blocker_admission_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(15);
  for (std::size_t index = 0; index < blockers_per_lane; ++index) {
    auto status =
        submit_probe_task_until(*arena, blocker, half, TaskArenaLane::kUpstream,
                                blocker_admission_deadline);
    if (status.ok()) {
      status =
          submit_probe_task_until(*arena, blocker, half, TaskArenaLane::kOutput,
                                  blocker_admission_deadline);
    }
    if (!status.ok()) {
      release_gate(&release_blockers);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }

  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const auto startup_target = std::min<std::size_t>(
      blocker_count,
      static_cast<std::size_t>(
          sanitize::internal::process_cpu_governor().capacity()));
  if (!wait_until(blockers_started, startup_target, startup_deadline)) {
    release_gate(&release_blockers);
    PyErr_SetString(PyExc_RuntimeError,
                    "mixed-lane probe blockers did not start");
    return nullptr;
  }
  // The probe validates lane scheduling/stealing, not deliberate CPU
  // oversubscription. Release the prewarm blockers once the dynamic CPU
  // governor has admitted its runnable window; queued blockers then drain
  // ahead of ordinary work while spare CPU leases remain available.
  release_gate(&release_blockers);

  const auto work = [&](std::size_t, sanitize::internal::StopToken stop) {
    if (!stop.stop_requested()) {
      work_finished.fetch_add(1, std::memory_order_release);
    }
  };
  const auto started_at = std::chrono::steady_clock::now();
  const auto work_admission_deadline = started_at + std::chrono::seconds(15);
  for (int round = 0; round < rounds; ++round) {
    auto status = submit_probe_task_until(
        *arena, work, half, TaskArenaLane::kUpstream, work_admission_deadline);
    if (status.ok()) {
      status = submit_probe_task_until(
          *arena, work, half, TaskArenaLane::kOutput, work_admission_deadline);
    }
    if (status.ok()) {
      status = submit_probe_task_until(
          *arena, work, workers, TaskArenaLane::kAll, work_admission_deadline);
    }
    if (!status.ok()) {
      release_gate(&release_blockers);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }

  const auto steal_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (arena->stolen_tasks() == 0U &&
         std::chrono::steady_clock::now() < steal_deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  const auto observed_steal = arena->stolen_tasks() > 0U;
  release_gate(&release_blockers);
  const auto drain_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(15);
  const auto work_drained =
      wait_until(work_finished, work_count, drain_deadline);
  const auto blockers_drained =
      wait_until(blockers_finished, blocker_count, drain_deadline);
  if (!observed_steal) {
    PyErr_SetString(PyExc_RuntimeError,
                    "mixed-lane probe did not observe compatible stealing");
    return nullptr;
  }
  if (!work_drained || !blockers_drained) {
    PyErr_SetString(PyExc_RuntimeError,
                    "mixed-lane probe did not drain all tasks");
    return nullptr;
  }
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - started_at);

  PyObject *result = PyTuple_New(7);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(
          result, 0,
          PyLong_FromLongLong(static_cast<long long>(elapsed.count()))) ||
      !tuple_set_item_steal(result, 1,
                            PyLong_FromSize_t(arena->stolen_tasks())) ||
      !tuple_set_item_steal(result, 2,
                            PyLong_FromSize_t(arena->started_workers())) ||
      !tuple_set_item_steal(result, 3,
                            PyLong_FromSize_t(arena->peak_active_tasks())) ||
      !tuple_set_item_steal(result, 4,
                            PyLong_FromSize_t(work_finished.load())) ||
      !tuple_set_item_steal(result, 5,
                            PyLong_FromSize_t(arena->queued_tasks())) ||
      !tuple_set_item_steal(result, 6,
                            PyLong_FromSize_t(arena->submitted_tasks()))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

/// Measures output-lane preference while upstream work is also queued.
PyObject *py_operation_task_arena_output_preference_probe(PyObject *,
                                                          PyObject *args) {
  int requested_workers = 16;
  int output_waves = 1;
  if (!PyArg_ParseTuple(args,
                        "i|i:operation_task_arena_output_preference_probe",
                        &requested_workers, &output_waves)) {
    return nullptr;
  }
  if (requested_workers < 4 || requested_workers > 32 || output_waves < 1 ||
      output_waves > 2) {
    PyErr_SetString(PyExc_ValueError,
                    "workers must be within [4, 32] and output_waves must "
                    "be 1 or 2");
    return nullptr;
  }
  const auto workers = static_cast<std::size_t>(requested_workers);
  const auto high_begin = workers / 2U;
  auto arena_result = OperationTaskArena::Make(workers);
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();

  // The low-core contract reports the complete physical worker budget, while
  // production startup intentionally remains demand-driven. Prewarm each
  // low-core slot behind a probe-only gate before arranging the measured
  // queues, so short callbacks cannot finish soon enough for their slot to be
  // reused while another physical worker is still unstarted.
  if (workers <= 8U) {
    struct PrewarmState {
      std::atomic<std::size_t> finished{0};
      std::atomic<bool> release{false};
    };
    auto prewarm = std::make_shared<PrewarmState>();
    const auto all_plan =
        arena->PrepareSubmissionPlan(workers, TaskArenaLane::kAll);
    for (std::size_t ordinal = 0; ordinal < workers; ++ordinal) {
      auto status = arena->Submit(
          [prewarm](std::size_t, sanitize::internal::StopToken stop) {
            (void)wait_gate_or_stop(&prewarm->release, stop);
            prewarm->finished.fetch_add(1, std::memory_order_release);
          },
          all_plan, ordinal);
      if (!status.ok()) {
        release_gate(&prewarm->release);
        PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
        return nullptr;
      }
    }
    if (arena->started_workers() != workers) {
      release_gate(&prewarm->release);
      PyErr_SetString(PyExc_RuntimeError,
                      "output preference probe did not prewarm every worker");
      return nullptr;
    }
    release_gate(&prewarm->release);
    const auto prewarm_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    const auto prewarm_drained =
        wait_until(prewarm->finished, workers, prewarm_deadline);
    while ((arena->queued_tasks() != 0U || arena->active_tasks() != 0U) &&
           std::chrono::steady_clock::now() < prewarm_deadline) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    if (!prewarm_drained || arena->queued_tasks() != 0U ||
        arena->active_tasks() != 0U) {
      PyErr_SetString(PyExc_RuntimeError,
                      "output preference probe prewarm did not drain");
      return nullptr;
    }
  }

  std::atomic<std::size_t> blockers_started{0};
  std::atomic<std::size_t> high_outputs_finished{0};
  std::atomic<std::size_t> broad_finished{0};
  std::atomic<bool> release_high{false};

  // Put one blocker at the front of every high-output worker queue. Broad and
  // dedicated output packets are queued behind it before the gate opens. The
  // dynamic CPU governor therefore needs to run only a subset of blockers at
  // once; after release it can rotate across all physical lanes without
  // changing the local FIFO/preference ordering being measured.
  const auto high_workers = workers - high_begin;
  for (std::size_t ordinal = 0; ordinal < high_workers; ++ordinal) {
    auto status = arena->Submit(
        [&](std::size_t, sanitize::internal::StopToken stop) {
          blockers_started.fetch_add(1, std::memory_order_release);
          (void)wait_gate_or_stop(&release_high, stop);
        },
        high_workers, TaskArenaLane::kOutput);
    if (!status.ok()) {
      release_gate(&release_high);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const auto startup_target = std::min<std::size_t>(
      high_workers, static_cast<std::size_t>(
                        sanitize::internal::process_cpu_governor().capacity()));
  if (!wait_until(blockers_started, startup_target, startup_deadline)) {
    release_gate(&release_high);
    PyErr_SetString(PyExc_RuntimeError,
                    "output preference blockers did not start");
    return nullptr;
  }

  for (std::size_t ordinal = 0; ordinal < workers; ++ordinal) {
    auto status = arena->Submit(
        [&](std::size_t worker_index, sanitize::internal::StopToken stop) {
          if (stop.stop_requested()) {
            return;
          }
          if (worker_index >= high_begin) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
          }
          broad_finished.fetch_add(1, std::memory_order_release);
        },
        workers, TaskArenaLane::kAll);
    if (!status.ok()) {
      release_gate(&release_high);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  for (int wave = 0; wave < output_waves; ++wave) {
    for (std::size_t ordinal = 0; ordinal < workers / 2U; ++ordinal) {
      auto status = arena->Submit(
          [&](std::size_t, sanitize::internal::StopToken stop) {
            if (stop.stop_requested()) {
              return;
            }
            high_outputs_finished.fetch_add(1, std::memory_order_release);
          },
          workers / 2U, TaskArenaLane::kOutput, TaskTelemetryKind::kOutput);
      if (!status.ok()) {
        release_gate(&release_high);
        PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
        return nullptr;
      }
    }
  }

  const auto expected_outputs =
      workers / 2U * static_cast<std::size_t>(output_waves);
  const auto released_at = std::chrono::steady_clock::now();
  release_gate(&release_high);
  const auto high_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const auto high_outputs_drained =
      wait_until(high_outputs_finished, expected_outputs, high_deadline);
  const auto output_elapsed =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - released_at);
  const auto broad_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const auto broad_drained =
      wait_until(broad_finished, workers, broad_deadline);
  if (!high_outputs_drained || !broad_drained) {
    PyErr_SetString(PyExc_RuntimeError,
                    "output preference probe did not drain all tasks");
    return nullptr;
  }

  PyObject *result = PyTuple_New(6);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(
          result, 0, PyLong_FromSize_t(arena->output_preference_bypasses())) ||
      !tuple_set_item_steal(result, 1,
                            PyLong_FromSize_t(high_outputs_finished.load(
                                std::memory_order_acquire))) ||
      !tuple_set_item_steal(
          result, 2,
          PyLong_FromSize_t(broad_finished.load(std::memory_order_acquire))) ||
      !tuple_set_item_steal(result, 3,
                            PyLong_FromSize_t(arena->started_workers())) ||
      !tuple_set_item_steal(result, 4,
                            PyLong_FromSize_t(arena->queued_tasks())) ||
      !tuple_set_item_steal(result, 5,
                            PyLong_FromLongLong(static_cast<long long>(
                                output_elapsed.count())))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

/// Verifies that eligible workers can steal queued output-lane work.
PyObject *py_operation_task_arena_output_steal_probe(PyObject *,
                                                     PyObject *args) {
  int requested_workers = 16;
  if (!PyArg_ParseTuple(args, "|i:operation_task_arena_output_steal_probe",
                        &requested_workers)) {
    return nullptr;
  }
  if (requested_workers < 4 || requested_workers > 32) {
    PyErr_SetString(PyExc_ValueError, "workers must be within [4, 32]");
    return nullptr;
  }

  const auto workers = static_cast<std::size_t>(requested_workers);
  const auto half = workers / 2U;
  const auto output_count = half - 1U;
  const auto broad_count = workers - 1U;
  const auto cpu_window = std::min<std::size_t>(
      workers, static_cast<std::size_t>(
                   sanitize::internal::process_cpu_governor().capacity()));
  auto arena_result = OperationTaskArena::Make(workers);
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  const auto all_plan =
      arena->PrepareSubmissionPlan(workers, TaskArenaLane::kAll);
  const auto output_plan =
      arena->PrepareSubmissionPlan(half, TaskArenaLane::kOutput);
  const auto helper_index = workers - 1U;

  std::array<std::atomic<bool>, 32> release_worker{};
  std::atomic<std::size_t> blockers_started{0};
  std::atomic<std::size_t> outputs_finished{0};
  std::atomic<std::size_t> outputs_before_broad{0};
  std::atomic<std::size_t> broad_started{0};
  std::atomic<std::size_t> broad_finished{0};
  std::atomic<bool> release_broad{false};

  const auto blocker = [&](std::size_t worker_index,
                           sanitize::internal::StopToken stop) {
    blockers_started.fetch_add(1, std::memory_order_release);
    while (!release_worker[worker_index].load(std::memory_order_acquire) &&
           !stop.stop_requested()) {
      std::this_thread::yield();
    }
  };

  // Start the high-lane helper first and place only enough additional blockers
  // to occupy the *actual* runnable CPU window. Submission tickets make the
  // helper/victim geometry deterministic without requiring every physical
  // worker to run simultaneously (which would contradict ProcessCpuGovernor).
  auto status = arena->Submit(blocker, all_plan, helper_index);
  if (!status.ok()) {
    PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
    return nullptr;
  }
  for (std::size_t ordinal = 0; ordinal + 1U < cpu_window; ++ordinal) {
    status = arena->Submit(blocker, all_plan, ordinal);
    if (!status.ok()) {
      for (auto &release : release_worker) {
        release.store(true, std::memory_order_release);
      }
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  if (!wait_until(blockers_started, cpu_window, startup_deadline)) {
    for (auto &release : release_worker) {
      release.store(true, std::memory_order_release);
    }
    PyErr_SetString(PyExc_RuntimeError,
                    "output steal probe runnable blockers did not start");
    return nullptr;
  }

  // Put one output at the front of every high victim queue except the helper.
  // Then put broad work behind it. Small pools scan the broad tail first;
  // larger pools prefer a front dedicated output. Stealing must never skip a
  // compatible broad front to reach an output hidden later in another queue.
  for (std::size_t ordinal = 0; ordinal < output_count; ++ordinal) {
    status = arena->Submit(
        [&](std::size_t, sanitize::internal::StopToken stop) {
          if (stop.stop_requested()) {
            return;
          }
          if (broad_started.load(std::memory_order_acquire) == 0) {
            outputs_before_broad.fetch_add(1, std::memory_order_relaxed);
          }
          outputs_finished.fetch_add(1, std::memory_order_release);
        },
        output_plan, ordinal, TaskTelemetryKind::kOutput);
    if (!status.ok()) {
      for (auto &release : release_worker) {
        release.store(true, std::memory_order_release);
      }
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  for (std::size_t ordinal = 0; ordinal < broad_count; ++ordinal) {
    status = arena->Submit(
        [&](std::size_t, sanitize::internal::StopToken stop) {
          if (stop.stop_requested()) {
            return;
          }
          broad_started.fetch_add(1, std::memory_order_release);
          while (!release_broad.load(std::memory_order_acquire) &&
                 !stop.stop_requested()) {
            std::this_thread::yield();
          }
          broad_finished.fetch_add(1, std::memory_order_release);
        },
        all_plan, ordinal);
    if (!status.ok()) {
      for (auto &release : release_worker) {
        release.store(true, std::memory_order_release);
      }
      release_broad.store(true, std::memory_order_release);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }

  // Free only the helper. It gives back and immediately reacquires one CPU
  // lease, then has no local work and must steal from the deterministic victim
  // queues above. The remaining blockers keep the runnable window saturated.
  release_worker[helper_index].store(true, std::memory_order_release);
  const auto observation_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (outputs_finished.load(std::memory_order_acquire) < output_count &&
         broad_started.load(std::memory_order_acquire) == 0 &&
         std::chrono::steady_clock::now() < observation_deadline) {
    std::this_thread::yield();
  }
  if (outputs_finished.load(std::memory_order_acquire) < output_count &&
      broad_started.load(std::memory_order_acquire) == 0) {
    for (auto &release : release_worker) {
      release.store(true, std::memory_order_release);
    }
    release_broad.store(true, std::memory_order_release);
    PyErr_SetString(PyExc_RuntimeError,
                    "output steal probe did not observe helper progress");
    return nullptr;
  }

  release_broad.store(true, std::memory_order_release);
  for (auto &release : release_worker) {
    release.store(true, std::memory_order_release);
  }
  const auto drain_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const auto outputs_drained =
      wait_until(outputs_finished, output_count, drain_deadline);
  const auto broad_drained =
      wait_until(broad_finished, broad_count, drain_deadline);
  if (!outputs_drained || !broad_drained) {
    PyErr_SetString(PyExc_RuntimeError,
                    "output steal probe did not drain all tasks");
    return nullptr;
  }

  PyObject *result = PyTuple_New(8);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(result, 0,
                            PyLong_FromSize_t(outputs_before_broad.load(
                                std::memory_order_acquire))) ||
      !tuple_set_item_steal(result, 1,
                            PyLong_FromSize_t(outputs_finished.load(
                                std::memory_order_acquire))) ||
      !tuple_set_item_steal(
          result, 2,
          PyLong_FromSize_t(broad_finished.load(std::memory_order_acquire))) ||
      !tuple_set_item_steal(result, 3,
                            PyLong_FromSize_t(arena->stolen_tasks())) ||
      !tuple_set_item_steal(result, 4,
                            PyLong_FromSize_t(arena->started_workers())) ||
      !tuple_set_item_steal(result, 5,
                            PyLong_FromSize_t(arena->queued_tasks())) ||
      !tuple_set_item_steal(result, 6,
                            PyLong_FromSize_t(arena->submitted_tasks())) ||
      !tuple_set_item_steal(result, 7, PyLong_FromSize_t(cpu_window))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

} // namespace core_abi3_internal

// ---- arena_wake_probe ----
namespace core_abi3_internal {
namespace {

using sanitize::internal::OperationTaskArena;
using sanitize::internal::TaskArenaLane;

/// Waits until an atomic counter reaches its target or the deadline expires.
[[nodiscard]] bool
wait_for_count(const std::atomic<std::size_t> &value, std::size_t target,
               std::chrono::steady_clock::time_point deadline) noexcept {
  while (value.load(std::memory_order_acquire) < target &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return value.load(std::memory_order_acquire) >= target;
}

/// Waits until the task arena has no queued or active work, or the deadline
/// expires.
[[nodiscard]] bool
wait_for_arena_idle(const std::shared_ptr<OperationTaskArena> &arena,
                    std::chrono::steady_clock::time_point deadline) noexcept {
  while ((arena->queued_tasks() != 0U || arena->active_tasks() != 0U) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return arena->queued_tasks() == 0U && arena->active_tasks() == 0U;
}

} // namespace

/// Measures wake coalescing while bursts of arena tasks become runnable.
PyObject *py_operation_task_arena_wake_coalescing_probe(PyObject *,
                                                        PyObject *args) {
  int requested_workers = 4;
  int rounds = 20'000;
  int waves = 64;
  if (!PyArg_ParseTuple(args, "i|ii:operation_task_arena_wake_coalescing_probe",
                        &requested_workers, &rounds, &waves)) {
    return nullptr;
  }
  if (requested_workers < 2 || requested_workers > 32 || rounds < 1 ||
      rounds > 200'000 || waves < 1 || waves > 1'000) {
    PyErr_SetString(PyExc_ValueError,
                    "workers must be within [2, 32], rounds within "
                    "[1, 200000], and waves within [1, 1000]");
    return nullptr;
  }

  const auto workers = static_cast<std::size_t>(requested_workers);
  auto arena_result = OperationTaskArena::Make(workers);
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  const auto runnable_blockers = std::min<std::size_t>(
      workers, static_cast<std::size_t>(
                   sanitize::internal::process_cpu_governor().capacity()));
  const auto preload_plan =
      arena->PrepareSubmissionPlan(runnable_blockers, TaskArenaLane::kAll);
  std::atomic<std::size_t> blockers_started{0};
  std::atomic<std::size_t> blockers_finished{0};
  std::atomic<std::size_t> work_finished{0};
  std::atomic<bool> release_blockers{false};

  const auto blocker = [&](std::size_t, sanitize::internal::StopToken stop) {
    blockers_started.fetch_add(1, std::memory_order_release);
    while (!release_blockers.load(std::memory_order_acquire) &&
           !stop.stop_requested()) {
      std::this_thread::yield();
    }
    blockers_finished.fetch_add(1, std::memory_order_release);
  };
  for (std::size_t index = 0; index < runnable_blockers; ++index) {
    const auto status = arena->Submit(blocker, preload_plan);
    if (!status.ok()) {
      release_blockers.store(true, std::memory_order_release);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
    // Waiting after each submission closes the dequeue-before-running window
    // and guarantees that the next blocker admits another physical worker.
    if (!wait_for_count(blockers_started, index + 1U,
                        std::chrono::steady_clock::now() +
                            std::chrono::seconds(5))) {
      release_blockers.store(true, std::memory_order_release);
      PyErr_SetString(PyExc_RuntimeError, "wake probe blocker did not start");
      return nullptr;
    }
  }

  const auto wake_before_preload = arena->wake_epoch_publishes();
  const auto started_at = std::chrono::steady_clock::now();
  const auto work = [&](std::size_t, sanitize::internal::StopToken stop) {
    if (!stop.stop_requested()) {
      work_finished.fetch_add(1, std::memory_order_release);
    }
  };
  // Keep the deliberately blocked preload inside the production queue bound.
  // The probe predates bounded admission and must not require an unbounded
  // queue merely to observe wake coalescing.
  const auto preload_batch = std::max<std::size_t>(
      1U, std::min<std::size_t>(static_cast<std::size_t>(rounds),
                                arena->queue_capacity() / 2U));
  for (std::size_t index = 0; index < preload_batch; ++index) {
    const auto status = arena->Submit(work, preload_plan);
    if (!status.ok()) {
      release_blockers.store(true, std::memory_order_release);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  const auto wake_after_preload = arena->wake_epoch_publishes();
  release_blockers.store(true, std::memory_order_release);

  const auto preload_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(20);
  if (!wait_for_count(work_finished, preload_batch, preload_deadline) ||
      !wait_for_count(blockers_finished, runnable_blockers, preload_deadline) ||
      !wait_for_arena_idle(arena, preload_deadline)) {
    PyErr_SetString(PyExc_RuntimeError, "wake probe preload did not drain");
    return nullptr;
  }

  // Feed the rest in bounded batches after workers are available. This keeps
  // the original workload and telemetry assertions without bypassing the
  // queue's global memory/admission contract.
  const auto preload_target = static_cast<std::size_t>(rounds);
  for (std::size_t submitted = preload_batch; submitted < preload_target;) {
    const auto batch = std::min(preload_batch, preload_target - submitted);
    for (std::size_t index = 0; index < batch; ++index) {
      const auto status = arena->Submit(work, workers, TaskArenaLane::kAll);
      if (!status.ok()) {
        PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
        return nullptr;
      }
    }
    submitted += batch;
    if (!wait_for_count(work_finished, submitted, preload_deadline) ||
        !wait_for_arena_idle(arena, preload_deadline)) {
      PyErr_SetString(PyExc_RuntimeError,
                      "wake probe bounded preload did not drain");
      return nullptr;
    }
  }

  const auto tasks_per_wave = workers * 2U;
  for (int wave = 0; wave < waves; ++wave) {
    const auto base =
        preload_target + static_cast<std::size_t>(wave) * tasks_per_wave;
    for (std::size_t task = 0; task < tasks_per_wave; ++task) {
      auto lane = TaskArenaLane::kAll;
      auto width = workers;
      if (workers >= 4U && task % 3U != 2U) {
        width = workers / 2U;
        lane =
            task % 3U == 0U ? TaskArenaLane::kUpstream : TaskArenaLane::kOutput;
      }
      const auto status = arena->Submit(work, width, lane);
      if (!status.ok()) {
        PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
        return nullptr;
      }
    }
    const auto wave_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    if (!wait_for_count(work_finished, base + tasks_per_wave, wave_deadline) ||
        !wait_for_arena_idle(arena, wave_deadline)) {
      PyErr_SetString(PyExc_RuntimeError,
                      "wake probe park/wake wave did not drain");
      return nullptr;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - started_at);
  const auto wake_final = arena->wake_epoch_publishes();
  PyObject *result = PyTuple_New(9);
  if (!result) {
    return nullptr;
  }
  const auto finished = work_finished.load(std::memory_order_acquire);
  if (!tuple_set_item_steal(result, 0,
                            PyLong_FromLongLong(elapsed_ns.count())) ||
      !tuple_set_item_steal(result, 1,
                            PyLong_FromSize_t(arena->submitted_tasks())) ||
      !tuple_set_item_steal(result, 2, PyLong_FromSize_t(finished)) ||
      !tuple_set_item_steal(result, 3,
                            PyLong_FromSize_t(arena->queued_tasks())) ||
      !tuple_set_item_steal(result, 4,
                            PyLong_FromUnsignedLongLong(wake_before_preload)) ||
      !tuple_set_item_steal(result, 5,
                            PyLong_FromUnsignedLongLong(wake_after_preload)) ||
      !tuple_set_item_steal(result, 6,
                            PyLong_FromUnsignedLongLong(wake_final)) ||
      !tuple_set_item_steal(result, 7,
                            PyLong_FromSize_t(arena->started_workers())) ||
      !tuple_set_item_steal(result, 8,
                            PyLong_FromSize_t(arena->peak_active_tasks()))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

} // namespace core_abi3_internal

// ---- cpu_governor_probe ----
namespace core_abi3_internal {

/// Exercises process CPU permits across competing tasks and reports
/// concurrency.
PyObject *py_process_cpu_governor_probe(PyObject *, PyObject *args) {
  int requested_tasks = 0;
  if (!PyArg_ParseTuple(args, "i:process_cpu_governor_probe",
                        &requested_tasks)) {
    return nullptr;
  }
  if (requested_tasks < 2 || requested_tasks > 256) {
    PyErr_SetString(PyExc_ValueError,
                    "requested_tasks must be between 2 and 256");
    return nullptr;
  }

  auto &governor = sanitize::internal::process_cpu_governor();
  // Each probe registration represents a genuinely wide arena. Passing a
  // boolean here used to collapse the width to 1 and silently bypass the
  // governor after Registration gained width-aware single-arena semantics.
  const auto arena_width = static_cast<std::size_t>(requested_tasks);
  auto first = governor.MakeRegistration(arena_width);
  auto second = governor.MakeRegistration(arena_width);
  std::atomic<std::int64_t> active{0};
  std::atomic<std::int64_t> peak{0};
  std::atomic<std::int64_t> waits{0};
  std::atomic<std::int64_t> completed{0};
  std::vector<sanitize::internal::JThread> threads;
  try {
    threads.reserve(static_cast<std::size_t>(requested_tasks));
    for (int index = 0; index < requested_tasks; ++index) {
      auto *registration = (index & 1) == 0 ? &first : &second;
      threads.emplace_back([&,
                            registration](sanitize::internal::StopToken stop) {
        auto lease = registration->Acquire(stop);
        if (lease.waited()) {
          waits.fetch_add(1, std::memory_order_relaxed);
        }
        const auto now = active.fetch_add(1, std::memory_order_relaxed) + 1;
        auto observed = peak.load(std::memory_order_relaxed);
        while (now > observed && !peak.compare_exchange_weak(
                                     observed, now, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        active.fetch_sub(1, std::memory_order_relaxed);
        completed.fetch_add(1, std::memory_order_relaxed);
      });
    }
  } catch (const std::exception &error) {
    PyErr_SetString(PyExc_RuntimeError, error.what());
    return nullptr;
  }
  for (auto &thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  PyObject *out = PyTuple_New(4);
  if (!out) {
    return nullptr;
  }
  if (!tuple_set_item_steal(out, 0, PyLong_FromLongLong(governor.capacity())) ||
      !tuple_set_item_steal(
          out, 1, PyLong_FromLongLong(peak.load(std::memory_order_relaxed))) ||
      !tuple_set_item_steal(
          out, 2, PyLong_FromLongLong(waits.load(std::memory_order_relaxed))) ||
      !tuple_set_item_steal(
          out, 3,
          PyLong_FromLongLong(completed.load(std::memory_order_relaxed)))) {
    Py_DECREF(out);
    return nullptr;
  }
  return out;
}

} // namespace core_abi3_internal

// ---- ordered_executor_cancellation_probe ----
namespace core_abi3_internal {
namespace {

using CancellationProbeExecutor =
    sanitize::internal::OrderedExecutor<std::uint64_t, std::uint64_t>;

} // namespace

/// Verifies that cancelling one arena-backed stage stops its active packets.
/// Cancellation leaves the operation arena and unrelated work running.
PyObject *py_operation_task_arena_cancellation_probe(PyObject *, PyObject *) {
  auto arena_result = sanitize::internal::OperationTaskArena::Make(4);
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  std::atomic<std::size_t> active{0};
  std::atomic<std::size_t> observed_stop{0};
  auto executor_result = CancellationProbeExecutor::Make(
      4, 8, 8,
      [&active, &observed_stop](std::uint64_t &&value, std::size_t,
                                sanitize::internal::StopToken stop)
          -> sanitize::Result<std::uint64_t> {
        active.fetch_add(1, std::memory_order_release);
        while (!stop.stop_requested()) {
          std::this_thread::yield();
        }
        observed_stop.fetch_add(1, std::memory_order_relaxed);
        active.fetch_sub(1, std::memory_order_release);
        return value;
      },
      arena, sanitize::internal::TaskArenaLane::kAll);
  if (!executor_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    executor_result.status().ToString().c_str());
    return nullptr;
  }
  auto executor = std::move(executor_result).ValueOrDie();
  for (std::uint64_t ordinal = 0; ordinal < 4U; ++ordinal) {
    const auto status = executor->Submit({ordinal, ordinal});
    if (!status.ok()) {
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  const auto start_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  const auto expected_active = std::min<std::size_t>(
      4U, static_cast<std::size_t>(
              sanitize::internal::process_cpu_governor().capacity()));
  while (active.load(std::memory_order_acquire) < expected_active &&
         std::chrono::steady_clock::now() < start_deadline) {
    std::this_thread::yield();
  }
  if (active.load(std::memory_order_acquire) < expected_active) {
    PyErr_SetString(PyExc_RuntimeError,
                    "arena cancellation probe workers did not start");
    return nullptr;
  }

  const auto drained = executor->Shutdown();
  executor.reset();
  const auto active_after = active.load(std::memory_order_acquire);
  const auto observed_stop_after =
      observed_stop.load(std::memory_order_acquire);
  const auto queued_after = arena->queued_tasks();
  if (active_after != 0U || observed_stop_after != expected_active ||
      queued_after != 0U) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "arena cancellation probe did not stop its exact active CPU window");
    return nullptr;
  }

  PyObject *result = PyTuple_New(4);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(result, 0,
                            PyBool_FromLong(static_cast<long>(drained))) ||
      !tuple_set_item_steal(result, 1, PyLong_FromSize_t(active_after)) ||
      !tuple_set_item_steal(result, 2,
                            PyLong_FromSize_t(observed_stop_after)) ||
      !tuple_set_item_steal(result, 3, PyLong_FromSize_t(queued_after))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

} // namespace core_abi3_internal

// ---- ordered_executor_completion_probe ----
namespace core_abi3_internal {
namespace {

using CompletionProbeExecutor =
    sanitize::internal::OrderedExecutor<std::uint64_t, std::uint64_t>;

/// Runs the completion-ring probe workload used to verify ordered publication.
sanitize::Result<std::uint64_t>
completion_probe_work(std::uint64_t value, std::size_t iterations,
                      sanitize::internal::StopToken stop) {
  auto result = value ^ UINT64_C(0x9e3779b97f4a7c15);
  for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
    result ^= result << 7U;
    result ^= result >> 9U;
    result *= UINT64_C(0xbf58476d1ce4e5b9);
  }
  if (stop.stop_requested()) {
    return sanitize::Status::Cancelled("completion probe cancelled");
  }
  return result;
}

/// Consumes completion and validates the runtime probe result.
bool consume_completion(CompletionProbeExecutor *executor,
                        std::uint64_t expected_ordinal,
                        std::uint64_t *checksum) {
  auto next = executor->TakeNext();
  if (!next.ok()) {
    PyErr_SetString(PyExc_RuntimeError, next.status().ToString().c_str());
    return false;
  }
  auto outcome = std::move(next).ValueOrDie();
  if (outcome.ordinal != expected_ordinal) {
    PyErr_SetString(PyExc_RuntimeError,
                    "completion probe observed out-of-order publication");
    return false;
  }
  if (!outcome.result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    outcome.result.status().ToString().c_str());
    return false;
  }
  *checksum ^= std::move(outcome.result).ValueOrDie() + outcome.ordinal;
  return true;
}

} // namespace

/// Returns elapsed time and invariants for a high-volume arena completion pass.
PyObject *py_ordered_executor_arena_completion_probe(PyObject *,
                                                     PyObject *args) {
  int requested_workers = 1;
  int task_count = 1;
  int work_iterations = 0;
  if (!PyArg_ParseTuple(args, "iii:ordered_executor_arena_completion_probe",
                        &requested_workers, &task_count, &work_iterations)) {
    return nullptr;
  }
  if (requested_workers < 1 || requested_workers > 32 || task_count < 1 ||
      task_count > 500000 || work_iterations < 0 || work_iterations > 10000) {
    PyErr_SetString(PyExc_ValueError,
                    "invalid completion probe worker, task, or work count");
    return nullptr;
  }

  const auto workers = static_cast<std::size_t>(requested_workers);
  const auto capacity = std::max<std::size_t>(32U, workers * 4U);
  auto arena_result = sanitize::internal::OperationTaskArena::Make(workers);
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  auto executor_result = CompletionProbeExecutor::Make(
      workers, capacity, capacity,
      [iterations = static_cast<std::size_t>(work_iterations)](
          std::uint64_t &&value, std::size_t,
          sanitize::internal::StopToken stop) {
        return completion_probe_work(value, iterations, stop);
      },
      arena, sanitize::internal::TaskArenaLane::kAll);
  if (!executor_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    executor_result.status().ToString().c_str());
    return nullptr;
  }
  auto executor = std::move(executor_result).ValueOrDie();

  std::uint64_t checksum = 0;
  std::uint64_t next_expected = 0;
  const auto started_at = std::chrono::steady_clock::now();
  for (std::uint64_t ordinal = 0;
       ordinal < static_cast<std::uint64_t>(task_count); ++ordinal) {
    while (executor->in_flight() >= executor->dispatch_window()) {
      if (!consume_completion(executor.get(), next_expected++, &checksum)) {
        return nullptr;
      }
    }
    const auto status =
        executor->Submit({.ordinal = ordinal, .payload = ordinal});
    if (!status.ok()) {
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  const auto finish = executor->FinishSubmission();
  if (!finish.ok()) {
    PyErr_SetString(PyExc_RuntimeError, finish.ToString().c_str());
    return nullptr;
  }
  while (executor->in_flight() > 0U) {
    if (!consume_completion(executor.get(), next_expected++, &checksum)) {
      return nullptr;
    }
  }
  const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now() - started_at)
                              .count();

  PyObject *result = PyTuple_New(7);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(
          result, 0, PyLong_FromLongLong(static_cast<long long>(elapsed_us))) ||
      !tuple_set_item_steal(result, 1, PyLong_FromSize_t(next_expected)) ||
      !tuple_set_item_steal(result, 2,
                            PyLong_FromUnsignedLongLong(
                                static_cast<unsigned long long>(checksum))) ||
      !tuple_set_item_steal(result, 3,
                            PyLong_FromSize_t(arena->started_workers())) ||
      !tuple_set_item_steal(result, 4,
                            PyLong_FromSize_t(arena->peak_active_tasks())) ||
      !tuple_set_item_steal(result, 5,
                            PyLong_FromSize_t(arena->queued_tasks())) ||
      !tuple_set_item_steal(result, 6,
                            PyLong_FromSize_t(arena->submitted_tasks()))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

} // namespace core_abi3_internal

// ---- output_admission_probe ----
namespace core_abi3_internal {

/// Reports worker admission under full and constrained output-lane capacity.
PyObject *py_output_worker_admission_probe(PyObject *, PyObject *args) {
  int full_admission = 0;
  if (!PyArg_ParseTuple(args, "p:output_worker_admission_probe",
                        &full_admission)) {
    return nullptr;
  }

  constexpr std::int64_t kProbeMemoryBytes = 256LL * 1024LL * 1024LL;
  constexpr auto operation_policy = sanitize::internal::execution_policy_from(
      sanitize::ThreadingMode::kMulti, kProbeMemoryBytes, 16);
  constexpr auto output_policy =
      sanitize::internal::execution_policy_with_worker_ceiling(operation_policy,
                                                               8, 1);
  static_assert(operation_policy.effective_workers == 16);
  static_assert(output_policy.effective_workers == 8);

  sanitize::internal::ordered_text_output::OutputAdmissionState state;
  const auto first =
      sanitize::internal::ordered_text_output::select_output_admission(
          output_policy, 3, 1, true, full_admission != 0, &state);
  const auto second =
      sanitize::internal::ordered_text_output::select_output_admission(
          output_policy, 3, 1, true, full_admission != 0, &state);
  const auto generations =
      first.effective_workers == second.effective_workers ? 1 : 2;

  PyObject *result = PyTuple_New(7);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(result, 0,
                            PyLong_FromLongLong(first.effective_workers)) ||
      !tuple_set_item_steal(result, 1,
                            PyLong_FromLongLong(second.effective_workers)) ||
      !tuple_set_item_steal(result, 2, PyLong_FromLong(generations)) ||
      !tuple_set_item_steal(
          result, 3,
          PyBool_FromLong(
              sanitize::internal::ordered_text_output::
                      output_admission_requires_sampling(full_admission != 0)
                  ? 1
                  : 0)) ||
      !tuple_set_item_steal(
          result, 4, PyLong_FromLongLong(output_policy.task_queue_capacity)) ||
      !tuple_set_item_steal(
          result, 5, PyLong_FromLongLong(output_policy.reorder_capacity)) ||
      !tuple_set_item_steal(
          result, 6, PyLong_FromLongLong(state.accumulated_work_items))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

} // namespace core_abi3_internal

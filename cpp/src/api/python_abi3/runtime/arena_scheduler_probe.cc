// Exercises mixed-lane stealing on a high-core operation arena.
#include "internal/abi/python_abi3/methods.hh"

#include "internal/runtime/operation_task_arena.hh"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>
#include <utility>
#include <vector>

namespace core_abi3_internal {
namespace {

using sanitize::internal::OperationTaskArena;
using sanitize::internal::TaskArenaLane;
using sanitize::internal::TaskTelemetryKind;

void release_gate(std::atomic<bool> *gate) noexcept {
  gate->store(true, std::memory_order_release);
  gate->notify_all();
}

bool wait_gate_or_stop(std::atomic<bool> *gate,
                       sanitize::internal::StopToken stop) {
  auto notify_gate = [gate] { gate->notify_all(); };
  sanitize::internal::StopCallback<decltype(notify_gate)> stop_gate(
      stop, std::move(notify_gate));
  while (!gate->load(std::memory_order_acquire) && !stop.stop_requested()) {
    gate->wait(false, std::memory_order_acquire);
  }
  return !stop.stop_requested();
}

[[nodiscard]] bool wait_until(const std::atomic<std::size_t> &value,
                              std::size_t target,
                              std::chrono::steady_clock::time_point deadline) {
  while (value.load(std::memory_order_acquire) < target &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return value.load(std::memory_order_acquire) >= target;
}

} // namespace

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
          for (std::size_t task_index = 0; task_index < per_producer;
               ++task_index) {
            auto status = arena->Submit(
                [&tasks_finished](std::size_t,
                                  sanitize::internal::StopToken task_stop) {
                  if (!task_stop.stop_requested()) {
                    tasks_finished.fetch_add(1, std::memory_order_release);
                  }
                },
                plan, TaskTelemetryKind::kOther);
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
  for (std::size_t index = 0; index < blockers_per_lane; ++index) {
    auto status = arena->Submit(blocker, half, TaskArenaLane::kUpstream);
    if (status.ok()) {
      status = arena->Submit(blocker, half, TaskArenaLane::kOutput);
    }
    if (!status.ok()) {
      release_gate(&release_blockers);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }

  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  if (!wait_until(blockers_started, blocker_count, startup_deadline)) {
    release_gate(&release_blockers);
    PyErr_SetString(PyExc_RuntimeError,
                    "mixed-lane probe blockers did not start");
    return nullptr;
  }

  const auto work = [&](std::size_t, sanitize::internal::StopToken stop) {
    if (!stop.stop_requested()) {
      work_finished.fetch_add(1, std::memory_order_release);
    }
  };
  const auto started_at = std::chrono::steady_clock::now();
  for (int round = 0; round < rounds; ++round) {
    auto status = arena->Submit(work, half, TaskArenaLane::kUpstream);
    if (status.ok()) {
      status = arena->Submit(work, half, TaskArenaLane::kOutput);
    }
    if (status.ok()) {
      status = arena->Submit(work, workers, TaskArenaLane::kAll);
    }
    if (!status.ok()) {
      release_gate(&release_blockers);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }

  const auto work_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(15);
  const auto drained = wait_until(work_finished, work_count, work_deadline);
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - started_at);
  release_gate(&release_blockers);
  const auto blocker_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const auto blockers_drained =
      wait_until(blockers_finished, blocker_count, blocker_deadline);
  if (!drained || !blockers_drained) {
    PyErr_SetString(PyExc_RuntimeError,
                    "mixed-lane probe did not drain all tasks");
    return nullptr;
  }

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
  std::array<std::atomic<bool>, 32> broad_done{};
  std::atomic<std::size_t> blockers_started{0};
  std::atomic<std::size_t> high_outputs_finished{0};
  std::atomic<std::size_t> broad_finished{0};
  std::atomic<std::size_t> outputs_before_broad{0};
  std::atomic<bool> release_high{false};
  std::atomic<bool> release_low{false};

  for (std::size_t ordinal = 0; ordinal < workers; ++ordinal) {
    auto status = arena->Submit(
        [&](std::size_t worker_index, sanitize::internal::StopToken stop) {
          blockers_started.fetch_add(1, std::memory_order_release);
          auto *release =
              worker_index >= high_begin ? &release_high : &release_low;
          (void)wait_gate_or_stop(release, stop);
        },
        workers, TaskArenaLane::kAll);
    if (!status.ok()) {
      release_gate(&release_high);
      release_gate(&release_low);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  if (!wait_until(blockers_started, workers, startup_deadline)) {
    release_gate(&release_high);
    release_gate(&release_low);
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
          broad_done[worker_index].store(true, std::memory_order_release);
          broad_finished.fetch_add(1, std::memory_order_release);
        },
        workers, TaskArenaLane::kAll);
    if (!status.ok()) {
      release_gate(&release_high);
      release_gate(&release_low);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  for (int wave = 0; wave < output_waves; ++wave) {
    for (std::size_t ordinal = 0; ordinal < workers / 2U; ++ordinal) {
      auto status = arena->Submit(
          [&](std::size_t relative_worker, sanitize::internal::StopToken stop) {
            if (stop.stop_requested()) {
              return;
            }
            const auto physical = high_begin + relative_worker;
            if (!broad_done[physical].load(std::memory_order_acquire)) {
              outputs_before_broad.fetch_add(1, std::memory_order_release);
            }
            high_outputs_finished.fetch_add(1, std::memory_order_release);
          },
          workers / 2U, TaskArenaLane::kOutput, TaskTelemetryKind::kOutput);
      if (!status.ok()) {
        release_gate(&release_high);
        release_gate(&release_low);
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
  release_gate(&release_low);
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
  if (!tuple_set_item_steal(result, 0,
                            PyLong_FromSize_t(outputs_before_broad.load(
                                std::memory_order_acquire))) ||
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
  auto arena_result = OperationTaskArena::Make(workers);
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();

  std::array<std::atomic<bool>, 32> release_worker{};
  std::atomic<std::size_t> blockers_started{0};
  std::atomic<std::size_t> outputs_finished{0};
  std::atomic<std::size_t> outputs_before_broad{0};
  std::atomic<std::size_t> broad_started{0};
  std::atomic<std::size_t> broad_finished{0};
  std::atomic<bool> release_broad{false};

  for (std::size_t ordinal = 0; ordinal < workers; ++ordinal) {
    auto status = arena->Submit(
        [&](std::size_t worker_index, sanitize::internal::StopToken stop) {
          blockers_started.fetch_add(1, std::memory_order_release);
          while (
              !release_worker[worker_index].load(std::memory_order_acquire) &&
              !stop.stop_requested()) {
            std::this_thread::yield();
          }
        },
        workers, TaskArenaLane::kAll);
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
  if (!wait_until(blockers_started, workers, startup_deadline)) {
    for (auto &release : release_worker) {
      release.store(true, std::memory_order_release);
    }
    PyErr_SetString(PyExc_RuntimeError,
                    "output steal probe blockers did not start");
    return nullptr;
  }

  for (std::size_t ordinal = 0; ordinal < output_count; ++ordinal) {
    auto status = arena->Submit(
        [&](std::size_t, sanitize::internal::StopToken stop) {
          if (stop.stop_requested()) {
            return;
          }
          if (broad_started.load(std::memory_order_acquire) == 0) {
            outputs_before_broad.fetch_add(1, std::memory_order_relaxed);
          }
          outputs_finished.fetch_add(1, std::memory_order_release);
        },
        half, TaskArenaLane::kOutput, TaskTelemetryKind::kOutput);
    if (!status.ok()) {
      for (auto &release : release_worker) {
        release.store(true, std::memory_order_release);
      }
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  for (std::size_t ordinal = 0; ordinal < broad_count; ++ordinal) {
    auto status = arena->Submit(
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
        workers, TaskArenaLane::kAll);
    if (!status.ok()) {
      for (auto &release : release_worker) {
        release.store(true, std::memory_order_release);
      }
      release_broad.store(true, std::memory_order_release);
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }

  release_worker[workers - 1U].store(true, std::memory_order_release);
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

  PyObject *result = PyTuple_New(7);
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
                            PyLong_FromSize_t(arena->submitted_tasks()))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

} // namespace core_abi3_internal

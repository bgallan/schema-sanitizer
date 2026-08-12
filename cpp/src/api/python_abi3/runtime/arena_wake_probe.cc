// Exercises targeted worker wake epochs and running-worker signal coalescing.
#include "internal/abi/python_abi3/methods.hh"

#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/process_cpu_governor.hh"

#include "internal/runtime/thread_compat.hh"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>

namespace core_abi3_internal {
namespace {

using sanitize::internal::OperationTaskArena;
using sanitize::internal::TaskArenaLane;

[[nodiscard]] bool
wait_for_count(const std::atomic<std::size_t> &value, std::size_t target,
               std::chrono::steady_clock::time_point deadline) noexcept {
  while (value.load(std::memory_order_acquire) < target &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  return value.load(std::memory_order_acquire) >= target;
}

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

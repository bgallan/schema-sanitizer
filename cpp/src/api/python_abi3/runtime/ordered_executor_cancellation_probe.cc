// Verifies prompt stage-local cancellation on the shared native task arena.

#include "internal/abi/python_abi3/methods.hh"

#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor.hh"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>

namespace core_abi3_internal {
namespace {

using CancellationProbeExecutor =
    sanitize::internal::OrderedExecutor<std::uint64_t, std::uint64_t>;

} // namespace

// Verifies that cancelling one arena-backed stage stops its active packets
// without shutting down the operation-wide arena or waiting for unrelated work.
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
  while (active.load(std::memory_order_acquire) < 4U &&
         std::chrono::steady_clock::now() < start_deadline) {
    std::this_thread::yield();
  }
  if (active.load(std::memory_order_acquire) == 0U) {
    PyErr_SetString(PyExc_RuntimeError,
                    "arena cancellation probe workers did not start");
    return nullptr;
  }

  const auto started = std::chrono::steady_clock::now();
  executor->Cancel();
  executor.reset();
  const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now() - started)
                              .count();

  PyObject *result = PyTuple_New(4);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(
          result, 0, PyLong_FromLongLong(static_cast<long long>(elapsed_us))) ||
      !tuple_set_item_steal(
          result, 1,
          PyLong_FromSize_t(active.load(std::memory_order_acquire))) ||
      !tuple_set_item_steal(
          result, 2,
          PyLong_FromSize_t(observed_stop.load(std::memory_order_acquire))) ||
      !tuple_set_item_steal(result, 3,
                            PyLong_FromSize_t(arena->queued_tasks()))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

} // namespace core_abi3_internal

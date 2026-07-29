// Measures bounded arena-backed completion publication without Python work.

#include "internal/abi/python_abi3/methods.hh"

#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor.hh"

#include "internal/runtime/thread_compat.hh"
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace core_abi3_internal {
namespace {

using CompletionProbeExecutor =
    sanitize::internal::OrderedExecutor<std::uint64_t, std::uint64_t>;

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

// Returns elapsed time and invariants for a high-volume arena completion pass.
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

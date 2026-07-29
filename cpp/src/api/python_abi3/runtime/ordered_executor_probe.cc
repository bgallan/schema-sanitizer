// Exercises the native ordinal executor for internal differential tests.

#include "internal/abi/python_abi3/methods.hh"

#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor.hh"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace core_abi3_internal {
namespace {

using ProbeExecutor =
    sanitize::internal::OrderedExecutor<std::uint64_t, std::uint64_t>;

struct ProbeState {
  std::mutex mutex;
  std::set<std::thread::id> worker_threads;
};

// Consumes one coordinator-visible outcome and records its ordered result.
bool append_probe_outcome(ProbeExecutor *executor,
                          std::vector<std::uint64_t> *ordinals,
                          std::vector<std::uint64_t> *values,
                          std::int64_t *failure_ordinal,
                          sanitize::Status *failure_status) {
  auto next = executor->TakeNext();
  if (!next.ok()) {
    *failure_status = next.status();
    return false;
  }
  auto outcome = std::move(next).ValueOrDie();
  if (!outcome.result.ok()) {
    *failure_ordinal = static_cast<std::int64_t>(outcome.ordinal);
    *failure_status = outcome.result.status();
    return false;
  }
  ordinals->push_back(outcome.ordinal);
  values->push_back(std::move(outcome.result).ValueOrDie());
  return true;
}

// Packs one unsigned integer sequence into an owned Python tuple.
PyObject *pack_probe_sequence(const std::vector<std::uint64_t> &values) {
  PyObject *out = PyTuple_New(static_cast<Py_ssize_t>(values.size()));
  if (!out) {
    return nullptr;
  }
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (!tuple_set_item_steal(
            out, static_cast<Py_ssize_t>(index),
            PyLong_FromUnsignedLongLong(
                static_cast<unsigned long long>(values[index])))) {
      Py_DECREF(out);
      return nullptr;
    }
  }
  return out;
}

} // namespace

// Runs a bounded native executor probe with forced out-of-order completion.
PyObject *py_ordered_executor_probe(PyObject *, PyObject *args) {
  int mode = 0;
  int requested_workers = 1;
  int task_count = 0;
  int fail_ordinal = -1;
  if (!PyArg_ParseTuple(args, "iiii:ordered_executor_probe", &mode,
                        &requested_workers, &task_count, &fail_ordinal)) {
    return nullptr;
  }
  if (mode < 0 || mode > 1) {
    PyErr_SetString(PyExc_ValueError, "mode must be 0 (single) or 1 (multi)");
    return nullptr;
  }
  if (requested_workers < 1 || requested_workers > 1024) {
    PyErr_SetString(PyExc_ValueError,
                    "diagnostic workers must be within [1, 1024]");
    return nullptr;
  }
  if (task_count < 0 || task_count > 10000) {
    PyErr_SetString(PyExc_ValueError, "task_count must be within [0, 10000]");
    return nullptr;
  }
  if (fail_ordinal < -1 || fail_ordinal >= task_count) {
    PyErr_SetString(PyExc_ValueError,
                    "fail_ordinal must be -1 or a valid task ordinal");
    return nullptr;
  }

  const auto effective_workers =
      mode == 0 ? std::size_t{1} : static_cast<std::size_t>(requested_workers);
  const auto capacity =
      std::max<std::size_t>(1, effective_workers * std::size_t{2});
  auto state = std::make_shared<ProbeState>();
  const auto failure = static_cast<std::int64_t>(fail_ordinal);
  auto executor_result = ProbeExecutor::Make(
      effective_workers, capacity, capacity,
      [state, task_count, failure](std::uint64_t &&value, std::size_t,
                                   sanitize::internal::StopToken stop)
          -> sanitize::Result<std::uint64_t> {
        {
          std::lock_guard lock(state->mutex);
          state->worker_threads.insert(std::this_thread::get_id());
        }
        const auto delay_slot = static_cast<std::uint64_t>(task_count) - value;
        const auto delay = std::chrono::microseconds(
            static_cast<std::chrono::microseconds::rep>((delay_slot % 7U) *
                                                        250U));
        if (delay.count() > 0) {
          std::this_thread::sleep_for(delay);
        }
        if (stop.stop_requested()) {
          return sanitize::Status::Cancelled("probe worker cancelled");
        }
        if (failure >= 0 &&
            (value == static_cast<std::uint64_t>(failure) ||
             value == static_cast<std::uint64_t>(failure + 2))) {
          return sanitize::Status::Invalid("probe failure at ordinal ", value);
        }
        return value * 10U;
      });
  if (!executor_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    executor_result.status().ToString().c_str());
    return nullptr;
  }
  auto executor = std::move(executor_result).ValueOrDie();

  std::vector<std::uint64_t> ordinals;
  std::vector<std::uint64_t> values;
  ordinals.reserve(static_cast<std::size_t>(task_count));
  values.reserve(static_cast<std::size_t>(task_count));
  std::int64_t observed_failure = -1;
  sanitize::Status observed_status = sanitize::Status::OK();
  bool continue_work = true;

  for (int ordinal = 0; ordinal < task_count && continue_work; ++ordinal) {
    while (executor->in_flight() >= executor->dispatch_window()) {
      continue_work = append_probe_outcome(executor.get(), &ordinals, &values,
                                           &observed_failure, &observed_status);
      if (!continue_work) {
        break;
      }
    }
    if (!continue_work) {
      break;
    }
    const auto submit = executor->Submit(
        ProbeExecutor::Packet{.ordinal = static_cast<std::uint64_t>(ordinal),
                              .payload = static_cast<std::uint64_t>(ordinal)});
    if (!submit.ok()) {
      observed_status = submit;
      continue_work = false;
    }
  }

  if (continue_work) {
    const auto finish = executor->FinishSubmission();
    if (!finish.ok()) {
      observed_status = finish;
      continue_work = false;
    }
  }
  while (continue_work && executor->in_flight() > 0) {
    continue_work = append_probe_outcome(executor.get(), &ordinals, &values,
                                         &observed_failure, &observed_status);
  }
  if (!continue_work) {
    executor->Cancel();
  }

  std::size_t thread_count = 0;
  {
    std::lock_guard lock(state->mutex);
    thread_count = state->worker_threads.size();
  }

  PyObject *out = PyTuple_New(7);
  if (!out) {
    return nullptr;
  }
  const auto status_string =
      observed_status.ok() ? std::string{"OK"} : observed_status.ToString();
  if (!tuple_set_item_steal(out, 0, pack_probe_sequence(ordinals)) ||
      !tuple_set_item_steal(out, 1, pack_probe_sequence(values)) ||
      !tuple_set_item_steal(out, 2, PyLong_FromSize_t(thread_count)) ||
      !tuple_set_item_steal(out, 3, PyLong_FromLongLong(observed_failure)) ||
      !tuple_set_item_steal(out, 4,
                            PyBool_FromLong(executor->inline_mode() ? 1 : 0)) ||
      !tuple_set_item_steal(out, 5,
                            PyLong_FromSize_t(executor->worker_count())) ||
      !tuple_set_item_steal(out, 6,
                            PyUnicode_FromString(status_string.c_str()))) {
    Py_DECREF(out);
    return nullptr;
  }
  return out;
}

// Exercises two ordered stages on one operation-wide arena.
PyObject *py_operation_task_arena_probe(PyObject *, PyObject *args) {
  int requested_workers = 1;
  int upstream_workers = 1;
  int output_workers = 1;
  int tasks_per_stage = 1;
  if (!PyArg_ParseTuple(args, "iiii:operation_task_arena_probe",
                        &requested_workers, &upstream_workers, &output_workers,
                        &tasks_per_stage)) {
    return nullptr;
  }
  if (requested_workers < 1 || requested_workers > 1024 ||
      upstream_workers < 1 || upstream_workers > requested_workers ||
      output_workers < 1 || output_workers > requested_workers ||
      tasks_per_stage < 1 || tasks_per_stage > 1000) {
    PyErr_SetString(PyExc_ValueError, "invalid arena worker or task count");
    return nullptr;
  }

  auto arena_result = sanitize::internal::OperationTaskArena::Make(
      static_cast<std::size_t>(requested_workers));
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();

  struct SharedState {
    std::mutex mutex;
    std::set<std::thread::id> upstream;
    std::set<std::thread::id> output;
    std::atomic<std::size_t> active{0};
    std::atomic<std::size_t> peak{0};
    std::atomic<bool> release{false};
  };
  auto state = std::make_shared<SharedState>();
  const auto expected_peak = static_cast<std::size_t>(requested_workers);
  const auto make_worker = [state, expected_peak](bool is_output) {
    return [state, expected_peak, is_output](std::uint64_t &&value, std::size_t,
                                             sanitize::internal::StopToken stop)
               -> sanitize::Result<std::uint64_t> {
      {
        std::lock_guard lock(state->mutex);
        (is_output ? state->output : state->upstream)
            .insert(std::this_thread::get_id());
      }
      const auto active =
          state->active.fetch_add(1, std::memory_order_relaxed) + 1;
      auto observed = state->peak.load(std::memory_order_relaxed);
      while (observed < active &&
             !state->peak.compare_exchange_weak(observed, active,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
      }
      if (active == expected_peak) {
        state->release.store(true, std::memory_order_release);
        state->release.notify_all();
      }
      auto notify_release = [state] { state->release.notify_all(); };
      sanitize::internal::StopCallback<decltype(notify_release)> stop_release(
          stop, std::move(notify_release));
      while (!state->release.load(std::memory_order_acquire) &&
             !stop.stop_requested()) {
        state->release.wait(false, std::memory_order_acquire);
      }
      state->active.fetch_sub(1, std::memory_order_relaxed);
      if (stop.stop_requested()) {
        return sanitize::Status::Cancelled("arena probe cancelled");
      }
      return value;
    };
  };

  const auto capacity = static_cast<std::size_t>(tasks_per_stage);
  auto upstream_result = ProbeExecutor::Make(
      static_cast<std::size_t>(upstream_workers), capacity, capacity,
      make_worker(false), arena, sanitize::internal::TaskArenaLane::kUpstream);
  auto output_result = ProbeExecutor::Make(
      static_cast<std::size_t>(output_workers), capacity, capacity,
      make_worker(true), arena, sanitize::internal::TaskArenaLane::kOutput);
  if (!upstream_result.ok() || !output_result.ok()) {
    const auto status = !upstream_result.ok() ? upstream_result.status()
                                              : output_result.status();
    PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
    return nullptr;
  }
  auto upstream = std::move(upstream_result).ValueOrDie();
  auto output = std::move(output_result).ValueOrDie();
  for (int ordinal = 0; ordinal < tasks_per_stage; ++ordinal) {
    const auto value = static_cast<std::uint64_t>(ordinal);
    auto status = upstream->Submit({value, value});
    if (status.ok()) {
      status = output->Submit({value, value});
    }
    if (!status.ok()) {
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }
  auto status = upstream->FinishSubmission();
  if (status.ok()) {
    status = output->FinishSubmission();
  }
  if (!status.ok()) {
    PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
    return nullptr;
  }
  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (state->peak.load(std::memory_order_acquire) < expected_peak &&
         std::chrono::steady_clock::now() < startup_deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  state->release.store(true, std::memory_order_release);
  state->release.notify_all();
  for (int ordinal = 0; ordinal < tasks_per_stage; ++ordinal) {
    auto left = upstream->TakeNext();
    auto right = output->TakeNext();
    if (!left.ok() || !right.ok() || !left.ValueOrDie().result.ok() ||
        !right.ValueOrDie().result.ok()) {
      PyErr_SetString(PyExc_RuntimeError, "operation arena probe task failed");
      return nullptr;
    }
  }

  std::size_t overlap = 0;
  std::size_t total_threads = 0;
  std::size_t upstream_threads = 0;
  std::size_t output_threads = 0;
  {
    std::lock_guard lock(state->mutex);
    upstream_threads = state->upstream.size();
    output_threads = state->output.size();
    std::set<std::thread::id> combined = state->upstream;
    combined.insert(state->output.begin(), state->output.end());
    total_threads = combined.size();
    for (const auto &thread : state->upstream) {
      overlap += state->output.contains(thread) ? 1U : 0U;
    }
  }

  PyObject *result = PyTuple_New(7);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(result, 0,
                            PyLong_FromSize_t(arena->worker_count())) ||
      !tuple_set_item_steal(result, 1, PyLong_FromSize_t(state->peak.load())) ||
      !tuple_set_item_steal(result, 2, PyLong_FromSize_t(total_threads)) ||
      !tuple_set_item_steal(result, 3, PyLong_FromSize_t(overlap)) ||
      !tuple_set_item_steal(result, 4, PyLong_FromSize_t(upstream_threads)) ||
      !tuple_set_item_steal(result, 5, PyLong_FromSize_t(output_threads)) ||
      !tuple_set_item_steal(result, 6,
                            PyLong_FromSize_t(arena->submitted_tasks()))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

PyObject *py_operation_task_arena_stealing_probe(PyObject *, PyObject *) {
  constexpr std::size_t worker_count = 4U;
  auto arena_result =
      sanitize::internal::OperationTaskArena::Make(worker_count);
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  std::array<std::atomic<bool>, worker_count> release_worker{};
  std::atomic<std::size_t> blockers_started{0};
  std::atomic<bool> displaced_finished{false};
  std::atomic<std::size_t> displaced_worker{99};
  std::atomic<std::size_t> completed{0};

  const auto release_all = [&]() noexcept {
    for (auto &release : release_worker) {
      release.store(true, std::memory_order_release);
    }
  };
  for (std::size_t ordinal = 0; ordinal < worker_count; ++ordinal) {
    auto status = arena->Submit(
        [&](std::size_t worker_index, sanitize::internal::StopToken stop) {
          blockers_started.fetch_add(1, std::memory_order_release);
          while (
              !release_worker[worker_index].load(std::memory_order_acquire) &&
              !stop.stop_requested()) {
            std::this_thread::yield();
          }
          completed.fetch_add(1, std::memory_order_release);
        },
        worker_count, sanitize::internal::TaskArenaLane::kAll);
    if (!status.ok()) {
      release_all();
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (blockers_started.load(std::memory_order_acquire) < worker_count &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  if (blockers_started.load(std::memory_order_acquire) != worker_count) {
    release_all();
    PyErr_SetString(PyExc_RuntimeError,
                    "arena stealing probe blockers did not start");
    return nullptr;
  }

  for (std::size_t ordinal = 0; ordinal < worker_count; ++ordinal) {
    auto status = arena->Submit(
        [&, ordinal](std::size_t worker_index,
                     sanitize::internal::StopToken stop) {
          if (stop.stop_requested()) {
            return;
          }
          if (ordinal == 0U) {
            displaced_worker.store(worker_index, std::memory_order_release);
            displaced_finished.store(true, std::memory_order_release);
          }
          completed.fetch_add(1, std::memory_order_release);
        },
        worker_count, sanitize::internal::TaskArenaLane::kAll);
    if (!status.ok()) {
      release_all();
      PyErr_SetString(PyExc_RuntimeError, status.ToString().c_str());
      return nullptr;
    }
  }

  for (std::size_t worker = 1; worker < worker_count; ++worker) {
    release_worker[worker].store(true, std::memory_order_release);
  }
  while (!displaced_finished.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  release_worker[0].store(true, std::memory_order_release);
  while (completed.load(std::memory_order_acquire) < worker_count * 2U &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  if (!displaced_finished.load(std::memory_order_acquire) ||
      completed.load(std::memory_order_acquire) != worker_count * 2U) {
    release_all();
    PyErr_SetString(PyExc_RuntimeError,
                    "arena stealing probe did not drain all tasks");
    return nullptr;
  }

  PyObject *result = PyTuple_New(5);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(result, 0,
                            PyLong_FromSize_t(arena->stolen_tasks())) ||
      !tuple_set_item_steal(result, 1,
                            PyLong_FromSize_t(displaced_worker.load(
                                std::memory_order_acquire))) ||
      !tuple_set_item_steal(
          result, 2,
          PyLong_FromSize_t(completed.load(std::memory_order_acquire))) ||
      !tuple_set_item_steal(result, 3,
                            PyLong_FromSize_t(arena->queued_tasks())) ||
      !tuple_set_item_steal(result, 4,
                            PyLong_FromSize_t(arena->peak_active_tasks()))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

} // namespace core_abi3_internal

// Exercises fair process-wide CPU admission for internal tests.

#include "internal/abi/python_abi3/methods.hh"

#include "internal/runtime/process_cpu_governor.hh"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <thread>
#include <vector>

namespace core_abi3_internal {

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
  auto first = governor.MakeRegistration(true);
  auto second = governor.MakeRegistration(true);
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

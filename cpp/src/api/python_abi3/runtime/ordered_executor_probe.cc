// Exercises the native ordinal executor for internal differential tests.

#include "internal/abi/python_abi3/methods.hh"

#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor.hh"
#include "internal/runtime/process_cpu_governor.hh"
#include "internal/runtime/process_fd_governor.hh"
#include "internal/runtime/process_identity.hh"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <new>
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

constexpr const char *kExternalRuntimePermitLeaseCapsuleName =
    "schema_sanitizer.external_runtime_thread_permit_lease";
constexpr const char *kFdPermitLeaseCapsuleName =
    "schema_sanitizer.file_descriptor_permit_lease";

using ExternalRuntimePermitLease =
    sanitize::internal::ProcessExternalRuntimeThreadPermitLease;
using FdPermitLease = sanitize::internal::ProcessFdPermitLease;

std::atomic<std::uint64_t> g_next_external_runtime_permit_receipt_id{1U};
std::atomic<std::uint64_t> g_next_fd_permit_receipt_id{1U};

[[nodiscard]] std::uint64_t
allocate_receipt_id(std::atomic<std::uint64_t> &counter) noexcept {
  auto current = counter.load(std::memory_order_relaxed);
  for (;;) {
    if (current == 0U || current == std::numeric_limits<std::uint64_t>::max()) {
      return 0U;
    }
    if (counter.compare_exchange_weak(current, current + 1U,
                                      std::memory_order_relaxed,
                                      std::memory_order_relaxed)) {
      return current;
    }
  }
}

struct ExternalRuntimePermitReceipt final {
  ExternalRuntimePermitLease lease;
  std::uint64_t receipt_id = 0U;
  std::uint64_t generation = 1U;
  sanitize::internal::RuntimeProcessId owner_process = 0U;

  ExternalRuntimePermitReceipt(std::size_t desired,
                               std::size_t minimum) noexcept
      : lease(desired, minimum),
        receipt_id(
            allocate_receipt_id(g_next_external_runtime_permit_receipt_id)),
        owner_process(sanitize::internal::current_runtime_process_id()) {}

  [[nodiscard]] bool owner_process_matches() const noexcept {
    return sanitize::internal::runtime_owner_process() &&
           owner_process == sanitize::internal::current_runtime_process_id();
  }
};

struct FdPermitReceipt final {
  FdPermitLease lease;
  std::uint64_t receipt_id = 0U;
  std::uint64_t generation = 1U;
  sanitize::internal::RuntimeProcessId owner_process = 0U;

  FdPermitReceipt(std::size_t desired, std::size_t minimum,
                  std::uint64_t timeout_millis) noexcept
      : lease(FdPermitLease::TryAcquireUpToWait(desired, minimum,
                                                timeout_millis)),
        receipt_id(allocate_receipt_id(g_next_fd_permit_receipt_id)),
        owner_process(sanitize::internal::current_runtime_process_id()) {}

  [[nodiscard]] bool owner_process_matches() const noexcept {
    return sanitize::internal::runtime_owner_process() &&
           owner_process == sanitize::internal::current_runtime_process_id();
  }
};

void destroy_fd_permit_lease_capsule(PyObject *capsule) {
  auto *receipt = static_cast<FdPermitReceipt *>(
      PyCapsule_GetPointer(capsule, kFdPermitLeaseCapsuleName));
  if (!receipt) {
    PyErr_Clear();
    return;
  }
  delete receipt;
}

bool resolve_fd_permit_lease(PyObject *capsule, FdPermitReceipt **out) {
  if (!out) {
    PyErr_SetString(PyExc_RuntimeError,
                    "file descriptor permit lease output is null");
    return false;
  }
  auto *receipt = static_cast<FdPermitReceipt *>(
      PyCapsule_GetPointer(capsule, kFdPermitLeaseCapsuleName));
  if (!receipt) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_RuntimeError,
                      "file descriptor permit lease is null");
    }
    return false;
  }
  *out = receipt;
  return true;
}

void destroy_external_runtime_permit_lease_capsule(PyObject *capsule) {
  auto *receipt = static_cast<ExternalRuntimePermitReceipt *>(
      PyCapsule_GetPointer(capsule, kExternalRuntimePermitLeaseCapsuleName));
  if (!receipt) {
    PyErr_Clear();
    return;
  }
  delete receipt;
}

bool resolve_external_runtime_permit_lease(PyObject *capsule,
                                           ExternalRuntimePermitReceipt **out) {
  if (!out) {
    PyErr_SetString(PyExc_RuntimeError,
                    "external runtime permit lease output is null");
    return false;
  }
  auto *receipt = static_cast<ExternalRuntimePermitReceipt *>(
      PyCapsule_GetPointer(capsule, kExternalRuntimePermitLeaseCapsuleName));
  if (!receipt) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_RuntimeError,
                      "external runtime permit lease is null");
    }
    return false;
  }
  *out = receipt;
  return true;
}

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
PyObject *py_operation_task_arena_reaper_shutdown(PyObject *, PyObject *args) {
  unsigned long long timeout_millis = 0U;
  if (!PyArg_ParseTuple(args, "K:operation_task_arena_reaper_shutdown",
                        &timeout_millis)) {
    return nullptr;
  }
  const bool stopped =
      sanitize::internal::OperationTaskArena::ShutdownCleanupReaper(
          static_cast<std::uint64_t>(timeout_millis));
  if (stopped) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

PyObject *py_process_physical_thread_count(PyObject *, PyObject *) {
  const auto count = sanitize::internal::process_physical_thread_count();
  if (!count) {
    Py_RETURN_NONE;
  }
  return PyLong_FromSize_t(*count);
}

PyObject *py_process_physical_thread_permits_acquire(PyObject *,
                                                     PyObject *args) {
  unsigned long long desired = 0U;
  unsigned long long minimum = 0U;
  if (!PyArg_ParseTuple(args, "KK:process_physical_thread_permits_acquire",
                        &desired, &minimum)) {
    return nullptr;
  }
  const auto granted =
      sanitize::internal::acquire_process_physical_thread_permits(
          static_cast<std::size_t>(desired), static_cast<std::size_t>(minimum));
  PyObject *result = PyLong_FromSize_t(granted);
  if (!result && granted != 0U) {
    // The native commit succeeded but Python could not publish its ownership
    // amount. Roll back before propagating MemoryError so no permit is
    // orphaned.
    sanitize::internal::release_process_physical_thread_permits(granted);
  }
  return result;
}

PyObject *py_process_physical_thread_permits_release(PyObject *,
                                                     PyObject *args) {
  unsigned long long amount = 0U;
  if (!PyArg_ParseTuple(args, "K:process_physical_thread_permits_release",
                        &amount)) {
    return nullptr;
  }
  sanitize::internal::release_process_physical_thread_permits(
      static_cast<std::size_t>(amount));
  Py_RETURN_NONE;
}

PyObject *
py_process_external_runtime_thread_permit_lease_acquire(PyObject *,
                                                        PyObject *args) {
  if (!sanitize::internal::runtime_owner_process()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "external runtime permit lease cannot be acquired after fork");
    return nullptr;
  }
  unsigned long long desired = 0U;
  unsigned long long minimum = 0U;
  if (!PyArg_ParseTuple(
          args, "KK:process_external_runtime_thread_permit_lease_acquire",
          &desired, &minimum)) {
    return nullptr;
  }
  auto *receipt = new (std::nothrow) ExternalRuntimePermitReceipt(
      static_cast<std::size_t>(desired), static_cast<std::size_t>(minimum));
  if (!receipt) {
    PyErr_NoMemory();
    return nullptr;
  }
  if (receipt->receipt_id == 0U) {
    delete receipt;
    PyErr_SetString(PyExc_RuntimeError,
                    "external runtime permit receipt id space exhausted");
    return nullptr;
  }
  const auto granted = receipt->lease.amount();
  if (granted < static_cast<std::size_t>(minimum)) {
    delete receipt;
    Py_RETURN_NONE;
  }
  PyObject *capsule =
      PyCapsule_New(receipt, kExternalRuntimePermitLeaseCapsuleName,
                    destroy_external_runtime_permit_lease_capsule);
  if (!capsule) {
    delete receipt;
    return nullptr;
  }
  PyObject *result = PyTuple_New(2);
  if (!result) {
    Py_DECREF(capsule);
    return nullptr;
  }
  if (!tuple_set_item_steal(result, 0, capsule) ||
      !tuple_set_item_steal(result, 1, PyLong_FromSize_t(granted))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

PyObject *
py_process_external_runtime_thread_permit_lease_resize(PyObject *,
                                                       PyObject *args) {
  if (!sanitize::internal::runtime_owner_process()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "external runtime permit lease cannot be mutated after fork");
    return nullptr;
  }
  PyObject *capsule = nullptr;
  unsigned long long target = 0U;
  unsigned long long expected_generation = 0U;
  if (!PyArg_ParseTuple(
          args, "OK|K:process_external_runtime_thread_permit_lease_resize",
          &capsule, &target, &expected_generation)) {
    return nullptr;
  }
  ExternalRuntimePermitReceipt *receipt = nullptr;
  if (!resolve_external_runtime_permit_lease(capsule, &receipt))
    return nullptr;
  if (!receipt->owner_process_matches()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "external runtime permit receipt belongs to a different process");
    return nullptr;
  }
  if (expected_generation != 0U && expected_generation != receipt->generation) {
    PyErr_SetString(PyExc_RuntimeError,
                    "stale external runtime permit receipt generation");
    return nullptr;
  }
  const auto before = receipt->lease.amount();
  const auto wanted = static_cast<std::size_t>(target);
  if (wanted > before) {
    PyErr_SetString(PyExc_ValueError,
                    "external runtime permit lease cannot grow via resize");
    return nullptr;
  }
  const bool mutates = wanted != before;
  if (mutates &&
      receipt->generation == std::numeric_limits<std::uint64_t>::max()) {
    PyErr_SetString(PyExc_RuntimeError,
                    "external runtime permit receipt generation exhausted");
    return nullptr;
  }
  const auto next_generation = receipt->generation + (mutates ? 1U : 0U);
  PyObject *out = PyTuple_New(2);
  if (!out)
    return nullptr;
  if (!tuple_set_item_steal(out, 0,
                            PyLong_FromUnsignedLongLong(next_generation)) ||
      !tuple_set_item_steal(out, 1, PyLong_FromSize_t(wanted))) {
    Py_DECREF(out);
    return nullptr;
  }
  if (!receipt->lease.shrink(wanted)) {
    Py_DECREF(out);
    PyErr_SetString(PyExc_ValueError,
                    "external runtime permit lease cannot grow via resize");
    return nullptr;
  }
  receipt->generation = next_generation;
  return out;
}

PyObject *
py_process_external_runtime_thread_permit_lease_metadata(PyObject *,
                                                         PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(
          args, "O:process_external_runtime_thread_permit_lease_metadata",
          &capsule)) {
    return nullptr;
  }
  ExternalRuntimePermitReceipt *receipt = nullptr;
  if (!resolve_external_runtime_permit_lease(capsule, &receipt) ||
      !receipt->owner_process_matches()) {
    if (!PyErr_Occurred())
      PyErr_SetString(PyExc_RuntimeError,
                      "invalid external runtime permit receipt");
    return nullptr;
  }
  PyObject *out = PyTuple_New(3);
  if (!out)
    return nullptr;
  if (!tuple_set_item_steal(out, 0,
                            PyLong_FromUnsignedLongLong(receipt->receipt_id)) ||
      !tuple_set_item_steal(out, 1,
                            PyLong_FromUnsignedLongLong(receipt->generation)) ||
      !tuple_set_item_steal(out, 2,
                            PyLong_FromSize_t(receipt->lease.amount()))) {
    Py_DECREF(out);
    return nullptr;
  }
  return out;
}

PyObject *
py_process_external_runtime_thread_permit_lease_amount(PyObject *,
                                                       PyObject *args) {
  if (!sanitize::internal::runtime_owner_process()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "external runtime permit lease cannot be queried after fork");
    return nullptr;
  }
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args,
                        "O:process_external_runtime_thread_permit_lease_amount",
                        &capsule)) {
    return nullptr;
  }
  ExternalRuntimePermitReceipt *receipt = nullptr;
  if (!resolve_external_runtime_permit_lease(capsule, &receipt)) {
    return nullptr;
  }
  if (!receipt->owner_process_matches()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "external runtime permit receipt belongs to a different process");
    return nullptr;
  }
  return PyLong_FromSize_t(receipt->lease.amount());
}

PyObject *py_process_external_runtime_resident_threads_add(PyObject *,
                                                           PyObject *args) {
  unsigned long long amount = 0U;
  if (!PyArg_ParseTuple(args, "K:process_external_runtime_resident_threads_add",
                        &amount)) {
    return nullptr;
  }
  sanitize::internal::add_process_external_runtime_resident_threads(
      static_cast<std::size_t>(amount));
  Py_RETURN_NONE;
}

PyObject *py_process_external_runtime_resident_threads_release(PyObject *,
                                                               PyObject *args) {
  unsigned long long amount = 0U;
  if (!PyArg_ParseTuple(args,
                        "K:process_external_runtime_resident_threads_release",
                        &amount)) {
    return nullptr;
  }
  sanitize::internal::release_process_external_runtime_resident_threads(
      static_cast<std::size_t>(amount));
  Py_RETURN_NONE;
}

PyObject *py_process_external_runtime_stack_debt_threads_add(PyObject *,
                                                             PyObject *args) {
  unsigned long long amount = 0U;
  if (!PyArg_ParseTuple(
          args, "K:process_external_runtime_stack_debt_threads_add", &amount)) {
    return nullptr;
  }
  sanitize::internal::add_process_external_runtime_stack_debt_threads(
      static_cast<std::size_t>(amount));
  Py_RETURN_NONE;
}

PyObject *
py_process_external_runtime_stack_debt_threads_release(PyObject *,
                                                       PyObject *args) {
  unsigned long long amount = 0U;
  if (!PyArg_ParseTuple(args,
                        "K:process_external_runtime_stack_debt_threads_release",
                        &amount)) {
    return nullptr;
  }
  sanitize::internal::release_process_external_runtime_stack_debt_threads(
      static_cast<std::size_t>(amount));
  Py_RETURN_NONE;
}

PyObject *py_process_external_runtime_residency_update(PyObject *,
                                                       PyObject *args) {
  long long identity_delta = 0;
  long long stack_debt_delta = 0;
  if (!PyArg_ParseTuple(args, "LL:process_external_runtime_residency_update",
                        &identity_delta, &stack_debt_delta)) {
    return nullptr;
  }
  sanitize::internal::update_process_external_runtime_residency(
      static_cast<std::int64_t>(identity_delta),
      static_cast<std::int64_t>(stack_debt_delta));
  Py_RETURN_NONE;
}

PyObject *py_process_thread_stack_reservation_bytes(PyObject *, PyObject *) {
  return PyLong_FromUnsignedLongLong(
      sanitize::internal::process_thread_stack_reservation_bytes());
}

PyObject *py_process_file_descriptor_permit_lease_acquire_wait(PyObject *,
                                                               PyObject *args) {
  unsigned long long desired = 0U;
  unsigned long long minimum = 0U;
  unsigned long long timeout_millis = 0U;
  if (!PyArg_ParseTuple(args,
                        "KKK:process_file_descriptor_permit_lease_acquire_wait",
                        &desired, &minimum, &timeout_millis)) {
    return nullptr;
  }
  if (!sanitize::internal::runtime_owner_process()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "file descriptor permit lease cannot be acquired after fork");
    return nullptr;
  }
  auto *receipt = new (std::nothrow) FdPermitReceipt(
      static_cast<std::size_t>(desired), static_cast<std::size_t>(minimum),
      static_cast<std::uint64_t>(timeout_millis));
  if (!receipt) {
    PyErr_NoMemory();
    return nullptr;
  }
  if (receipt->receipt_id == 0U) {
    delete receipt;
    PyErr_SetString(PyExc_RuntimeError,
                    "file descriptor permit receipt id space exhausted");
    return nullptr;
  }
  const auto granted = receipt->lease.amount();
  if (granted < static_cast<std::size_t>(minimum)) {
    delete receipt;
    Py_RETURN_NONE;
  }
  PyObject *capsule = PyCapsule_New(receipt, kFdPermitLeaseCapsuleName,
                                    destroy_fd_permit_lease_capsule);
  if (!capsule) {
    delete receipt;
    return nullptr;
  }
  PyObject *result = PyTuple_New(2);
  if (!result) {
    Py_DECREF(capsule);
    return nullptr;
  }
  if (!tuple_set_item_steal(result, 0, capsule) ||
      !tuple_set_item_steal(result, 1, PyLong_FromSize_t(granted))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

PyObject *py_process_file_descriptor_permit_lease_resize(PyObject *,
                                                         PyObject *args) {
  PyObject *capsule = nullptr;
  unsigned long long target = 0U;
  unsigned long long expected_generation = 0U;
  if (!PyArg_ParseTuple(args,
                        "OK|K:process_file_descriptor_permit_lease_resize",
                        &capsule, &target, &expected_generation))
    return nullptr;
  if (!sanitize::internal::runtime_owner_process()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "file descriptor permit lease cannot be mutated after fork");
    return nullptr;
  }
  FdPermitReceipt *receipt = nullptr;
  if (!resolve_fd_permit_lease(capsule, &receipt))
    return nullptr;
  if (!receipt->owner_process_matches()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "file descriptor permit receipt belongs to a different process");
    return nullptr;
  }
  if (expected_generation != 0U && expected_generation != receipt->generation) {
    PyErr_SetString(PyExc_RuntimeError,
                    "stale file descriptor permit receipt generation");
    return nullptr;
  }
  const auto before = receipt->lease.amount();
  const auto opened = receipt->lease.opened();
  const auto wanted = static_cast<std::size_t>(target);
  if (wanted > before || wanted < opened) {
    PyErr_SetString(PyExc_ValueError,
                    "file descriptor permit lease cannot grow or shrink below "
                    "opened descriptors");
    return nullptr;
  }
  const bool mutates = wanted != before;
  if (mutates &&
      receipt->generation == std::numeric_limits<std::uint64_t>::max()) {
    PyErr_SetString(PyExc_RuntimeError,
                    "file descriptor permit receipt generation exhausted");
    return nullptr;
  }
  const auto next_generation = receipt->generation + (mutates ? 1U : 0U);
  PyObject *out = PyTuple_New(3);
  if (!out)
    return nullptr;
  if (!tuple_set_item_steal(out, 0,
                            PyLong_FromUnsignedLongLong(next_generation)) ||
      !tuple_set_item_steal(out, 1, PyLong_FromSize_t(wanted)) ||
      !tuple_set_item_steal(out, 2, PyLong_FromSize_t(opened))) {
    Py_DECREF(out);
    return nullptr;
  }
  if (!receipt->lease.shrink(wanted)) {
    Py_DECREF(out);
    PyErr_SetString(PyExc_ValueError,
                    "file descriptor permit lease cannot grow or shrink below "
                    "opened descriptors");
    return nullptr;
  }
  receipt->generation = next_generation;
  return out;
}

PyObject *py_process_file_descriptor_permit_lease_metadata(PyObject *,
                                                           PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:process_file_descriptor_permit_lease_metadata",
                        &capsule))
    return nullptr;
  FdPermitReceipt *receipt = nullptr;
  if (!resolve_fd_permit_lease(capsule, &receipt) ||
      !receipt->owner_process_matches()) {
    if (!PyErr_Occurred())
      PyErr_SetString(PyExc_RuntimeError,
                      "invalid file descriptor permit receipt");
    return nullptr;
  }
  PyObject *out = PyTuple_New(4);
  if (!out)
    return nullptr;
  if (!tuple_set_item_steal(out, 0,
                            PyLong_FromUnsignedLongLong(receipt->receipt_id)) ||
      !tuple_set_item_steal(out, 1,
                            PyLong_FromUnsignedLongLong(receipt->generation)) ||
      !tuple_set_item_steal(out, 2,
                            PyLong_FromSize_t(receipt->lease.amount())) ||
      !tuple_set_item_steal(out, 3,
                            PyLong_FromSize_t(receipt->lease.opened()))) {
    Py_DECREF(out);
    return nullptr;
  }
  return out;
}

PyObject *py_process_file_descriptor_permit_lease_mark_opened(PyObject *,
                                                              PyObject *args) {
  PyObject *capsule = nullptr;
  unsigned long long amount = 0U;
  unsigned long long expected_generation = 0U;
  if (!PyArg_ParseTuple(args,
                        "OK|K:process_file_descriptor_permit_lease_mark_opened",
                        &capsule, &amount, &expected_generation))
    return nullptr;
  FdPermitReceipt *receipt = nullptr;
  if (!resolve_fd_permit_lease(capsule, &receipt) ||
      !receipt->owner_process_matches()) {
    if (!PyErr_Occurred())
      PyErr_SetString(PyExc_RuntimeError,
                      "invalid file descriptor permit receipt");
    return nullptr;
  }
  if (expected_generation != 0U && expected_generation != receipt->generation) {
    PyErr_SetString(PyExc_RuntimeError,
                    "stale file descriptor permit receipt generation");
    return nullptr;
  }
  const auto requested = static_cast<std::size_t>(amount);
  const auto before_opened = receipt->lease.opened();
  const auto permit_amount = receipt->lease.amount();
  if (requested > permit_amount - before_opened) {
    PyErr_SetString(PyExc_ValueError,
                    "file descriptor permit lease open exceeds authority");
    return nullptr;
  }
  const bool mutates = requested != 0U;
  if (mutates &&
      receipt->generation == std::numeric_limits<std::uint64_t>::max()) {
    PyErr_SetString(PyExc_RuntimeError,
                    "file descriptor permit receipt generation exhausted");
    return nullptr;
  }
  const auto next_generation = receipt->generation + (mutates ? 1U : 0U);
  const auto next_opened = before_opened + requested;
  PyObject *out = PyTuple_New(3);
  if (!out)
    return nullptr;
  if (!tuple_set_item_steal(out, 0,
                            PyLong_FromUnsignedLongLong(next_generation)) ||
      !tuple_set_item_steal(out, 1, PyLong_FromSize_t(permit_amount)) ||
      !tuple_set_item_steal(out, 2, PyLong_FromSize_t(next_opened))) {
    Py_DECREF(out);
    return nullptr;
  }
  if (mutates) {
    receipt->lease.mark_opened(requested);
    receipt->generation = next_generation;
  }
  return out;
}

PyObject *py_process_file_descriptor_permit_lease_mark_closed(PyObject *,
                                                              PyObject *args) {
  PyObject *capsule = nullptr;
  unsigned long long amount = 0U;
  unsigned long long expected_generation = 0U;
  if (!PyArg_ParseTuple(args,
                        "OK|K:process_file_descriptor_permit_lease_mark_closed",
                        &capsule, &amount, &expected_generation))
    return nullptr;
  FdPermitReceipt *receipt = nullptr;
  if (!resolve_fd_permit_lease(capsule, &receipt) ||
      !receipt->owner_process_matches()) {
    if (!PyErr_Occurred())
      PyErr_SetString(PyExc_RuntimeError,
                      "invalid file descriptor permit receipt");
    return nullptr;
  }
  if (expected_generation != 0U && expected_generation != receipt->generation) {
    PyErr_SetString(PyExc_RuntimeError,
                    "stale file descriptor permit receipt generation");
    return nullptr;
  }
  const auto requested = static_cast<std::size_t>(amount);
  const auto before_opened = receipt->lease.opened();
  const auto permit_amount = receipt->lease.amount();
  if (requested > before_opened) {
    PyErr_SetString(
        PyExc_ValueError,
        "file descriptor permit lease close exceeds opened authority");
    return nullptr;
  }
  const bool mutates = requested != 0U;
  if (mutates &&
      receipt->generation == std::numeric_limits<std::uint64_t>::max()) {
    PyErr_SetString(PyExc_RuntimeError,
                    "file descriptor permit receipt generation exhausted");
    return nullptr;
  }
  const auto next_generation = receipt->generation + (mutates ? 1U : 0U);
  const auto next_opened = before_opened - requested;
  PyObject *out = PyTuple_New(3);
  if (!out)
    return nullptr;
  if (!tuple_set_item_steal(out, 0,
                            PyLong_FromUnsignedLongLong(next_generation)) ||
      !tuple_set_item_steal(out, 1, PyLong_FromSize_t(permit_amount)) ||
      !tuple_set_item_steal(out, 2, PyLong_FromSize_t(next_opened))) {
    Py_DECREF(out);
    return nullptr;
  }
  if (mutates) {
    receipt->lease.mark_closed(requested);
    receipt->generation = next_generation;
  }
  return out;
}

PyObject *py_process_file_descriptor_permit_lease_amount(PyObject *,
                                                         PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:process_file_descriptor_permit_lease_amount",
                        &capsule)) {
    return nullptr;
  }
  if (!sanitize::internal::runtime_owner_process()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "file descriptor permit lease cannot be queried after fork");
    return nullptr;
  }
  FdPermitReceipt *receipt = nullptr;
  if (!resolve_fd_permit_lease(capsule, &receipt)) {
    return nullptr;
  }
  if (!receipt->owner_process_matches()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "file descriptor permit receipt belongs to a different process");
    return nullptr;
  }
  return PyLong_FromSize_t(receipt->lease.amount());
}

PyObject *py_process_file_descriptor_mark_opened(PyObject *, PyObject *args) {
  unsigned long long amount = 0U;
  if (!PyArg_ParseTuple(args, "K:process_file_descriptor_mark_opened",
                        &amount)) {
    return nullptr;
  }
  sanitize::internal::mark_process_file_descriptors_opened(
      static_cast<std::size_t>(amount));
  Py_RETURN_NONE;
}

PyObject *py_process_file_descriptor_mark_closed(PyObject *, PyObject *args) {
  unsigned long long amount = 0U;
  if (!PyArg_ParseTuple(args, "K:process_file_descriptor_mark_closed",
                        &amount)) {
    return nullptr;
  }
  sanitize::internal::mark_process_file_descriptors_closed(
      static_cast<std::size_t>(amount));
  Py_RETURN_NONE;
}

PyObject *py_process_file_descriptor_count(PyObject *, PyObject *) {
  const auto observed = sanitize::internal::process_file_descriptor_count();
  if (!observed) {
    Py_RETURN_NONE;
  }
  return PyLong_FromSize_t(*observed);
}

PyObject *py_process_file_descriptor_permits_snapshot(PyObject *, PyObject *) {
  PyObject *out = PyTuple_New(6);
  if (!out) {
    return nullptr;
  }
  if (!tuple_set_item_steal(
          out, 0,
          PyLong_FromSize_t(
              sanitize::internal::process_file_descriptor_permits_in_use())) ||
      !tuple_set_item_steal(
          out, 1,
          PyLong_FromSize_t(
              sanitize::internal::process_file_descriptors_opened())) ||
      !tuple_set_item_steal(
          out, 2,
          PyLong_FromSize_t(
              sanitize::internal::process_file_descriptor_capacity())) ||
      !tuple_set_item_steal(
          out, 3,
          PyLong_FromSize_t(
              sanitize::internal::process_file_descriptor_rejections())) ||
      !tuple_set_item_steal(
          out, 4,
          PyLong_FromSize_t(
              sanitize::internal::
                  process_file_descriptor_protocol_violations())) ||
      !tuple_set_item_steal(
          out, 5,
          PyLong_FromSize_t(
              sanitize::internal::
                  process_file_descriptor_uncertain_close_debts()))) {
    Py_DECREF(out);
    return nullptr;
  }
  return out;
}

PyObject *py_process_physical_thread_mark_running(PyObject *, PyObject *) {
  sanitize::internal::mark_process_physical_thread_running();
  Py_RETURN_NONE;
}

PyObject *py_process_physical_thread_mark_stopped(PyObject *, PyObject *) {
  sanitize::internal::mark_process_physical_thread_stopped();
  Py_RETURN_NONE;
}

PyObject *py_operation_task_arena_runtime_snapshot(PyObject *, PyObject *) {
  const auto snapshot =
      sanitize::internal::OperationTaskArena::RuntimeSnapshot();
  // Keep this tuple synchronized with the runtime snapshot decoder.
  PyObject *out = PyTuple_New(30);
  if (!out) {
    return nullptr;
  }
  if (!tuple_set_item_steal(out, 0, PyLong_FromSize_t(snapshot.live_arenas)) ||
      !tuple_set_item_steal(out, 1,
                            PyLong_FromSize_t(snapshot.detached_workers)) ||
      !tuple_set_item_steal(out, 2,
                            PyLong_FromSize_t(snapshot.reaper_workers)) ||
      !tuple_set_item_steal(out, 3,
                            PyLong_FromSize_t(snapshot.reaper_queued_states)) ||
      !tuple_set_item_steal(out, 4,
                            PyLong_FromSize_t(snapshot.reaper_active_states)) ||
      !tuple_set_item_steal(
          out, 5, PyLong_FromSize_t(snapshot.reaper_reserved_states)) ||
      !tuple_set_item_steal(out, 6,
                            PyLong_FromSize_t(snapshot.reaper_parked_states)) ||
      !tuple_set_item_steal(out, 7,
                            PyLong_FromSize_t(snapshot.counter_underflows)) ||
      !tuple_set_item_steal(out, 8,
                            PyLong_FromSize_t(snapshot.reaper_queued_bytes)) ||
      !tuple_set_item_steal(out, 9,
                            PyLong_FromSize_t(snapshot.reaper_active_bytes)) ||
      !tuple_set_item_steal(
          out, 10, PyLong_FromSize_t(snapshot.reaper_reserved_bytes)) ||
      !tuple_set_item_steal(out, 11,
                            PyLong_FromSize_t(snapshot.reaper_parked_bytes)) ||
      !tuple_set_item_steal(
          out, 12, PyLong_FromLongLong(snapshot.oldest_parked_since_ns)) ||
      !tuple_set_item_steal(
          out, 13, PyLong_FromSize_t(snapshot.reaper_thread_permits)) ||
      !tuple_set_item_steal(
          out, 14, PyLong_FromSize_t(snapshot.reaper_thread_start_failures)) ||
      !tuple_set_item_steal(out, 15,
                            PyLong_FromSize_t(snapshot.reaper_over_capacity)) ||
      !tuple_set_item_steal(
          out, 16, PyLong_FromSize_t(snapshot.reaper_terminal_states)) ||
      !tuple_set_item_steal(
          out, 17, PyLong_FromSize_t(snapshot.reaper_terminal_bytes)) ||
      !tuple_set_item_steal(
          out, 18, PyLong_FromLongLong(snapshot.oldest_terminal_since_ns)) ||
      !tuple_set_item_steal(
          out, 19, PyLong_FromSize_t(snapshot.reaper_stopping_lanes)) ||
      !tuple_set_item_steal(
          out, 20, PyLong_FromSize_t(snapshot.native_physical_threads)) ||
      !tuple_set_item_steal(
          out, 21,
          PyLong_FromSize_t(snapshot.native_physical_thread_capacity)) ||
      !tuple_set_item_steal(
          out, 22,
          PyLong_FromSize_t(snapshot.native_physical_thread_rejections)) ||
      !tuple_set_item_steal(
          out, 23,
          PyLong_FromSize_t(snapshot.external_runtime_thread_permits)) ||
      !tuple_set_item_steal(
          out, 24,
          PyLong_FromSize_t(snapshot.completion_memory_protocol_violations)) ||
      !tuple_set_item_steal(
          out, 25, PyLong_FromSize_t(snapshot.total_physical_thread_permits)) ||
      !tuple_set_item_steal(
          out, 26,
          PyLong_FromSize_t(snapshot.external_runtime_resident_threads)) ||
      !tuple_set_item_steal(
          out, 27,
          PyLong_FromLong(snapshot.thread_permit_snapshot_stable ? 1L : 0L)) ||
      !tuple_set_item_steal(
          out, 28,
          PyLong_FromSize_t(
              snapshot.external_runtime_resident_protocol_violations)) ||
      !tuple_set_item_steal(
          out, 29,
          PyLong_FromSize_t(snapshot.external_runtime_stack_debt_threads))) {
    Py_DECREF(out);
    return nullptr;
  }
  return out;
}

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
  const auto task_count = static_cast<std::size_t>(tasks_per_stage);
  const auto stage_runnable_width =
      std::min(static_cast<std::size_t>(upstream_workers), task_count) +
      std::min(static_cast<std::size_t>(output_workers), task_count);
  const auto expected_peak = std::max<std::size_t>(
      1U, std::min<std::size_t>(
              std::min<std::size_t>(static_cast<std::size_t>(requested_workers),
                                    stage_runnable_width),
              static_cast<std::size_t>(
                  sanitize::internal::process_cpu_governor().capacity())));
  const bool release_inline = requested_workers == 1;
  const auto make_worker = [state, release_inline](bool is_output) {
    return [state, release_inline,
            is_output](std::uint64_t &&value, std::size_t,
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
      // A one-worker arena executes inline during Submit(), so no coordinator
      // frame is available to open the probe barrier. Wider arenas always use
      // governed worker threads and keep the barrier closed until submission
      // has populated every stage-eligible physical slot.
      if (release_inline) {
        state->release.store(true, std::memory_order_release);
        state->release.notify_all();
      }
      auto release_on_stop = [state] {
        state->release.store(true, std::memory_order_release);
        state->release.notify_all();
      };
      sanitize::internal::StopCallback<decltype(release_on_stop)> stop_release(
          stop, std::move(release_on_stop));
      while (!state->release.load(std::memory_order_acquire) &&
             !stop.stop_requested()) {
        sanitize::internal::WaitOnAtomic(state->release, false,
                                         std::memory_order_acquire);
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
  // Keep the admitted callbacks blocked until both stages have submitted all
  // work. Releasing from the callback that first reaches the CPU limit lets a
  // small, fast cohort drain queues before the remaining stage-eligible workers
  // are started. Once every eligible slot has work, FIFO CPU admission rotates
  // the bounded runnable credits through that complete set without exceeding
  // expected_peak.
  while (state->peak.load(std::memory_order_acquire) < expected_peak &&
         std::chrono::steady_clock::now() < startup_deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  const bool reached_expected_peak =
      state->peak.load(std::memory_order_acquire) >= expected_peak;
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
  if (!reached_expected_peak) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "operation arena probe did not reach runnable CPU capacity");
    return nullptr;
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
  constexpr std::size_t kMaximumWorkerCount = 4U;
  const auto worker_count = std::min<std::size_t>(
      kMaximumWorkerCount,
      static_cast<std::size_t>(
          sanitize::internal::process_cpu_governor().capacity()));
  if (worker_count < 2U) {
    PyErr_SetString(PyExc_RuntimeError,
                    "arena stealing probe requires two runnable CPUs");
    return nullptr;
  }
  auto arena_result =
      sanitize::internal::OperationTaskArena::Make(worker_count);
  if (!arena_result.ok()) {
    PyErr_SetString(PyExc_RuntimeError,
                    arena_result.status().ToString().c_str());
    return nullptr;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  std::array<std::atomic<bool>, kMaximumWorkerCount> release_worker{};
  std::atomic<std::size_t> blockers_started{0};
  std::atomic<bool> displaced_finished{false};
  std::atomic<std::size_t> displaced_worker{99};
  std::atomic<std::size_t> completed{0};

  const auto release_all = [&]() noexcept {
    for (std::size_t index = 0; index < worker_count; ++index) {
      release_worker[index].store(true, std::memory_order_release);
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

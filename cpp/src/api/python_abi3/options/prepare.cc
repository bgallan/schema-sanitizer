// Validates serialized options and returns prepared native state.

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/runtime/process_identity.hh"
#include "sanitize/options/options.hh"

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>

namespace core_abi3_internal {
namespace {

constexpr const char *kOperationMemoryLedgerCapsuleName =
    "schema_sanitizer.operation_memory_ledger";
constexpr const char *kOperationMemoryReservationCapsuleName =
    "schema_sanitizer.operation_memory_reservation";

using OperationMemoryLedgerPtr =
    std::shared_ptr<sanitize::internal::OperationMemoryLedger>;

std::atomic<std::uint64_t> g_next_operation_memory_reservation_id{1U};

[[nodiscard]] std::uint64_t
allocate_operation_memory_reservation_id() noexcept {
  auto current =
      g_next_operation_memory_reservation_id.load(std::memory_order_relaxed);
  for (;;) {
    if (current == 0U || current == std::numeric_limits<std::uint64_t>::max()) {
      return 0U;
    }
    if (g_next_operation_memory_reservation_id.compare_exchange_weak(
            current, current + 1U, std::memory_order_relaxed,
            std::memory_order_relaxed)) {
      return current;
    }
  }
}

struct OperationMemoryReservation final {
  OperationMemoryLedgerPtr ledger;
  std::int64_t bytes = 0;
  std::uint64_t reservation_id = 0U;
  std::uint64_t generation = 1U;
  sanitize::internal::RuntimeProcessId owner_process = 0U;

  [[nodiscard]] bool owner_process_matches() const noexcept {
    return sanitize::internal::runtime_owner_process() &&
           owner_process == sanitize::internal::current_runtime_process_id();
  }

  void release() noexcept {
    const auto amount = std::exchange(bytes, 0);
    if (amount > 0 && ledger) {
      ledger->Release(amount);
    }
  }
};

struct AtomicEpochCounter final {
  std::atomic<std::uint64_t> value{0};
};

constexpr const char *kAtomicEpochCounterCapsuleName =
    "schema_sanitizer.atomic_epoch_counter";

void destroy_atomic_epoch_counter_capsule(PyObject *capsule) {
  auto *counter = static_cast<AtomicEpochCounter *>(
      PyCapsule_GetPointer(capsule, kAtomicEpochCounterCapsuleName));
  if (!counter) {
    PyErr_Clear();
    return;
  }
  delete counter;
}

bool resolve_atomic_epoch_counter(PyObject *capsule, AtomicEpochCounter **out) {
  if (!out) {
    PyErr_SetString(PyExc_RuntimeError, "atomic epoch output is null");
    return false;
  }
  auto *counter = static_cast<AtomicEpochCounter *>(
      PyCapsule_GetPointer(capsule, kAtomicEpochCounterCapsuleName));
  if (!counter) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_RuntimeError, "atomic epoch counter is null");
    }
    return false;
  }
  *out = counter;
  return true;
}

void destroy_operation_memory_ledger_capsule(PyObject *capsule) {
  if (!sanitize::internal::runtime_owner_process()) {
    return;
  }
  auto *holder = static_cast<OperationMemoryLedgerPtr *>(
      PyCapsule_GetPointer(capsule, kOperationMemoryLedgerCapsuleName));
  if (!holder) {
    PyErr_Clear();
    return;
  }
  delete holder;
}

bool resolve_operation_memory_ledger(PyObject *capsule,
                                     OperationMemoryLedgerPtr *out) {
  if (!out) {
    PyErr_SetString(PyExc_RuntimeError,
                    "operation memory ledger output is null");
    return false;
  }
  auto *holder = static_cast<OperationMemoryLedgerPtr *>(
      PyCapsule_GetPointer(capsule, kOperationMemoryLedgerCapsuleName));
  if (!holder || !*holder) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_RuntimeError, "operation memory ledger is null");
    }
    return false;
  }
  *out = *holder;
  return true;
}

PyObject *wrap_operation_memory_ledger(OperationMemoryLedgerPtr ledger) {
  auto *holder = new (std::nothrow) OperationMemoryLedgerPtr(std::move(ledger));
  if (!holder) {
    PyErr_NoMemory();
    return nullptr;
  }
  PyObject *capsule = PyCapsule_New(holder, kOperationMemoryLedgerCapsuleName,
                                    destroy_operation_memory_ledger_capsule);
  if (!capsule) {
    delete holder;
  }
  return capsule;
}

void destroy_operation_memory_reservation_capsule(PyObject *capsule) {
  auto *reservation = static_cast<OperationMemoryReservation *>(
      PyCapsule_GetPointer(capsule, kOperationMemoryReservationCapsuleName));
  if (!reservation) {
    PyErr_Clear();
    return;
  }
  if (reservation->owner_process_matches()) {
    reservation->release();
  }
  delete reservation;
}

bool resolve_operation_memory_reservation(PyObject *capsule,
                                          OperationMemoryReservation **out) {
  if (!out) {
    PyErr_SetString(PyExc_RuntimeError,
                    "operation memory reservation output is null");
    return false;
  }
  auto *reservation = static_cast<OperationMemoryReservation *>(
      PyCapsule_GetPointer(capsule, kOperationMemoryReservationCapsuleName));
  if (!reservation || !reservation->ledger) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_RuntimeError,
                      "operation memory reservation is null");
    }
    return false;
  }
  *out = reservation;
  return true;
}

} // namespace

PyObject *py_options_prepare_bytes(PyObject *, PyObject *args) {
  PyObject *bytes_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:options_prepare_bytes", &bytes_obj)) {
    return nullptr;
  }

  PyObject *view_owner = nullptr;
  const std::uint8_t *data = nullptr;
  Py_ssize_t size = 0;
  if (!readonly_buffer_view(bytes_obj, &data, &size, &view_owner)) {
    return nullptr;
  }

  schema_sanitizer_prepared_options *out = nullptr;
  char *error = nullptr;
  const int status = schema_sanitizer_options_prepare_bytes(
      data, static_cast<std::size_t>(size), &out, &error);
  Py_DECREF(view_owner);

  if (status != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(status, error);
    return nullptr;
  }
  return wrap_prepared_options_capsule(out);
}

PyObject *py_options_with_detected_at(PyObject *, PyObject *args) {
  PyObject *prepared_obj = nullptr;
  const char *detected_at = nullptr;
  if (!PyArg_ParseTuple(args, "Os:options_with_detected_at", &prepared_obj,
                        &detected_at)) {
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  auto cloned = std::make_shared<sanitize::PreparedOptions>(*prepared);
  cloned->operation_detected_at = detected_at ? detected_at : "";
  auto *out = new (std::nothrow)
      schema_sanitizer_prepared_options{.prepared = std::move(cloned)};
  if (!out) {
    PyErr_NoMemory();
    return nullptr;
  }
  return wrap_prepared_options_capsule(out);
}

PyObject *py_operation_memory_ledger_create(PyObject *, PyObject *args) {
  long long limit_bytes = 0;
  if (!PyArg_ParseTuple(args, "L:operation_memory_ledger_create",
                        &limit_bytes)) {
    return nullptr;
  }
  if (limit_bytes <= 0) {
    PyErr_SetString(PyExc_ValueError,
                    "operation memory ledger limit must be > 0");
    return nullptr;
  }
  return wrap_operation_memory_ledger(
      sanitize::internal::make_operation_memory_ledger(limit_bytes));
}

PyObject *py_operation_memory_reservation_create(PyObject *, PyObject *args) {
  if (!sanitize::internal::runtime_owner_process()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "operation memory reservation cannot be created after fork");
    return nullptr;
  }
  PyObject *capsule = nullptr;
  long long bytes = 0;
  const char *stage = "python_runtime";
  if (!PyArg_ParseTuple(args, "OL|s:operation_memory_reservation_create",
                        &capsule, &bytes, &stage)) {
    return nullptr;
  }
  if (bytes < 0) {
    PyErr_SetString(PyExc_ValueError,
                    "operation memory reservation must be >= 0");
    return nullptr;
  }
  OperationMemoryLedgerPtr ledger;
  if (!resolve_operation_memory_ledger(capsule, &ledger)) {
    return nullptr;
  }
  auto *reservation = new (std::nothrow) OperationMemoryReservation{
      ledger, 0, allocate_operation_memory_reservation_id(), 1U,
      sanitize::internal::current_runtime_process_id()};
  if (!reservation) {
    PyErr_NoMemory();
    return nullptr;
  }
  if (reservation->reservation_id == 0U) {
    delete reservation;
    PyErr_SetString(PyExc_RuntimeError,
                    "operation memory reservation id space exhausted");
    return nullptr;
  }
  auto status = ledger->Reserve(bytes, stage ? stage : "python_runtime");
  if (!status.ok()) {
    delete reservation;
    PyErr_SetString(PyExc_MemoryError, status.message().c_str());
    return nullptr;
  }
  reservation->bytes = bytes;
  PyObject *result =
      PyCapsule_New(reservation, kOperationMemoryReservationCapsuleName,
                    destroy_operation_memory_reservation_capsule);
  if (!result) {
    reservation->release();
    delete reservation;
    return nullptr;
  }
  return result;
}

PyObject *py_operation_memory_reservation_resize(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  long long requested = 0;
  const char *stage = "python_runtime";
  unsigned long long expected_generation = 0U;
  if (!PyArg_ParseTuple(args, "OL|sK:operation_memory_reservation_resize",
                        &capsule, &requested, &stage, &expected_generation)) {
    return nullptr;
  }
  if (requested < 0) {
    PyErr_SetString(PyExc_ValueError,
                    "operation memory reservation size must be >= 0");
    return nullptr;
  }
  OperationMemoryReservation *reservation = nullptr;
  if (!resolve_operation_memory_reservation(capsule, &reservation)) {
    return nullptr;
  }
  if (!reservation->owner_process_matches()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "operation memory reservation cannot be resized after fork");
    return nullptr;
  }
  if (expected_generation != 0U &&
      expected_generation != reservation->generation) {
    PyErr_SetString(PyExc_RuntimeError,
                    "stale operation memory reservation generation");
    return nullptr;
  }
  const auto current = reservation->bytes;
  const bool mutates = requested != current;
  if (mutates &&
      reservation->generation == std::numeric_limits<std::uint64_t>::max()) {
    PyErr_SetString(PyExc_RuntimeError,
                    "operation memory reservation generation exhausted");
    return nullptr;
  }
  const auto next_generation = reservation->generation + (mutates ? 1U : 0U);

  // Allocate every Python return object before the native commit. MemoryError
  // therefore remains a pre-commit failure instead of reporting failure after
  // exact authority has already changed.
  PyObject *out = PyTuple_New(2);
  if (!out)
    return nullptr;
  if (!tuple_set_item_steal(out, 0,
                            PyLong_FromUnsignedLongLong(next_generation)) ||
      !tuple_set_item_steal(out, 1, PyLong_FromLongLong(requested))) {
    Py_DECREF(out);
    return nullptr;
  }

  if (requested > current) {
    const auto growth = requested - current;
    auto status =
        reservation->ledger->Reserve(growth, stage ? stage : "python_runtime");
    if (!status.ok()) {
      Py_DECREF(out);
      PyErr_SetString(PyExc_MemoryError, status.message().c_str());
      return nullptr;
    }
    reservation->bytes = requested;
    reservation->generation = next_generation;
  } else if (requested < current) {
    reservation->bytes = requested;
    reservation->generation = next_generation;
    reservation->ledger->Release(current - requested);
  }
  return out;
}

PyObject *py_operation_memory_reservation_release(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  unsigned long long expected_generation = 0U;
  if (!PyArg_ParseTuple(args, "O|K:operation_memory_reservation_release",
                        &capsule, &expected_generation)) {
    return nullptr;
  }
  OperationMemoryReservation *reservation = nullptr;
  if (!resolve_operation_memory_reservation(capsule, &reservation)) {
    return nullptr;
  }
  if (!reservation->owner_process_matches()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "operation memory reservation cannot be released after fork");
    return nullptr;
  }
  if (expected_generation != 0U &&
      expected_generation != reservation->generation) {
    PyErr_SetString(PyExc_RuntimeError,
                    "stale operation memory reservation generation");
    return nullptr;
  }
  const bool mutates = reservation->bytes != 0;
  if (mutates &&
      reservation->generation == std::numeric_limits<std::uint64_t>::max()) {
    PyErr_SetString(PyExc_RuntimeError,
                    "operation memory reservation generation exhausted");
    return nullptr;
  }
  const auto next_generation = reservation->generation + (mutates ? 1U : 0U);
  PyObject *out = PyTuple_New(2);
  if (!out)
    return nullptr;
  if (!tuple_set_item_steal(out, 0,
                            PyLong_FromUnsignedLongLong(next_generation)) ||
      !tuple_set_item_steal(out, 1, PyLong_FromLongLong(0))) {
    Py_DECREF(out);
    return nullptr;
  }
  if (mutates) {
    reservation->release();
    reservation->generation = next_generation;
  }
  return out;
}

PyObject *py_operation_memory_reservation_metadata(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:operation_memory_reservation_metadata",
                        &capsule))
    return nullptr;
  OperationMemoryReservation *reservation = nullptr;
  if (!resolve_operation_memory_reservation(capsule, &reservation) ||
      !reservation->owner_process_matches()) {
    if (!PyErr_Occurred())
      PyErr_SetString(PyExc_RuntimeError,
                      "invalid operation memory reservation");
    return nullptr;
  }
  PyObject *out = PyTuple_New(3);
  if (!out)
    return nullptr;
  if (!tuple_set_item_steal(
          out, 0, PyLong_FromUnsignedLongLong(reservation->reservation_id)) ||
      !tuple_set_item_steal(
          out, 1, PyLong_FromUnsignedLongLong(reservation->generation)) ||
      !tuple_set_item_steal(
          out, 2,
          PyLong_FromLongLong(static_cast<long long>(reservation->bytes)))) {
    Py_DECREF(out);
    return nullptr;
  }
  return out;
}

PyObject *py_operation_memory_reservation_bytes(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:operation_memory_reservation_bytes",
                        &capsule)) {
    return nullptr;
  }
  OperationMemoryReservation *reservation = nullptr;
  if (!resolve_operation_memory_reservation(capsule, &reservation)) {
    return nullptr;
  }
  if (!reservation->owner_process_matches()) {
    PyErr_SetString(
        PyExc_RuntimeError,
        "operation memory reservation cannot be queried after fork");
    return nullptr;
  }
  return PyLong_FromLongLong(static_cast<long long>(reservation->bytes));
}

PyObject *py_operation_memory_ledger_reserve(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  long long bytes = 0;
  const char *stage = "python_runtime";
  if (!PyArg_ParseTuple(args, "OL|s:operation_memory_ledger_reserve", &capsule,
                        &bytes, &stage)) {
    return nullptr;
  }
  OperationMemoryLedgerPtr ledger;
  if (!resolve_operation_memory_ledger(capsule, &ledger)) {
    return nullptr;
  }
  auto status = ledger->Reserve(bytes, stage ? stage : "python_runtime");
  if (!status.ok()) {
    PyErr_SetString(PyExc_MemoryError, status.message().c_str());
    return nullptr;
  }
  Py_RETURN_NONE;
}

PyObject *py_operation_memory_ledger_reserve_snapshot(PyObject *,
                                                      PyObject *args) {
  PyObject *capsule = nullptr;
  long long bytes = 0;
  const char *stage = "python_runtime";
  if (!PyArg_ParseTuple(args, "OL|s:operation_memory_ledger_reserve_snapshot",
                        &capsule, &bytes, &stage)) {
    return nullptr;
  }
  OperationMemoryLedgerPtr ledger;
  if (!resolve_operation_memory_ledger(capsule, &ledger)) {
    return nullptr;
  }
  auto status = ledger->Reserve(bytes, stage ? stage : "python_runtime");
  if (!status.ok()) {
    PyErr_SetString(PyExc_MemoryError, status.message().c_str());
    return nullptr;
  }
  // Building the Python observation is part of the transaction: if CPython
  // cannot allocate it, restore the exact reservation before propagating OOM.
  PyObject *result =
      Py_BuildValue("(LLL)", static_cast<long long>(ledger->limit_bytes()),
                    static_cast<long long>(ledger->bytes_reserved()),
                    static_cast<long long>(ledger->peak_bytes_reserved()));
  if (result == nullptr) {
    ledger->Release(bytes);
    return nullptr;
  }
  return result;
}

PyObject *py_operation_memory_ledger_release(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  long long bytes = 0;
  if (!PyArg_ParseTuple(args, "OL:operation_memory_ledger_release", &capsule,
                        &bytes)) {
    return nullptr;
  }
  OperationMemoryLedgerPtr ledger;
  if (!resolve_operation_memory_ledger(capsule, &ledger)) {
    return nullptr;
  }
  ledger->Release(bytes);
  Py_RETURN_NONE;
}

PyObject *py_operation_memory_ledger_snapshot(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:operation_memory_ledger_snapshot", &capsule)) {
    return nullptr;
  }
  OperationMemoryLedgerPtr ledger;
  if (!resolve_operation_memory_ledger(capsule, &ledger)) {
    return nullptr;
  }
  return Py_BuildValue("(LLL)", static_cast<long long>(ledger->limit_bytes()),
                       static_cast<long long>(ledger->bytes_reserved()),
                       static_cast<long long>(ledger->peak_bytes_reserved()));
}

PyObject *py_operation_memory_ledger_diagnostics(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:operation_memory_ledger_diagnostics",
                        &capsule)) {
    return nullptr;
  }
  OperationMemoryLedgerPtr ledger;
  if (!resolve_operation_memory_ledger(capsule, &ledger)) {
    return nullptr;
  }
  return Py_BuildValue("(LL)",
                       static_cast<long long>(ledger->over_release_count()),
                       static_cast<long long>(ledger->over_release_bytes()));
}

PyObject *py_options_with_operation_context(PyObject *, PyObject *args) {
  PyObject *prepared_obj = nullptr;
  const char *detected_at = nullptr;
  PyObject *ledger_obj = nullptr;
  if (!PyArg_ParseTuple(args, "OsO:options_with_operation_context",
                        &prepared_obj, &detected_at, &ledger_obj)) {
    return nullptr;
  }
  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }
  OperationMemoryLedgerPtr ledger;
  if (!resolve_operation_memory_ledger(ledger_obj, &ledger)) {
    return nullptr;
  }
  auto cloned = std::make_shared<sanitize::PreparedOptions>(*prepared);
  cloned->operation_detected_at = detected_at ? detected_at : "";
  cloned->operation_memory_ledger = std::static_pointer_cast<void>(ledger);
  auto *out = new (std::nothrow)
      schema_sanitizer_prepared_options{.prepared = std::move(cloned)};
  if (!out) {
    PyErr_NoMemory();
    return nullptr;
  }
  return wrap_prepared_options_capsule(out);
}

PyObject *py_atomic_epoch_create(PyObject *, PyObject *) {
  auto *counter = new (std::nothrow) AtomicEpochCounter();
  if (!counter) {
    PyErr_NoMemory();
    return nullptr;
  }
  PyObject *capsule = PyCapsule_New(counter, kAtomicEpochCounterCapsuleName,
                                    destroy_atomic_epoch_counter_capsule);
  if (!capsule) {
    delete counter;
  }
  return capsule;
}

PyObject *py_atomic_epoch_increment(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:atomic_epoch_increment", &capsule)) {
    return nullptr;
  }
  AtomicEpochCounter *counter = nullptr;
  if (!resolve_atomic_epoch_counter(capsule, &counter)) {
    return nullptr;
  }
  std::uint64_t current = counter->value.load(std::memory_order_relaxed);
  for (;;) {
    if (current == UINT64_MAX) {
      Py_RETURN_FALSE;
    }
    if (counter->value.compare_exchange_weak(current, current + 1,
                                             std::memory_order_acq_rel,
                                             std::memory_order_relaxed)) {
      Py_RETURN_TRUE;
    }
  }
}

PyObject *py_atomic_epoch_decrement(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:atomic_epoch_decrement", &capsule)) {
    return nullptr;
  }
  AtomicEpochCounter *counter = nullptr;
  if (!resolve_atomic_epoch_counter(capsule, &counter)) {
    return nullptr;
  }
  std::uint64_t current = counter->value.load(std::memory_order_relaxed);
  for (;;) {
    if (current == 0) {
      Py_RETURN_FALSE;
    }
    if (counter->value.compare_exchange_weak(current, current - 1,
                                             std::memory_order_acq_rel,
                                             std::memory_order_relaxed)) {
      Py_RETURN_TRUE;
    }
  }
}

PyObject *py_atomic_epoch_value(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:atomic_epoch_value", &capsule)) {
    return nullptr;
  }
  AtomicEpochCounter *counter = nullptr;
  if (!resolve_atomic_epoch_counter(capsule, &counter)) {
    return nullptr;
  }
  return PyLong_FromUnsignedLongLong(
      counter->value.load(std::memory_order_acquire));
}

PyObject *py_atomic_epoch_write_le(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  PyObject *target = nullptr;
  Py_ssize_t offset = 0;
  if (!PyArg_ParseTuple(args, "OOn:atomic_epoch_write_le", &capsule, &target,
                        &offset)) {
    return nullptr;
  }
  AtomicEpochCounter *counter = nullptr;
  if (!resolve_atomic_epoch_counter(capsule, &counter)) {
    return nullptr;
  }
  if (!PyByteArray_Check(target)) {
    PyErr_SetString(PyExc_TypeError, "atomic epoch target must be bytearray");
    return nullptr;
  }
  const Py_ssize_t size = PyByteArray_Size(target);
  if (offset < 0 || offset > size || size - offset < 8) {
    PyErr_SetString(PyExc_ValueError, "atomic epoch target range is invalid");
    return nullptr;
  }
  char *data = PyByteArray_AsString(target);
  if (!data) {
    return nullptr;
  }
  const std::uint64_t value = counter->value.load(std::memory_order_acquire);
  for (unsigned shift = 0; shift < 64; shift += 8) {
    data[offset++] = static_cast<char>((value >> shift) & 0xffu);
  }
  Py_RETURN_NONE;
}

PyObject *py_atomic_epoch_write_activity(PyObject *, PyObject *args) {
  PyObject *capsules = nullptr;
  PyObject *target = nullptr;
  Py_ssize_t offset = 0;
  if (!PyArg_ParseTuple(args, "OOn:atomic_epoch_write_activity", &capsules,
                        &target, &offset)) {
    return nullptr;
  }
  if (!PyTuple_Check(capsules) || !PyByteArray_Check(target)) {
    PyErr_SetString(PyExc_TypeError,
                    "activity counters require tuple and bytearray");
    return nullptr;
  }
  const Py_ssize_t count = PyTuple_Size(capsules);
  const Py_ssize_t size = PyByteArray_Size(target);
  if (count < 0 || count % 4 != 0 || offset < 0 || offset > size ||
      size - offset < count * 8) {
    PyErr_SetString(PyExc_ValueError,
                    "activity counter buffer range is invalid");
    return nullptr;
  }
  char *data = PyByteArray_AsString(target);
  if (!data) {
    return nullptr;
  }
  bool quiescent = true;
  for (Py_ssize_t index = 0; index < count; ++index) {
    PyObject *capsule = PyTuple_GetItem(capsules, index); // borrowed
    AtomicEpochCounter *counter = nullptr;
    if (!capsule || !resolve_atomic_epoch_counter(capsule, &counter)) {
      return nullptr;
    }
    const std::uint64_t value = counter->value.load(std::memory_order_acquire);
    if ((index % 4 == 0 || index % 4 == 1) && value != 0) {
      quiescent = false;
    }
    for (unsigned shift = 0; shift < 64; shift += 8) {
      data[offset++] = static_cast<char>((value >> shift) & 0xffu);
    }
  }
  if (quiescent) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

PyObject *py_atomic_epoch_reset(PyObject *, PyObject *args) {
  PyObject *capsule = nullptr;
  if (!PyArg_ParseTuple(args, "O:atomic_epoch_reset", &capsule)) {
    return nullptr;
  }
  AtomicEpochCounter *counter = nullptr;
  if (!resolve_atomic_epoch_counter(capsule, &counter)) {
    return nullptr;
  }
  counter->value.store(0, std::memory_order_release);
  Py_RETURN_NONE;
}

} // namespace core_abi3_internal

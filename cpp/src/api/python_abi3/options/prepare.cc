// Validates serialized options and returns prepared native state.

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/runtime/process_identity.hh"
#include "sanitize/options/options.hh"

#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include <utility>

namespace core_abi3_internal {
namespace {

constexpr const char *kOperationMemoryLedgerCapsuleName =
    "schema_sanitizer.operation_memory_ledger";

using OperationMemoryLedgerPtr =
    std::shared_ptr<sanitize::internal::OperationMemoryLedger>;

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

} // namespace core_abi3_internal

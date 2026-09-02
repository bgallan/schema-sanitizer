/*
 * Implements Python ABI3 sink result packing and cleanup.
 *
 * This file owns result packing and cleanup helpers shared by normal and
 * registry-backed sink wrapper entry points.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "internal/abi/python_abi3/native_state.hh"

namespace core_abi3_internal {
namespace {

/// Returns None for a null stream or an Arrow stream capsule retaining its
/// keepalive.
PyObject *wrap_optional_stream(PyObject *keepalive, ArrowArrayStream *stream) {
  if (!stream) {
    Py_INCREF(Py_None);
    return Py_None;
  }
  return wrap_stream_capsule_with_keepalive(keepalive, stream);
}

} // namespace

/// Releases native stream and diagnostics handles after a failed sink call.
void release_sink_outputs(ArrowArrayStream *main_stream,
                          NativeDiagnostics *diagnostics) {
  release_arrow_stream(main_stream);
  delete diagnostics;
}

/// Packs a normal sink result into the Python ABI tuple shape.
PyObject *pack_stream_and_diagnostics(PyObject *keepalive,
                                      ArrowArrayStream *main_stream,
                                      NativeDiagnostics *diagnostics) {
  PyObject *py_main = wrap_optional_stream(keepalive, main_stream);
  if (!py_main) {
    delete diagnostics;
    return nullptr;
  }

  PyObject *py_diag = wrap_diagnostics_capsule(diagnostics);
  if (!py_diag) {
    Py_DECREF(py_main);
    return nullptr;
  }

  PyObject *tup = PyTuple_New(2);
  if (!tup) {
    Py_DECREF(py_diag);
    Py_DECREF(py_main);
    return nullptr;
  }
  if (!tuple_set_item_steal(tup, 0, py_main)) {
    Py_DECREF(tup);
    Py_DECREF(py_diag);
    return nullptr;
  }
  if (!tuple_set_item_steal(tup, 1, py_diag)) {
    Py_DECREF(tup);
    return nullptr;
  }
  return tup;
}

/// Packs a registry-backed sink result into the fixed Python ABI tuple shape.
/// The sixth slot receives a borrowed native registry state or `None`.
PyObject *pack_registry_stream_result(PyObject *keepalive,
                                      ArrowArrayStream *main_stream,
                                      NativeDiagnostics *diagnostics,
                                      std::string_view registry_json,
                                      std::string_view drifts_json,
                                      std::string_view conversion_timestamp,
                                      PyObject *native_registry_state) {
  PyObject *py_main = wrap_optional_stream(keepalive, main_stream);
  if (!py_main) {
    delete diagnostics;
    return nullptr;
  }

  PyObject *py_diag = wrap_diagnostics_capsule(diagnostics);
  if (!py_diag) {
    Py_DECREF(py_main);
    return nullptr;
  }

  PyObject *py_registry = PyUnicode_FromStringAndSize(
      registry_json.data(), static_cast<Py_ssize_t>(registry_json.size()));
  if (!py_registry) {
    Py_DECREF(py_diag);
    Py_DECREF(py_main);
    return nullptr;
  }

  PyObject *py_drifts = PyUnicode_FromStringAndSize(
      drifts_json.data(), static_cast<Py_ssize_t>(drifts_json.size()));
  if (!py_drifts) {
    Py_DECREF(py_registry);
    Py_DECREF(py_diag);
    Py_DECREF(py_main);
    return nullptr;
  }

  PyObject *py_timestamp = PyUnicode_FromStringAndSize(
      conversion_timestamp.data(),
      static_cast<Py_ssize_t>(conversion_timestamp.size()));
  if (!py_timestamp) {
    Py_DECREF(py_drifts);
    Py_DECREF(py_registry);
    Py_DECREF(py_diag);
    Py_DECREF(py_main);
    return nullptr;
  }

  PyObject *tup = PyTuple_New(6);
  if (!tup) {
    Py_DECREF(py_timestamp);
    Py_DECREF(py_drifts);
    Py_DECREF(py_registry);
    Py_DECREF(py_diag);
    Py_DECREF(py_main);
    return nullptr;
  }
  if (!tuple_set_item_steal(tup, 0, py_main)) {
    Py_DECREF(py_timestamp);
    Py_DECREF(tup);
    Py_DECREF(py_drifts);
    Py_DECREF(py_registry);
    Py_DECREF(py_diag);
    return nullptr;
  }
  if (!tuple_set_item_steal(tup, 1, py_diag)) {
    Py_DECREF(py_timestamp);
    Py_DECREF(tup);
    Py_DECREF(py_drifts);
    Py_DECREF(py_registry);
    return nullptr;
  }
  if (!tuple_set_item_steal(tup, 2, py_registry)) {
    Py_DECREF(py_timestamp);
    Py_DECREF(tup);
    Py_DECREF(py_drifts);
    return nullptr;
  }
  if (!tuple_set_item_steal(tup, 3, py_drifts)) {
    Py_DECREF(py_timestamp);
    Py_DECREF(tup);
    return nullptr;
  }
  if (!tuple_set_item_steal(tup, 4, py_timestamp)) {
    Py_DECREF(tup);
    return nullptr;
  }
  PyObject *py_state = native_registry_state ? native_registry_state : Py_None;
  Py_INCREF(py_state);
  if (!tuple_set_item_steal(tup, 5, py_state)) {
    Py_DECREF(py_state);
    Py_DECREF(tup);
    return nullptr;
  }
  return tup;
}

} // namespace core_abi3_internal

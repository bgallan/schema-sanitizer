/*
 * Python ABI3 sink helper routines.
 *
 * This file owns result packing and cleanup helpers shared by normal and
 * registry-backed sink wrapper entry points.
 */
#include "internal/abi/core_abi3_internal.hh"

#include "internal/abi/schema_sanitizer_c_internal.hh"

namespace core_abi3_internal {
namespace {

// Wraps optional stream.
PyObject *wrap_optional_stream(PyObject *keepalive, ArrowArrayStream *stream) {
  if (!stream) {
    Py_INCREF(Py_None);
    return Py_None;
  }
  return wrap_stream_capsule_with_keepalive(keepalive, stream);
}

} // namespace

// Releases native stream and diagnostics handles after a failed sink call.
void release_sink_outputs(ArrowArrayStream *main_stream,
                          schema_sanitizer_diagnostics *diagnostics) {
  schema_sanitizer_stream_free(main_stream);
  schema_sanitizer_diagnostics_free(diagnostics);
}

// Packs a normal sink result into the Python ABI tuple shape.
PyObject *
pack_stream_and_diagnostics(PyObject *keepalive, ArrowArrayStream *main_stream,
                            schema_sanitizer_diagnostics *diagnostics) {
  PyObject *py_main = wrap_optional_stream(keepalive, main_stream);
  if (!py_main) {
    schema_sanitizer_diagnostics_free(diagnostics);
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

// Packs a registry-backed sink result into the Python ABI tuple shape.
PyObject *pack_registry_stream_result(PyObject *keepalive,
                                      ArrowArrayStream *main_stream,
                                      schema_sanitizer_diagnostics *diagnostics,
                                      char *registry_json, char *drifts_json,
                                      char *conversion_timestamp) {
  PyObject *py_main = wrap_optional_stream(keepalive, main_stream);
  if (!py_main) {
    schema_sanitizer_diagnostics_free(diagnostics);
    schema_sanitizer_free_string(registry_json);
    schema_sanitizer_free_string(drifts_json);
    schema_sanitizer_free_string(conversion_timestamp);
    return nullptr;
  }

  PyObject *py_diag = wrap_diagnostics_capsule(diagnostics);
  if (!py_diag) {
    Py_DECREF(py_main);
    schema_sanitizer_free_string(registry_json);
    schema_sanitizer_free_string(drifts_json);
    schema_sanitizer_free_string(conversion_timestamp);
    return nullptr;
  }

  PyObject *py_registry =
      PyUnicode_FromString(registry_json ? registry_json : "{}");
  schema_sanitizer_free_string(registry_json);
  if (!py_registry) {
    Py_DECREF(py_diag);
    Py_DECREF(py_main);
    schema_sanitizer_free_string(drifts_json);
    schema_sanitizer_free_string(conversion_timestamp);
    return nullptr;
  }

  PyObject *py_drifts = PyUnicode_FromString(drifts_json ? drifts_json : "[]");
  schema_sanitizer_free_string(drifts_json);
  if (!py_drifts) {
    Py_DECREF(py_registry);
    Py_DECREF(py_diag);
    Py_DECREF(py_main);
    schema_sanitizer_free_string(conversion_timestamp);
    return nullptr;
  }

  PyObject *py_timestamp =
      PyUnicode_FromString(conversion_timestamp ? conversion_timestamp : "");
  schema_sanitizer_free_string(conversion_timestamp);
  if (!py_timestamp) {
    Py_DECREF(py_drifts);
    Py_DECREF(py_registry);
    Py_DECREF(py_diag);
    Py_DECREF(py_main);
    return nullptr;
  }

  PyObject *tup = PyTuple_New(5);
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
  return tup;
}

} // namespace core_abi3_internal

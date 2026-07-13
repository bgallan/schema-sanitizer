/*
 * Python ABI3 error, path, buffer, and signal adapters.
 *
 * This file provides shared error translation and Python value adapters used by
 * ABI3 binding entry points.
 */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

void raise_status_error(int status, char *err) {
  if (PyErr_Occurred()) {
    if (err) {
      schema_sanitizer_free_string(err);
    }
    return;
  }

  // Prefer the detailed error string.
  const char *msg = (err && err[0] != 0) ? err : nullptr;

  if (msg) {
    PyErr_Format(PyExc_RuntimeError, "schema-sanitizer error (status=%d): %s",
                 status, msg);
  } else {
    PyErr_Format(PyExc_RuntimeError, "schema-sanitizer error (status=%d)",
                 status);
  }

  if (err) {
    schema_sanitizer_free_string(err);
  }
}

// Convert a Python path-like object to filesystem-encoded bytes.
// Returns a new reference to a PyBytes object on success.
PyObject *fsencode_path(PyObject *obj) {
  // PyUnicode_FSConverter handles str, bytes, and os.PathLike.
  PyObject *out = nullptr;
  if (PyUnicode_FSConverter(obj, static_cast<void *>(&out)) == 0) {
    return nullptr;
  }
  // out is a bytes object.
  return out;
}

// Extract a (ptr,len) view from a bytes-or-str object.
//
// Accepted surface:
// - bytes: used as-is
// - str: UTF-8 view
//
// Returns 1 on success, 0 on error (with an exception set).
int bytes_or_str_view(PyObject *obj, const char **out_ptr,
                      Py_ssize_t *out_len) {
  if (!out_ptr || !out_len) {
    PyErr_SetString(PyExc_RuntimeError, "internal error: null out param");
    return 0;
  }
  *out_ptr = nullptr;
  *out_len = 0;

  if (PyBytes_Check(obj)) {
    char *data = nullptr;
    Py_ssize_t n = 0;
    if (PyBytes_AsStringAndSize(obj, &data, &n) != 0 || !data) {
      return 0;
    }
    *out_ptr = data;
    *out_len = n;
    return 1;
  }
  if (PyUnicode_Check(obj)) {
    Py_ssize_t n = 0;
    const char *data = PyUnicode_AsUTF8AndSize(obj, &n);
    if (!data)
      return 0;
    *out_ptr = data;
    *out_len = n;
    return 1;
  }

  PyErr_SetString(PyExc_TypeError, "expected bytes or str");
  return 0;
}

// Set tuple item using limited-API function and steal the incoming reference
// on success. On failure, decref item and leave an exception set.
int tuple_set_item_steal(PyObject *tup, Py_ssize_t index, PyObject *item) {
  if (!item)
    return 0;
  if (PyTuple_SetItem(tup, index, item) != 0) {
    Py_DECREF(item);
    return 0;
  }
  return 1;
}

// Read-only bytes-like view for options bytes.
//
// This intentionally materializes a bytes object via PyBytes_FromObject so the
// caller owns a stable view independent from the source object.
//
// Returns 1 on success, 0 on error (with an exception set). The caller owns
// a new reference in out_owner and must Py_DECREF it when done.
int readonly_buffer_view(PyObject *obj, const std::uint8_t **out_ptr,
                         Py_ssize_t *out_len, PyObject **out_owner) {
  if (!out_ptr || !out_len || !out_owner) {
    PyErr_SetString(PyExc_RuntimeError, "internal error: null out param");
    return 0;
  }
  *out_ptr = nullptr;
  *out_len = 0;
  *out_owner = nullptr;

  PyObject *bytes_obj = PyBytes_FromObject(obj);
  if (!bytes_obj) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_TypeError, "expected a bytes-like object");
    }
    return 0;
  }

  char *data = nullptr;
  Py_ssize_t n = 0;
  if (PyBytes_AsStringAndSize(bytes_obj, &data, &n) != 0 || !data || n < 0) {
    Py_DECREF(bytes_obj);
    PyErr_SetString(PyExc_TypeError, "invalid bytes-like object");
    return 0;
  }

  *out_owner = bytes_obj;
  *out_ptr = reinterpret_cast<const std::uint8_t *>(data);
  *out_len = n;
  return 1;
}

bool check_python_signals() { return PyErr_CheckSignals() == 0; }

void install_python_interrupt_check(schema_sanitizer_context *ctx) {
  if (!ctx || !ctx->ctx) {
    return;
  }
  ctx->ctx->set_interrupt_check([]() -> sanitize::Status {
    PyGILState_STATE gil = PyGILState_Ensure();
    const int rc = PyErr_CheckSignals();
    PyGILState_Release(gil);
    if (rc != 0) {
      return sanitize::Status::Cancelled("Python signal received");
    }
    return sanitize::Status::OK();
  });
}

} // namespace core_abi3_internal

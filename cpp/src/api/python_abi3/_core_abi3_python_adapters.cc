/*
 * Implements Python ABI3 error, path, buffer, and signal adapters.
 *
 * This file provides shared error translation and Python value adapters used by
 * ABI3 binding entry points.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "internal/abi/python_abi3/native_state.hh"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

/// Translates a failed native status into the corresponding Python exception.
void raise_status_error(const sanitize::Status &status) {
  if (PyErr_Occurred()) {
    return;
  }
  int code = 3;
  switch (status.code()) {
  case sanitize::StatusCode::kOK:
    code = 0;
    break;
  case sanitize::StatusCode::kInvalid:
    code = 1;
    break;
  case sanitize::StatusCode::kOutOfMemory:
    code = 2;
    break;
  case sanitize::StatusCode::kCancelled:
  case sanitize::StatusCode::kIOError:
  case sanitize::StatusCode::kNotImplemented:
    break;
  }
  const auto message = status.ToString();
  PyErr_Format(PyExc_RuntimeError, "schema-sanitizer error (status=%d): %s",
               code, message.c_str());
}

/// Converts a Python path-like object to filesystem-encoded bytes.
/// Returns a new reference to a PyBytes object on success.
PyObject *fsencode_path(PyObject *obj) {
  // PyUnicode_FSConverter handles str, bytes, and os.PathLike.
  PyObject *out = nullptr;
  if (PyUnicode_FSConverter(obj, static_cast<void *>(&out)) == 0) {
    return nullptr;
  }
  // out is a bytes object.
  return out;
}

/// Exposes bytes as-is or a string's UTF-8 storage through a pointer and
/// length. Returns 1 on success or 0 with a Python exception set.
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

/// Sets a tuple item through the limited API and steals its reference on
/// success. On failure, decrements the item reference and leaves a Python
/// exception set.
int tuple_set_item_steal(PyObject *tup, Py_ssize_t index, PyObject *item) {
  if (!item)
    return 0;
  if (PyTuple_SetItem(tup, index, item) != 0) {
    Py_DECREF(item);
    return 0;
  }
  return 1;
}

/// Materializes a bytes-like object and exposes its stable read-only storage.
/// Returns 1 with a new reference in `out_owner`, or 0 with an exception set.
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

/// Polls Python signal state from native work and translates interruptions into
/// status.
bool check_python_signals() { return PyErr_CheckSignals() == 0; }

/// Attaches Python signal polling to the execution context used by native
/// operations.
void install_python_interrupt_check(NativeContext *ctx) {
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

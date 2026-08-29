// Implements ABI3 Arrow stream capsule lifecycle helpers. They centralize
// capsule ownership and callback release rules for every Python-facing stream.

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"

#include <cstring>

namespace core_abi3_internal {
namespace {

constexpr const char *kArrowStreamCapsuleName = "arrow_array_stream";
constexpr const char *kArrowSchemaCapsuleName = "arrow_schema";

/// Returns an interned Python method name used by Arrow protocol calls.
PyObject *interned_method_name(const char *name) noexcept {
  static PyObject *arrow_stream_name = nullptr;
  static PyObject *arrow_schema_name = nullptr;
  PyObject **slot = (std::strcmp(name, "__arrow_c_stream__") == 0)
                        ? &arrow_stream_name
                        : &arrow_schema_name;
  if (!*slot) {
    *slot = PyUnicode_InternFromString(name);
  }
  return *slot;
}

/// Calls a no-argument protocol method with an interned attribute name.
PyObject *call_noarg_protocol(PyObject *obj, const char *name) {
  PyObject *method_name = interned_method_name(name);
  if (!method_name) {
    return nullptr;
  }
  PyObject *method = PyObject_GetAttr(obj, method_name);
  if (!method) {
    return nullptr;
  }
  PyObject *result = PyObject_CallObject(method, nullptr);
  Py_DECREF(method);
  return result;
}

} // namespace

const char *arrow_stream_capsule_name() noexcept {
  return kArrowStreamCapsuleName;
}

/// Releases a Python reference while the GIL is held.
void decref_with_gil(PyObject *obj) noexcept {
  if (!obj) {
    return;
  }
  PyGILState_STATE gil = PyGILState_Ensure();
  Py_DECREF(obj);
  PyGILState_Release(gil);
}

/// Acquires a Python Arrow C stream capsule and extracts its native pointer.
bool acquire_arrow_stream(PyObject *stream_obj, PyObject **capsule_out,
                          ArrowArrayStream **inner_out) {
  if (!capsule_out || !inner_out) {
    PyErr_SetString(PyExc_SystemError, "invalid Arrow stream output pointer");
    return false;
  }
  *capsule_out = nullptr;
  *inner_out = nullptr;
  PyObject *capsule = call_noarg_protocol(stream_obj, "__arrow_c_stream__");
  if (!capsule) {
    return false;
  }
  auto *inner = static_cast<ArrowArrayStream *>(
      PyCapsule_GetPointer(capsule, kArrowStreamCapsuleName));
  if (!inner) {
    Py_DECREF(capsule);
    return false;
  }
  *capsule_out = capsule;
  *inner_out = inner;
  return true;
}

/// Acquires a Python Arrow C schema capsule and extracts its native pointer.
bool acquire_arrow_schema(PyObject *schema_obj, PyObject **capsule_out,
                          ArrowSchema **schema_out) {
  if (!capsule_out || !schema_out) {
    PyErr_SetString(PyExc_SystemError, "invalid Arrow schema output pointer");
    return false;
  }
  *capsule_out = nullptr;
  *schema_out = nullptr;
  PyObject *capsule = call_noarg_protocol(schema_obj, "__arrow_c_schema__");
  if (!capsule) {
    return false;
  }
  auto *schema = static_cast<ArrowSchema *>(
      PyCapsule_GetPointer(capsule, kArrowSchemaCapsuleName));
  if (!schema) {
    Py_DECREF(capsule);
    return false;
  }
  *capsule_out = capsule;
  *schema_out = schema;
  return true;
}

/// Clears keepalive references owned by a wrapper Arrow stream.
void close_arrow_stream_keepalive(ArrowArrayStream **inner,
                                  PyObject **stream_obj,
                                  PyObject **stream_capsule,
                                  bool *closed) noexcept {
  if (!closed || *closed) {
    return;
  }
  *closed = true;
  if (stream_capsule) {
    decref_with_gil(*stream_capsule);
    *stream_capsule = nullptr;
  }
  if (stream_obj) {
    decref_with_gil(*stream_obj);
    *stream_obj = nullptr;
  }
  if (inner) {
    *inner = nullptr;
  }
}

} // namespace core_abi3_internal

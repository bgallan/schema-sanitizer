/*
 * Python ABI3 context and diagnostics wrappers.
 *
 * This file exposes execution-context and diagnostics entry points for
 * the ABI3 module.
 */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

namespace core_abi3_internal {

// ---- ExecutionContext ------------------------------------------------------

PyObject *py_context_new(PyObject *, PyObject *) {
  schema_sanitizer_context *ctx = nullptr;
  char *err = nullptr;
  int st = schema_sanitizer_context_new(&ctx, &err);
  if (st != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(st, err);
    return nullptr;
  }

  install_python_interrupt_check(ctx);
  return wrap_context_capsule(ctx);
}

PyObject *py_context_memory_stats_json(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:context_memory_stats_json", &ctx_obj))
    return nullptr;
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;

  char *out_json = nullptr;
  char *err = nullptr;
  int st = schema_sanitizer_context_memory_stats_json(ctx, &out_json, &err);
  if (st != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(st, err);
    return nullptr;
  }
  PyObject *py = PyUnicode_FromString(out_json ? out_json : "{}");
  if (out_json)
    schema_sanitizer_free_string(out_json);
  return py;
}

PyObject *py_diagnostics_json(PyObject *, PyObject *args) {
  PyObject *diagnostics_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:diagnostics_json", &diagnostics_obj))
    return nullptr;
  auto *diagnostics = unwrap_diagnostics(diagnostics_obj);
  if (!diagnostics)
    return nullptr;

  char *out_json = nullptr;
  char *err = nullptr;
  int st = schema_sanitizer_diagnostics_json(diagnostics, &out_json, &err);
  if (st != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(st, err);
    return nullptr;
  }
  PyObject *py = PyUnicode_FromString(out_json ? out_json : "{}");
  if (out_json)
    schema_sanitizer_free_string(out_json);
  return py;
}

} // namespace core_abi3_internal

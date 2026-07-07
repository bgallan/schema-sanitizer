/*
 * Python ABI3 context/options wrappers.
 *
 * This file exposes execution-context and option-validation entry points for
 * the ABI3 module.
 */
#include "internal/abi/core_abi3_internal.hh"

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

// ---- options ----------------------------------------------------------------

PyObject *py_options_prepare_bytes(PyObject *, PyObject *args) {
  PyObject *bytes_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:options_prepare_bytes", &bytes_obj))
    return nullptr;

  PyObject *view_owner = nullptr;
  const std::uint8_t *p = nullptr;
  Py_ssize_t n = 0;
  if (!readonly_buffer_view(bytes_obj, &p, &n, &view_owner)) {
    return nullptr;
  }

  schema_sanitizer_prepared_options *out = nullptr;
  char *err = nullptr;
  int st = schema_sanitizer_options_prepare_bytes(
      p, static_cast<std::size_t>(n), &out, &err);
  Py_DECREF(view_owner);

  if (st != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(st, err);
    return nullptr;
  }

  return wrap_prepared_options_capsule(out);
}

} // namespace core_abi3_internal

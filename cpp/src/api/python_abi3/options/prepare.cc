// Validates serialized options and returns prepared native state.

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "sanitize/options/options.hh"

#include <memory>
#include <new>
#include <utility>

namespace core_abi3_internal {

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

} // namespace core_abi3_internal

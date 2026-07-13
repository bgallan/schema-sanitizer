// Validates serialized options and returns prepared native state.

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

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

} // namespace core_abi3_internal

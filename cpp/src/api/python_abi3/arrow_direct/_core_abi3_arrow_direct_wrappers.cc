// Python ABI3 wrappers for direct Arrow schema helpers.

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"

#include "api/python_abi3/arrow_direct/schema/payload.hh"
#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"

#include <memory>
#include <string>

#include "nanoarrow/nanoarrow.h"

namespace core_abi3_internal {
namespace {

ArrowSchema *schema_from_pyarrow_object(PyObject *schema_obj,
                                        PyObject **capsule_out) {
  ArrowSchema *schema = nullptr;
  if (!acquire_arrow_schema(schema_obj, capsule_out, &schema)) {
    PyErr_SetString(PyExc_TypeError, "expected a PyArrow schema");
    return nullptr;
  }
  return schema;
}

} // namespace

PyObject *py_arrow_direct_schema_supported(PyObject *, PyObject *args) {
  PyObject *schema_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:arrow_direct_schema_supported", &schema_obj)) {
    return nullptr;
  }
  PyObject *capsule = nullptr;
  ArrowSchema *schema = schema_from_pyarrow_object(schema_obj, &capsule);
  if (!schema) {
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> capsule_owner(capsule,
                                                                Py_DECREF);
  return PyBool_FromLong(arrow_direct_schema_is_supported(*schema) ? 1 : 0);
}

PyObject *py_arrow_schema_contract_payload(PyObject *, PyObject *args) {
  PyObject *schema_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:arrow_schema_contract_payload", &schema_obj)) {
    return nullptr;
  }
  PyObject *capsule = nullptr;
  ArrowSchema *schema = schema_from_pyarrow_object(schema_obj, &capsule);
  if (!schema) {
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> capsule_owner(capsule,
                                                                Py_DECREF);
  auto payload = logical_schema_payload_from_arrow_schema(
      schema, ArrowDirectOptions{.timestamp_precision = "TIMESTAMP_MICROS"});
  if (!payload.ok()) {
    PyErr_SetString(PyExc_TypeError, payload.status().message().c_str());
    return nullptr;
  }
  std::string value = std::move(payload).ValueOrDie();
  return PyBytes_FromStringAndSize(value.data(),
                                   static_cast<Py_ssize_t>(value.size()));
}

} // namespace core_abi3_internal

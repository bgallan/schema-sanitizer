/*
 * Implements ABI3 JSONL Arrow batch extraction helpers.
 *
 * This translation unit owns the PyArrow `__arrow_c_array__` protocol handling
 * used by both single-batch and multi-batch JSONL byte encoders.
 */

#include "api/python_abi3/json/_core_abi3_jsonl_output_parts.hh"

#include "nanoarrow/nanoarrow.h"

namespace core_abi3_internal::jsonl_output {
namespace {

/// Returns the interned Python name for PyArrow batch C-array export.
PyObject *interned_arrow_c_array_name() noexcept {
  static PyObject *name = nullptr;
  if (!name) {
    name = PyUnicode_InternFromString("__arrow_c_array__");
  }
  return name;
}

/// Calls PyArrow's no-argument __arrow_c_array__ protocol method.
PyObject *call_arrow_c_array(PyObject *batch_obj) {
  PyObject *name = interned_arrow_c_array_name();
  if (!name) {
    return nullptr;
  }
  PyObject *method = PyObject_GetAttr(batch_obj, name);
  if (!method) {
    return nullptr;
  }
  PyObject *result = PyObject_CallObject(method, nullptr);
  Py_DECREF(method);
  return result;
}

} // namespace

sanitize::Status batch_capsules(PyObject *batch_obj, ArrowSchema **schema_out,
                                ArrowArray **array_out, PyObject **owner_out) {
  PyObject *exported = call_arrow_c_array(batch_obj);
  if (!exported) {
    return sanitize::Status::Invalid(
        "jsonl_batch_bytes expected a PyArrow record batch");
  }
  if (!PyTuple_Check(exported) || PyTuple_Size(exported) != 2) {
    Py_DECREF(exported);
    return sanitize::Status::Invalid(
        "jsonl_batch_bytes expected a PyArrow record batch");
  }
  PyObject *schema_capsule = PyTuple_GetItem(exported, 0);
  PyObject *array_capsule = PyTuple_GetItem(exported, 1);
  auto *schema = static_cast<ArrowSchema *>(
      PyCapsule_GetPointer(schema_capsule, kArrowSchemaCapsuleName));
  if (!schema) {
    Py_DECREF(exported);
    return sanitize::Status::Invalid(
        "jsonl_batch_bytes received an invalid Arrow schema");
  }
  auto *array = static_cast<ArrowArray *>(
      PyCapsule_GetPointer(array_capsule, kArrowArrayCapsuleName));
  if (!array) {
    Py_DECREF(exported);
    return sanitize::Status::Invalid(
        "jsonl_batch_bytes received an invalid Arrow array");
  }
  *schema_out = schema;
  *array_out = array;
  *owner_out = exported;
  return sanitize::Status::OK();
}

} // namespace core_abi3_internal::jsonl_output

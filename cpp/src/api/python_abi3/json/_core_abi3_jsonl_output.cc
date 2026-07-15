/*
 * ABI3 JSONL output wrapper helpers.
 *
 * This file parses Python-facing inputs and delegates concrete output handling
 * to the focused JSONL adapter translation unit.
 */
#include "api/python_abi3/json/_core_abi3_jsonl_output.hh"

#include "api/python_abi3/json/_core_abi3_jsonl_output_parts.hh"
#include "api/python_abi3/json/output_adapters/api.hh"
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <memory>
#include <string>

#include "nanoarrow/nanoarrow.h"

namespace core_abi3_internal {

sanitize::Result<sanitize::internal::jsonl_stream_writer::WriteStats>
jsonl_write_stream_to_output(PyObject *stream_obj, PyObject *output_obj,
                             std::int64_t memory_limit_bytes) {
  if (PyUnicode_Check(output_obj)) {
    Py_ssize_t path_len = 0;
    const char *path = PyUnicode_AsUTF8AndSize(output_obj, &path_len);
    if (!path) {
      return sanitize::Status::Invalid("output path must be a string");
    }
    return jsonl_write_stream_to_path(
        stream_obj, std::string(path, static_cast<std::size_t>(path_len)),
        memory_limit_bytes);
  }

  return jsonl_write_stream_to_python(stream_obj, output_obj,
                                      memory_limit_bytes);
}

sanitize::Status jsonl_append_batch_bytes(PyObject *batch_obj,
                                          std::string *out,
                                          std::int64_t memory_limit_bytes) {
  ArrowSchema *schema = nullptr;
  ArrowArray *array = nullptr;
  PyObject *owner = nullptr;
  auto status =
      jsonl_output::batch_capsules(batch_obj, &schema, &array, &owner);
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> owner_guard(owner, Py_DECREF);
  if (!status.ok()) {
    return status;
  }

  return jsonl_write_batch_to_string(*schema, *array, out,
                                    memory_limit_bytes);
}

sanitize::Status jsonl_append_batches_bytes(PyObject *batches_obj,
                                            std::string *out,
                                            std::int64_t memory_limit_bytes) {
  PyObject *batches =
      PySequence_Fast(batches_obj, "jsonl_batches_bytes expects a sequence");
  if (!batches) {
    return sanitize::Status::Invalid(
        "jsonl_batches_bytes expects a sequence of record batches");
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> batches_guard(batches,
                                                                Py_DECREF);
  const Py_ssize_t size = PySequence_Size(batches);
  for (Py_ssize_t i = 0; i < size; ++i) {
    bool borrowed = false;
    PyObject *item = sequence_item_borrowed_or_new(batches, i, &borrowed);
    if (!item) {
      return sanitize::Status::Invalid(
          "jsonl_batches_bytes failed reading a batch item");
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> item_guard(
        borrowed ? nullptr : item, Py_DECREF);
    auto status = jsonl_append_batch_bytes(item, out, memory_limit_bytes);
    if (!status.ok()) {
      return status;
    }
  }
  return sanitize::Status::OK();
}

} // namespace core_abi3_internal

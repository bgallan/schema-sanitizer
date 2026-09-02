/*
 * Implements Python ABI3 serialization of row sequences to JSONL.
 *
 * The Python-row analytical input route still ingests JSON Lines internally,
 * but this wrapper batches Python-object serialization in native code so the
 * hot loop does not cross the Python/native boundary once per source row.
 */

#include "api/python_abi3/json/_core_abi3_json_tools.hh"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>

namespace core_abi3_internal {
namespace {

/// Validates and appends one dictionary row as a complete JSONL record.
bool append_python_row(std::string *out, PyObject *item, Py_ssize_t row_index) {
  if (!PyDict_Check(item)) {
    PyErr_Format(PyExc_TypeError,
                 "source='python' only supports iterables of dict rows; row "
                 "%zd is not a dict",
                 row_index);
    return false;
  }
  const auto status = append_python_json_value(*out, item, 0);
  if (!status.ok()) {
    PyErr_SetString(PyExc_ValueError, status.message().c_str());
    return false;
  }
  out->push_back('\n');
  return true;
}

/// Packages iterator batch as a Python result while transferring native
/// ownership safely.
PyObject *pack_iterator_batch(std::string out, Py_ssize_t next_index,
                              bool exhausted) {
  PyObject *tuple = PyTuple_New(3);
  if (!tuple) {
    return nullptr;
  }
  if (!tuple_set_item_steal(
          tuple, 0,
          PyBytes_FromStringAndSize(out.data(),
                                    static_cast<Py_ssize_t>(out.size()))) ||
      !tuple_set_item_steal(tuple, 1, PyLong_FromSsize_t(next_index)) ||
      !tuple_set_item_steal(tuple, 2, PyBool_FromLong(exhausted ? 1 : 0))) {
    Py_DECREF(tuple);
    return nullptr;
  }
  return tuple;
}

} // namespace

/// Encodes a bounded slice of a Python row sequence and returns bytes plus the
/// next index.
PyObject *py_python_rows_jsonl_bytes(PyObject *, PyObject *args) {
  PyObject *rows = nullptr;
  Py_ssize_t start_index = 0;
  Py_ssize_t target_bytes = 0;
  Py_ssize_t max_rows = std::numeric_limits<Py_ssize_t>::max();
  if (!PyArg_ParseTuple(args, "Onn|n:python_rows_jsonl_bytes", &rows,
                        &start_index, &target_bytes, &max_rows)) {
    return nullptr;
  }
  if (start_index < 0) {
    PyErr_SetString(PyExc_ValueError, "start index must be non-negative");
    return nullptr;
  }

  const Py_ssize_t row_count = PySequence_Size(rows);
  if (row_count < 0) {
    PyErr_Clear();
    PyErr_SetString(PyExc_TypeError, "rows must be a sequence");
    return nullptr;
  }
  if (start_index > row_count) {
    start_index = row_count;
  }
  if (max_rows <= 0) {
    PyErr_SetString(PyExc_ValueError, "max rows must be positive");
    return nullptr;
  }
  const Py_ssize_t remaining = row_count - start_index;
  const Py_ssize_t bounded_rows = std::min(max_rows, remaining);
  const Py_ssize_t stop_index = start_index + bounded_rows;

  const std::size_t reserve_bytes =
      target_bytes > 0
          ? std::min<std::size_t>(static_cast<std::size_t>(target_bytes),
                                  static_cast<std::size_t>(1 << 20))
          : static_cast<std::size_t>(4096);
  std::string out;
  out.reserve(reserve_bytes);

  Py_ssize_t next_index = start_index;
  std::size_t interrupt_countdown = 0;
  while (next_index < stop_index) {
    if ((interrupt_countdown++ & std::size_t{1023}) == 0 &&
        !check_python_signals()) {
      return nullptr;
    }
    bool borrowed = false;
    PyObject *item = sequence_item_borrowed_or_new(rows, next_index, &borrowed);
    if (!item) {
      PyErr_Clear();
      PyErr_SetString(PyExc_TypeError, "failed reading row sequence item");
      return nullptr;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> item_owner(
        borrowed ? nullptr : item, Py_DECREF);
    if (!append_python_row(&out, item, next_index)) {
      return nullptr;
    }
    ++next_index;

    if (target_bytes > 0 &&
        out.size() >= static_cast<std::size_t>(target_bytes)) {
      break;
    }
  }

  PyObject *tuple = PyTuple_New(2);
  if (!tuple) {
    return nullptr;
  }
  if (!tuple_set_item_steal(
          tuple, 0,
          PyBytes_FromStringAndSize(out.data(),
                                    static_cast<Py_ssize_t>(out.size())))) {
    Py_DECREF(tuple);
    return nullptr;
  }
  if (!tuple_set_item_steal(tuple, 1, PyLong_FromSsize_t(next_index))) {
    Py_DECREF(tuple);
    return nullptr;
  }
  return tuple;
}

/// Consumes a bounded iterator chunk and returns JSONL bytes, position, and
/// exhaustion.
PyObject *py_python_iter_rows_jsonl_bytes(PyObject *, PyObject *args) {
  PyObject *iterator = nullptr;
  Py_ssize_t start_index = 0;
  Py_ssize_t target_bytes = 0;
  Py_ssize_t max_rows = 0;
  if (!PyArg_ParseTuple(args, "Onnn:python_iter_rows_jsonl_bytes", &iterator,
                        &start_index, &target_bytes, &max_rows)) {
    return nullptr;
  }
  if (start_index < 0) {
    PyErr_SetString(PyExc_ValueError, "start index must be non-negative");
    return nullptr;
  }
  if (max_rows <= 0) {
    PyErr_SetString(PyExc_ValueError, "max rows must be positive");
    return nullptr;
  }

  const std::size_t reserve_bytes =
      target_bytes > 0
          ? std::min<std::size_t>(static_cast<std::size_t>(target_bytes),
                                  static_cast<std::size_t>(1 << 20))
          : static_cast<std::size_t>(4096);
  std::string out;
  out.reserve(reserve_bytes);

  Py_ssize_t next_index = start_index;
  Py_ssize_t encoded_rows = 0;
  bool exhausted = false;
  std::size_t interrupt_countdown = 0;
  while (encoded_rows < max_rows) {
    if ((interrupt_countdown++ & std::size_t{1023}) == 0 &&
        !check_python_signals()) {
      return nullptr;
    }
    PyObject *item = PyIter_Next(iterator);
    if (!item) {
      if (PyErr_Occurred()) {
        return nullptr;
      }
      exhausted = true;
      break;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> item_owner(item, Py_DECREF);
    if (!append_python_row(&out, item, next_index)) {
      return nullptr;
    }
    ++next_index;
    ++encoded_rows;
    if (target_bytes > 0 &&
        out.size() >= static_cast<std::size_t>(target_bytes)) {
      break;
    }
  }
  return pack_iterator_batch(std::move(out), next_index, exhausted);
}

} // namespace core_abi3_internal

/*
 * Python ABI3 helpers for row-sequence JSONL serialization.
 *
 * The Python-row analytical input route still ingests JSON Lines internally,
 * but this wrapper batches Python-object serialization in native code so the
 * hot loop does not cross the Python/native boundary once per source row.
 */
#include "api/python_abi3/_core_abi3_json_tools.hh"

#include <algorithm>
#include <memory>
#include <string>

namespace core_abi3_internal {

PyObject *py_python_rows_jsonl_bytes(PyObject *, PyObject *args) {
  PyObject *rows = nullptr;
  Py_ssize_t start_index = 0;
  Py_ssize_t target_bytes = 0;
  if (!PyArg_ParseTuple(args, "Onn:python_rows_jsonl_bytes", &rows,
                        &start_index, &target_bytes)) {
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

  const std::size_t reserve_bytes =
      target_bytes > 0
          ? std::min<std::size_t>(static_cast<std::size_t>(target_bytes),
                                  static_cast<std::size_t>(1 << 20))
          : static_cast<std::size_t>(4096);
  std::string out;
  out.reserve(reserve_bytes);

  Py_ssize_t next_index = start_index;
  std::size_t interrupt_countdown = 0;
  while (next_index < row_count) {
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

    const auto status = append_python_json_value(out, item, 0);
    if (!status.ok()) {
      PyErr_SetString(PyExc_TypeError, status.message().c_str());
      return nullptr;
    }
    out.push_back('\n');
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

} // namespace core_abi3_internal

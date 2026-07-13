// Parses scalar, timestamp, and row-span metadata columns from Python objects.

#include "api/python_abi3/metadata/columns/api.hh"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace core_abi3_internal {
namespace {

bool py_unicode_to_string(PyObject *obj, const char *name, std::string *out) {
  if (!PyUnicode_Check(obj)) {
    PyErr_Format(PyExc_TypeError, "%s must contain string keys and values",
                 name);
    return false;
  }
  Py_ssize_t size = 0;
  const char *data = PyUnicode_AsUTF8AndSize(obj, &size);
  if (!data) {
    return false;
  }
  out->assign(data, static_cast<std::size_t>(size));
  return true;
}

bool py_value_to_metadata_column(PyObject *value, const char *placement_name,
                                 MetadataColumn *column) {
  if (value == Py_None) {
    column->is_null = true;
    return true;
  }
  if (PyUnicode_Check(value)) {
    return py_unicode_to_string(value, "metadata columns", &column->value);
  }
  PyErr_Format(PyExc_TypeError,
               "%s ETL metadata values must be strings or None",
               placement_name);
  return false;
}

bool append_utf8_columns_from_dict(PyObject *dict,
                                   MetadataColumnPlacement placement,
                                   const char *placement_name,
                                   std::vector<MetadataColumn> *out) {
  if (!PyDict_Check(dict)) {
    PyErr_SetString(PyExc_TypeError, "metadata columns must be dictionaries");
    return false;
  }
  out->reserve(out->size() + static_cast<std::size_t>(PyDict_Size(dict)));
  PyObject *key = nullptr;
  PyObject *value = nullptr;
  Py_ssize_t pos = 0;
  while (PyDict_Next(dict, &pos, &key, &value)) {
    MetadataColumn column;
    column.placement = placement;
    if (!py_unicode_to_string(key, "metadata columns", &column.name) ||
        !py_value_to_metadata_column(value, placement_name, &column)) {
      return false;
    }
    out->push_back(std::move(column));
  }
  return true;
}

bool py_value_to_metadata_span(PyObject *value, MetadataSpan *span) {
  if (value == Py_None) {
    span->is_null = true;
    return true;
  }
  if (PyUnicode_Check(value)) {
    return py_unicode_to_string(value, "row-span metadata columns",
                                &span->value);
  }
  PyErr_SetString(PyExc_TypeError,
                  "row-span ETL metadata values must be strings or None");
  return false;
}

bool py_span_to_metadata_span(PyObject *obj, MetadataSpan *span) {
  if (!PySequence_Check(obj) || PyUnicode_Check(obj)) {
    PyErr_SetString(PyExc_TypeError, "row-span entries must be pairs");
    return false;
  }
  const Py_ssize_t size = PySequence_Size(obj);
  if (size < 0) {
    return false;
  }
  if (size != 2) {
    PyErr_SetString(PyExc_ValueError, "row-span entries must be pairs");
    return false;
  }
  PyObject *row_count_obj = PySequence_GetItem(obj, 0);
  if (!row_count_obj) {
    return false;
  }
  const long long row_count = PyLong_AsLongLong(row_count_obj);
  Py_DECREF(row_count_obj);
  if (row_count == -1 && PyErr_Occurred()) {
    return false;
  }
  if (row_count < 0) {
    PyErr_SetString(PyExc_ValueError,
                    "row-span row counts must be non-negative integers");
    return false;
  }
  if (!std::in_range<std::int64_t>(row_count)) {
    PyErr_SetString(PyExc_OverflowError, "row-span row count is too large");
    return false;
  }
  span->row_count = static_cast<std::int64_t>(row_count);
  PyObject *value_obj = PySequence_GetItem(obj, 1);
  if (!value_obj) {
    return false;
  }
  const bool ok = py_value_to_metadata_span(value_obj, span);
  Py_DECREF(value_obj);
  return ok;
}

} // namespace

bool append_first_row_columns_from_dict(PyObject *dict,
                                        std::vector<MetadataColumn> *out) {
  return append_utf8_columns_from_dict(
      dict, MetadataColumnPlacement::FirstRowUtf8, "first-row", out);
}

bool append_all_row_columns_from_dict(PyObject *dict,
                                      std::vector<MetadataColumn> *out) {
  return append_utf8_columns_from_dict(
      dict, MetadataColumnPlacement::AllRowsUtf8, "all-row", out);
}

bool append_row_span_columns_from_dict(PyObject *dict,
                                       std::vector<MetadataColumn> *out) {
  if (!PyDict_Check(dict)) {
    PyErr_SetString(PyExc_TypeError, "row-span columns must be dictionaries");
    return false;
  }
  out->reserve(out->size() + static_cast<std::size_t>(PyDict_Size(dict)));
  PyObject *key = nullptr;
  PyObject *value = nullptr;
  Py_ssize_t pos = 0;
  while (PyDict_Next(dict, &pos, &key, &value)) {
    MetadataColumn column;
    column.placement = MetadataColumnPlacement::RowSpanUtf8;
    if (!py_unicode_to_string(key, "row-span metadata columns", &column.name)) {
      return false;
    }
    if (!PySequence_Check(value) || PyUnicode_Check(value)) {
      PyErr_SetString(PyExc_TypeError,
                      "row-span metadata columns must contain span sequences");
      return false;
    }
    const Py_ssize_t size = PySequence_Size(value);
    if (size < 0) {
      return false;
    }
    column.spans.reserve(static_cast<std::size_t>(size));
    for (Py_ssize_t i = 0; i < size; ++i) {
      PyObject *item = PySequence_GetItem(value, i);
      if (!item) {
        return false;
      }
      MetadataSpan span;
      const bool ok = py_span_to_metadata_span(item, &span);
      Py_DECREF(item);
      if (!ok) {
        return false;
      }
      if (span.row_count > 0) {
        column.spans.push_back(std::move(span));
      }
    }
    out->push_back(std::move(column));
  }
  return true;
}

bool append_timestamp_columns_from_sequence(PyObject *sequence,
                                            std::vector<MetadataColumn> *out) {
  if (!PySequence_Check(sequence) || PyUnicode_Check(sequence)) {
    PyErr_SetString(PyExc_TypeError,
                    "timestamp metadata columns must be a sequence of names");
    return false;
  }
  const Py_ssize_t size = PySequence_Size(sequence);
  if (size < 0) {
    return false;
  }
  out->reserve(out->size() + static_cast<std::size_t>(size));
  for (Py_ssize_t i = 0; i < size; ++i) {
    PyObject *item = PySequence_GetItem(sequence, i);
    if (!item) {
      return false;
    }
    MetadataColumn column;
    column.placement = MetadataColumnPlacement::AllRowsTimestampMicros;
    const bool ok =
        py_unicode_to_string(item, "timestamp metadata columns", &column.name);
    Py_DECREF(item);
    if (!ok) {
      return false;
    }
    out->push_back(std::move(column));
  }
  return true;
}

bool append_registry_metadata_columns(
    PyObject *first_row_columns, PyObject *timestamp_columns,
    std::vector<MetadataColumn> *first_row_out,
    std::vector<MetadataColumn> *timestamp_out) {
  return append_first_row_columns_from_dict(first_row_columns, first_row_out) &&
         append_timestamp_columns_from_sequence(timestamp_columns,
                                                timestamp_out);
}

} // namespace core_abi3_internal

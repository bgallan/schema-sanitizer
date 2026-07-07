/*
 * Python ABI3 metadata stream wrapper.
 *
 * This file exposes a private Arrow C Stream wrapper that appends metadata
 * columns to each exported batch before PyArrow consumes it.
 */

#include "internal/abi/core_abi3_internal.hh"

#include "api/python_abi3/_core_abi3_metadata_columns.hh"
#include "api/python_abi3/_core_abi3_metadata_stream_builders.hh"
#include "api/python_abi3/_core_abi3_stream_lifecycle.hh"
#include "internal/pipeline/cdata_stream_utils.hh"

#include "nanoarrow/nanoarrow.h"

#include <cerrno>
#include <cstring>
#include <memory>
#include <new>

namespace core_abi3_internal {
namespace {

void close_metadata_stream(MetadataStreamState *state) noexcept {
  if (!state) {
    return;
  }
  if (state->closed) {
    return;
  }
  if (state->inner && !state->stream_obj && !state->stream_capsule) {
    schema_sanitizer_stream_free(state->inner);
    state->inner = nullptr;
    state->closed = true;
    return;
  }
  close_arrow_stream_keepalive(&state->inner, &state->stream_obj,
                               &state->stream_capsule, &state->closed);
}

const char *metadata_stream_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid metadata stream";
  }
  auto *state = static_cast<MetadataStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

void metadata_stream_release(ArrowArrayStream *stream) {
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<MetadataStreamState *>(stream->private_data);
  close_metadata_stream(state);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int metadata_stream_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<MetadataStreamState *>(stream->private_data);
  if (!state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, state->last_error, "metadata_stream.get_schema",
      [&](ArrowSchema *schema) {
        return build_metadata_schema(state, schema);
      });
}

int metadata_stream_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<MetadataStreamState *>(stream->private_data);
  if (!state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_array_callback(
      out, state->last_error, "metadata_stream.get_next",
      [&](ArrowArray *array) { return build_metadata_array(state, array); });
}

} // namespace

bool append_metadata_columns(PyObject *first_row_columns,
                             PyObject *all_row_columns,
                             PyObject *row_span_columns,
                             PyObject *timestamp_columns,
                             std::vector<MetadataColumn> *columns) {
  if (!append_first_row_columns_from_dict(first_row_columns, columns)) {
    return false;
  }
  if (all_row_columns && all_row_columns != Py_None &&
      !append_all_row_columns_from_dict(all_row_columns, columns)) {
    return false;
  }
  if (row_span_columns && row_span_columns != Py_None &&
      !append_row_span_columns_from_dict(row_span_columns, columns)) {
    return false;
  }
  if (timestamp_columns && timestamp_columns != Py_None &&
      !append_timestamp_columns_from_sequence(timestamp_columns, columns)) {
    return false;
  }
  return true;
}

ArrowArrayStream *
make_metadata_stream_from_state(std::unique_ptr<MetadataStreamState> state) {
  auto *wrapped = new (std::nothrow) ArrowArrayStream();
  if (!wrapped) {
    close_metadata_stream(state.get());
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(wrapped, 0, sizeof(*wrapped));
  wrapped->get_schema = &metadata_stream_get_schema;
  wrapped->get_next = &metadata_stream_get_next;
  wrapped->get_last_error = &metadata_stream_last_error;
  wrapped->release = &metadata_stream_release;
  wrapped->private_data = state.release();
  return wrapped;
}

ArrowArrayStream *make_metadata_stream_wrapper(PyObject *stream_obj,
                                               PyObject *first_row_columns,
                                               PyObject *all_row_columns,
                                               PyObject *row_span_columns,
                                               PyObject *timestamp_columns) {
  if (!stream_obj || !first_row_columns) {
    PyErr_SetString(PyExc_SystemError,
                    "metadata stream wrapper received null arguments");
    return nullptr;
  }

  std::unique_ptr<MetadataStreamState> state(new (std::nothrow)
                                                 MetadataStreamState());
  if (!state) {
    PyErr_NoMemory();
    return nullptr;
  }
  if (!append_metadata_columns(first_row_columns, all_row_columns,
                               row_span_columns, timestamp_columns,
                               &state->columns)) {
    return nullptr;
  }

  PyObject *capsule = nullptr;
  ArrowArrayStream *inner = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &inner)) {
    return nullptr;
  }
  state->inner = inner;
  state->stream_capsule = capsule;
  Py_INCREF(stream_obj);
  state->stream_obj = stream_obj;

  return make_metadata_stream_from_state(std::move(state));
}

ArrowArrayStream *make_metadata_stream_wrapper_from_stream(
    ArrowArrayStream *inner, PyObject *first_row_columns,
    PyObject *all_row_columns, PyObject *row_span_columns,
    PyObject *timestamp_columns) {
  if (!inner || !first_row_columns) {
    PyErr_SetString(PyExc_SystemError,
                    "metadata stream wrapper received null native stream");
    return nullptr;
  }

  std::unique_ptr<MetadataStreamState> state(new (std::nothrow)
                                                 MetadataStreamState());
  if (!state) {
    PyErr_NoMemory();
    return nullptr;
  }
  if (!append_metadata_columns(first_row_columns, all_row_columns,
                               row_span_columns, timestamp_columns,
                               &state->columns)) {
    return nullptr;
  }
  state->inner = inner;
  return make_metadata_stream_from_state(std::move(state));
}

PyObject *py_metadata_stream_wrap(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *all_row_columns = nullptr;
  PyObject *row_span_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  if (!PyArg_ParseTuple(args, "OO|OOO:metadata_stream_wrap", &stream_obj,
                        &first_row_columns, &all_row_columns, &row_span_columns,
                        &timestamp_columns)) {
    return nullptr;
  }

  ArrowArrayStream *wrapped = make_metadata_stream_wrapper(
      stream_obj, first_row_columns, all_row_columns, row_span_columns,
      timestamp_columns);
  if (!wrapped) {
    return nullptr;
  }
  return wrap_stream_capsule_with_keepalive(stream_obj, wrapped);
}

} // namespace core_abi3_internal

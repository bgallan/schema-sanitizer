/*
 * Python ABI3 CSV nested-column stream wrapper.
 *
 * This file exposes an Arrow C Stream wrapper that converts top-level nested
 * columns to compact JSON UTF-8 strings so PyArrow's CSV writer can consume the
 * stream without Python row materialization.
 */
#include "internal/abi/core_abi3_internal.hh"

#include "api/python_abi3/_core_abi3_csv_nested_stream_parts.hh"
#include "api/python_abi3/_core_abi3_stream_lifecycle.hh"

#include "internal/pipeline/cdata_stream_utils.hh"

#include "nanoarrow/nanoarrow.h"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>

namespace core_abi3_internal {
namespace {

using csv_nested_stream::CsvNestedArrayState;
using csv_nested_stream::CsvNestedSchemaState;
using csv_nested_stream::CsvNestedStreamState;

void close_csv_nested_stream(CsvNestedStreamState *state) noexcept {
  if (!state) {
    return;
  }
  close_arrow_stream_keepalive(&state->inner, &state->stream_obj,
                               &state->stream_capsule, &state->closed);
}

const char *csv_nested_stream_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid CSV nested stream";
  }
  auto *state = static_cast<CsvNestedStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

void csv_nested_stream_release(ArrowArrayStream *stream) {
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<CsvNestedStreamState *>(stream->private_data);
  close_csv_nested_stream(state);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int csv_nested_stream_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *stream_state =
      static_cast<CsvNestedStreamState *>(stream->private_data);
  if (!stream_state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, stream_state->last_error, "csv_nested_stream.get_schema",
      [&](ArrowSchema *schema) {
        std::unique_ptr<CsvNestedSchemaState> schema_state(
            new (std::nothrow) CsvNestedSchemaState());
        if (!schema_state) {
          return sanitize::Status::OutOfMemory("CSV nested stream schema OOM");
        }
        const int rc = stream_state->inner->get_schema(
            stream_state->inner, schema_state->base.get());
        if (rc != 0) {
          return sanitize::Status::IOError(
              "CSV nested stream inner get_schema failed");
        }
        ArrowSchema &base_schema = schema_state->base.value();
        SAN_RETURN_NOT_OK(csv_nested_stream::load_csv_nested_schema(
            stream_state, &base_schema));
        SAN_RETURN_NOT_OK(csv_nested_stream::append_schema_children(
            stream_state, schema_state.get()));

        csv_nested_stream::clear_schema(schema);
        schema->format = base_schema.format;
        schema->name = base_schema.name;
        schema->metadata = base_schema.metadata;
        schema->flags = base_schema.flags;
        schema->n_children =
            static_cast<std::int64_t>(schema_state->children.size());
        schema->children = schema_state->children.empty()
                               ? nullptr
                               : schema_state->children.data();
        schema->dictionary = base_schema.dictionary;
        schema->private_data = schema_state.release();
        schema->release = &csv_nested_stream::csv_nested_schema_release;
        return sanitize::Status::OK();
      });
}

int csv_nested_stream_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *stream_state =
      static_cast<CsvNestedStreamState *>(stream->private_data);
  if (!stream_state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_array_callback(
      out, stream_state->last_error, "csv_nested_stream.get_next",
      [&](ArrowArray *array) {
        if (!stream_state->schema_loaded) {
          return sanitize::Status::Invalid(
              "CSV nested stream schema must be loaded before batches");
        }
        std::unique_ptr<CsvNestedArrayState> state(new (std::nothrow)
                                                       CsvNestedArrayState());
        if (!state) {
          return sanitize::Status::OutOfMemory("CSV nested stream array OOM");
        }
        const int rc = stream_state->inner->get_next(stream_state->inner,
                                                     state->base.get());
        if (rc != 0) {
          return sanitize::Status::IOError(
              "CSV nested stream inner get_next failed");
        }
        ArrowArray &base_array = state->base.value();
        if (!base_array.release) {
          csv_nested_stream::clear_array(array);
          return sanitize::Status::OK();
        }
        if (base_array.n_children !=
            static_cast<std::int64_t>(stream_state->columns.size())) {
          return sanitize::Status::Invalid(
              "CSV nested stream array/schema mismatch");
        }

        const std::int64_t length = base_array.length;
        state->nested_arrays.resize(stream_state->columns.size());
        state->children.reserve(stream_state->columns.size());
        for (std::size_t i = 0; i < stream_state->columns.size(); ++i) {
          if (!stream_state->columns[i].nested) {
            state->children.push_back(base_array.children[i]);
            continue;
          }
          SAN_RETURN_NOT_OK(csv_nested_stream::build_nested_utf8_array(
              &state->nested_arrays[i], stream_state->columns[i].field,
              *base_array.children[i], length));
          state->children.push_back(&state->nested_arrays[i].array);
        }

        csv_nested_stream::clear_array(array);
        array->length = length;
        array->null_count = base_array.null_count;
        array->offset = base_array.offset;
        array->n_buffers = base_array.n_buffers;
        array->buffers =
            base_array.buffers ? base_array.buffers : state->struct_buffers;
        array->n_children = static_cast<std::int64_t>(state->children.size());
        array->children =
            state->children.empty() ? nullptr : state->children.data();
        array->dictionary = base_array.dictionary;
        array->private_data = state.release();
        array->release = &csv_nested_stream::csv_nested_array_release;
        return sanitize::Status::OK();
      });
}

} // namespace

PyObject *py_csv_nested_stream_wrap(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:csv_nested_stream_wrap", &stream_obj)) {
    return nullptr;
  }

  std::unique_ptr<CsvNestedStreamState> state(new (std::nothrow)
                                                  CsvNestedStreamState());
  if (!state) {
    PyErr_NoMemory();
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

  auto *wrapped = new (std::nothrow) ArrowArrayStream();
  if (!wrapped) {
    close_csv_nested_stream(state.get());
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(wrapped, 0, sizeof(*wrapped));
  wrapped->get_schema = &csv_nested_stream_get_schema;
  wrapped->get_next = &csv_nested_stream_get_next;
  wrapped->get_last_error = &csv_nested_stream_last_error;
  wrapped->release = &csv_nested_stream_release;
  wrapped->private_data = state.release();

  return wrap_stream_capsule_with_keepalive(stream_obj, wrapped);
}

} // namespace core_abi3_internal

/*
 * Python ABI3 source-selected sink wrapper.
 *
 * This entry point chooses path, stream, or text handling from one native ABI
 * call so Python read/write orchestration can stay compact.
 */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"
#include "internal/abi/python_abi3/native_sink.hh"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>
#include <utility>

#include "api/python_abi3/metadata/stream/stream.hh"
#include "sanitize/ingest/chunk_source.hh"

namespace core_abi3_internal {
namespace {

bool metadata_object_has_items(PyObject *obj) {
  if (!obj || obj == Py_None) {
    return false;
  }
  const Py_ssize_t size = PyObject_Length(obj);
  if (size < 0) {
    PyErr_Clear();
    return true;
  }
  return size > 0;
}

bool metadata_args_have_columns(PyObject *first_row_columns,
                                PyObject *all_row_columns,
                                PyObject *timestamp_columns) {
  return metadata_object_has_items(first_row_columns) ||
         metadata_object_has_items(all_row_columns) ||
         metadata_object_has_items(timestamp_columns);
}

bool wrap_sink_stream_with_metadata(ArrowArrayStream **main_stream,
                                    PyObject *first_row_columns,
                                    PyObject *all_row_columns,
                                    PyObject *timestamp_columns) {
  if (!main_stream || !*main_stream ||
      !metadata_args_have_columns(first_row_columns, all_row_columns,
                                  timestamp_columns)) {
    return true;
  }
  PyObject *empty_first = nullptr;
  if (!first_row_columns || first_row_columns == Py_None) {
    empty_first = PyDict_New();
    if (!empty_first) {
      return false;
    }
    first_row_columns = empty_first;
  }
  ArrowArrayStream *wrapped = make_metadata_stream_wrapper_from_stream(
      *main_stream, first_row_columns, all_row_columns, Py_None,
      timestamp_columns);
  Py_XDECREF(empty_first);
  if (!wrapped) {
    return false;
  }
  *main_stream = wrapped;
  return true;
}

PyObject *pack_sink_or_raise_with_metadata(
    sanitize::Result<NativeSinkOutput> result, PyObject *keepalive,
    PyObject *first_row_columns, PyObject *all_row_columns,
    PyObject *timestamp_columns) {
  if (!result.ok()) {
    raise_status_error(result.status());
    return nullptr;
  }
  auto output = std::move(result).ValueOrDie();
  ArrowArrayStream *main_stream = output.stream.release();
  NativeDiagnostics *diagnostics = output.diagnostics.release();
  if (!wrap_sink_stream_with_metadata(&main_stream, first_row_columns,
                                      all_row_columns, timestamp_columns)) {
    release_sink_outputs(main_stream, diagnostics);
    return nullptr;
  }
  return pack_stream_and_diagnostics(keepalive, main_stream, diagnostics);
}

} // namespace

PyObject *py_context_to_sink_from_source(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  const char *frontend_name = nullptr;
  const char *source_name = nullptr;
  PyObject *payload_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  PyObject *first_row_columns = Py_None;
  PyObject *all_row_columns = Py_None;
  PyObject *timestamp_columns = Py_None;

  if (!PyArg_ParseTuple(args, "OsssO|OOOO:context_to_sink_from_source",
                        &ctx_obj, &sink_name, &frontend_name, &source_name,
                        &payload_obj, &prepared_obj, &first_row_columns,
                        &all_row_columns, &timestamp_columns)) {
    return nullptr;
  }

  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;

  sanitize::PreparedOptionsPtr prepared;
  if (!resolve_prepared_options(prepared_obj, &prepared)) {
    return nullptr;
  }

  try {
    switch (parse_python_source_kind(source_name)) {
    case PythonSourceKind::kPath: {
      PyObject *path_bytes = fsencode_path(payload_obj);
      if (!path_bytes) {
        return nullptr;
      }
      const char *path = PyBytes_AsString(path_bytes);
      if (!path) {
        Py_DECREF(path_bytes);
        return nullptr;
      }
      std::string path_copy(path);
      Py_DECREF(path_bytes);
      auto result = call_without_gil([&] {
        auto source = sanitize::chunk_source_from_path_with_encoding(
            std::move(path_copy), prepared->spec.input_text_encoding,
            prepared->spec.memory_limit_bytes);
        if (!source.ok()) {
          return sanitize::Result<NativeSinkOutput>(source.status());
        }
        return native_sink_from_source(ctx, sink_name, frontend_name,
                                       std::move(source).ValueOrDie(), prepared,
                                       "context_to_sink_from_source[path]");
      });
      return pack_sink_or_raise_with_metadata(
          std::move(result), ctx_obj, first_row_columns, all_row_columns,
          timestamp_columns);
    }

    case PythonSourceKind::kText: {
      const char *data = nullptr;
      Py_ssize_t data_len = 0;
      if (!bytes_or_str_view(payload_obj, &data, &data_len)) {
        return nullptr;
      }
      std::string bytes(data, static_cast<std::size_t>(data_len));
      auto result = call_without_gil([&] {
        return native_sink_from_source(
            ctx, sink_name, frontend_name,
            sanitize::chunk_source_from_bytes(std::move(bytes)), prepared,
            "context_to_sink_from_source[text]");
      });
      return pack_sink_or_raise_with_metadata(
          std::move(result), ctx_obj, first_row_columns, all_row_columns,
          timestamp_columns);
    }

    case PythonSourceKind::kStream: {
      if (!python_reader_has_read_seek(payload_obj)) {
        set_python_reader_type_error();
        return nullptr;
      }
      auto source = make_python_reader_chunk_source(payload_obj);
      auto result = call_without_gil([&] {
        return native_sink_from_source(ctx, sink_name, frontend_name,
                                       std::move(source), prepared,
                                       "context_to_sink_from_source[stream]");
      });
      return pack_sink_or_raise_with_metadata(
          std::move(result), payload_obj, first_row_columns, all_row_columns,
          timestamp_columns);
    }

    case PythonSourceKind::kUnknown:
      break;
    }
  } catch (const std::bad_alloc &) {
    PyErr_NoMemory();
    return nullptr;
  } catch (const std::exception &error) {
    PyErr_SetString(PyExc_RuntimeError, error.what());
    return nullptr;
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError, "unknown source sink error");
    return nullptr;
  }

  PyErr_SetString(PyExc_ValueError,
                  "source must be 'path', 'stream', or 'text'");
  return nullptr;
}

} // namespace core_abi3_internal

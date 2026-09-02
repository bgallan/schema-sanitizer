/*
 * Implements Python ABI3 path-source input and non-registry sink methods.
 *
 * The routines preserve source order and Arrow ownership while applying
 * compiled registry plans.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "api/python_abi3/registry/path_source_sinks_internal.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "api/python_abi3/registry/registry_stream_metadata.hh"

namespace core_abi3_internal {
using namespace path_registry_detail;

/// Reads one Python-backed source through a registry-aware native sink.
PyObject *py_context_to_registry_sink_from_source(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  const char *frontend_name = nullptr;
  const char *source_name = nullptr;
  PyObject *payload_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *all_row_columns = nullptr;
  PyObject *row_span_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(
          args, "OsssOOsss|OOOO:context_to_registry_sink_from_source", &ctx_obj,
          &sink_name, &frontend_name, &source_name, &payload_obj, &prepared_obj,
          &registry_json, &field_name_policy, &schema_mode, &first_row_columns,
          &all_row_columns, &row_span_columns, &timestamp_columns)) {
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
          return sanitize::Result<NativeRegistrySinkOutput>(source.status());
        }
        return native_registry_sink_from_source(
            ctx, sink_name, frontend_name, std::move(source).ValueOrDie(),
            prepared, registry_json, field_name_policy, schema_mode,
            "context_to_registry_sink_from_source[path]");
      });
      return pack_registry_or_raise_with_metadata(
          std::move(result), ctx_obj, first_row_columns, all_row_columns,
          row_span_columns, timestamp_columns,
          prepared->spec.memory_limit_bytes);
    }

    case PythonSourceKind::kStream: {
      if (!python_reader_has_read_seek(payload_obj)) {
        set_python_reader_type_error();
        return nullptr;
      }
      auto source = make_python_reader_chunk_source(payload_obj);
      auto result = call_without_gil([&] {
        return native_registry_sink_from_source(
            ctx, sink_name, frontend_name, std::move(source), prepared,
            registry_json, field_name_policy, schema_mode,
            "context_to_registry_sink_from_source[stream]");
      });
      return pack_registry_or_raise_with_metadata(
          std::move(result), payload_obj, first_row_columns, all_row_columns,
          row_span_columns, timestamp_columns,
          prepared->spec.memory_limit_bytes);
    }

    case PythonSourceKind::kText: {
      const char *data = nullptr;
      Py_ssize_t data_len = 0;
      if (!bytes_or_str_view(payload_obj, &data, &data_len)) {
        return nullptr;
      }
      std::string bytes;
      if (data_len != 0) {
        bytes.assign(data, static_cast<std::size_t>(data_len));
      }
      auto result = call_without_gil([&] {
        return native_registry_sink_from_source(
            ctx, sink_name, frontend_name,
            sanitize::chunk_source_from_bytes(std::move(bytes)), prepared,
            registry_json, field_name_policy, schema_mode,
            "context_to_registry_sink_from_source[text]");
      });
      return pack_registry_or_raise_with_metadata(
          std::move(result), ctx_obj, first_row_columns, all_row_columns,
          row_span_columns, timestamp_columns,
          prepared->spec.memory_limit_bytes);
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
    PyErr_SetString(PyExc_RuntimeError, "unknown registry sink error");
    return nullptr;
  }

  PyErr_SetString(PyExc_ValueError,
                  "source must be 'path', 'stream', or 'text'");
  return nullptr;
}

/// Reads ordered path-source specifications through the selected native sink.
PyObject *py_context_to_sink_from_path_sources(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *sources_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  int include_source_file = 1;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(args, "OsOOpOO:context_to_sink_from_path_sources",
                        &ctx_obj, &sink_name, &sources_obj, &prepared_obj,
                        &include_source_file, &first_row_columns,
                        &timestamp_columns)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_enabled = false;
  state->source_file_column = include_source_file != 0;
  if (!resolve_prepared_options(prepared_obj, &state->prepared)) {
    return nullptr;
  }
  if (!parse_path_sources(sources_obj, &state->sources)) {
    return nullptr;
  }
  if (!append_registry_metadata_columns(first_row_columns, timestamp_columns,
                                        &state->first_row_columns,
                                        &state->timestamp_columns)) {
    return nullptr;
  }

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;
  stream->private_data = state.release();

  auto *diagnostics = new (std::nothrow) NativeDiagnostics();
  if (!diagnostics) {
    release_arrow_stream(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  return pack_stream_and_diagnostics(ctx_obj, stream, diagnostics);
}

/// Streams path-source chunks from a Python provider through the selected sink.
PyObject *py_context_to_sink_from_path_source_chunk_provider(PyObject *,
                                                             PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  PyObject *provider_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  int include_source_file = 1;
  PyObject *first_row_columns = nullptr;
  PyObject *timestamp_columns = nullptr;

  if (!PyArg_ParseTuple(
          args, "OsOOpOO:context_to_sink_from_path_source_chunk_provider",
          &ctx_obj, &sink_name, &provider_obj, &prepared_obj,
          &include_source_file, &first_row_columns, &timestamp_columns)) {
    return nullptr;
  }
  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx)
    return nullptr;
  if (!PyObject_HasAttrString(provider_obj, "next_sources")) {
    PyErr_SetString(PyExc_TypeError,
                    "path-source chunk provider must expose next_sources()");
    return nullptr;
  }

  auto state = std::make_unique<NativePathSourcesStreamState>();
  state->ctx = ctx;
  state->sink_name = sink_name ? sink_name : "stream";
  state->registry_enabled = false;
  state->source_file_column = include_source_file != 0;
  if (!resolve_prepared_options(prepared_obj, &state->prepared)) {
    return nullptr;
  }
  if (!append_registry_metadata_columns(first_row_columns, timestamp_columns,
                                        &state->first_row_columns,
                                        &state->timestamp_columns)) {
    return nullptr;
  }

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &path_sources_get_schema;
  stream->get_next = &path_sources_get_next;
  stream->get_last_error = &path_sources_last_error;
  stream->release = &path_sources_release;

  Py_INCREF(provider_obj);
  state->chunk_provider = provider_obj;
  stream->private_data = state.release();

  auto *diagnostics = new (std::nothrow) NativeDiagnostics();
  if (!diagnostics) {
    release_arrow_stream(stream);
    PyErr_NoMemory();
    return nullptr;
  }
  return pack_stream_and_diagnostics(ctx_obj, stream, diagnostics);
}

} // namespace core_abi3_internal

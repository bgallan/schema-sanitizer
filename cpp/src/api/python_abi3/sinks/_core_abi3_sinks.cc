/*
 * Implements Python ABI3 sink wrapper entry points.
 *
 * This file converts Python Arrow C Stream inputs into native context-to-sink
 * calls and wraps the resulting Arrow C Stream capsules.
 *
 * Text/path/reader inputs are handled by the source-selected wrapper.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"
#include "internal/abi/python_abi3/native_sink.hh"

#include <exception>
#include <string_view>
#include <utility>

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"

namespace core_abi3_internal {

/// Runs a Python Arrow stream through a native sink and packs stream
/// diagnostics.
PyObject *py_context_to_sink_arrow_stream(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  const char *frontend_name = nullptr;
  PyObject *stream_obj = nullptr;
  PyObject *prepared_obj = Py_None;

  if (!PyArg_ParseTuple(args, "OssOO:context_to_sink_arrow_stream", &ctx_obj,
                        &sink_name, &frontend_name, &stream_obj,
                        &prepared_obj)) {
    return nullptr;
  }

  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  if (!sink_name || std::string_view(sink_name) != "stream") {
    PyErr_SetString(PyExc_ValueError,
                    "Arrow direct sink currently requires sink='stream'");
    return nullptr;
  }
  if (!frontend_name || std::string_view(frontend_name) != "arrow") {
    PyErr_SetString(PyExc_ValueError,
                    "Arrow direct sink requires frontend='arrow'");
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared_options;
  if (!resolve_prepared_options(prepared_obj, &prepared_options)) {
    return nullptr;
  }

  try {
    sanitize::LogicalSchema input_schema;
    auto frontend_r = make_arrow_frontend(
        stream_obj, &input_schema,
        ArrowDirectOptions{
            .timestamp_precision = prepared_options->spec.timestamp_precision,
            .memory_limit_bytes = prepared_options->spec.memory_limit_bytes});
    if (!frontend_r.ok()) {
      raise_status_error(frontend_r.status());
      return nullptr;
    }
    auto frontend = std::move(frontend_r).ValueOrDie();

    auto final_schema_r =
        finalize_direct_arrow_schema(input_schema, *prepared_options);
    if (!final_schema_r.ok()) {
      raise_status_error(final_schema_r.status());
      return nullptr;
    }
    auto final_schema = std::move(final_schema_r).ValueOrDie();

    auto out_r =
        ingest_direct_arrow_stream(std::move(frontend), std::move(final_schema),
                                   std::move(prepared_options), ctx->ctx);
    if (!out_r.ok()) {
      raise_status_error(out_r.status());
      return nullptr;
    }

    auto sink = native_sink_from_ingest_stream(std::move(out_r).ValueOrDie());
    if (!sink.ok()) {
      raise_status_error(sink.status());
      return nullptr;
    }
    auto output = std::move(sink).ValueOrDie();
    return pack_stream_and_diagnostics(stream_obj, output.stream.release(),
                                       output.diagnostics.release());
  } catch (const std::bad_alloc &) {
    PyErr_NoMemory();
    return nullptr;
  } catch (const std::exception &e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError, "unknown Arrow direct sink error");
    return nullptr;
  }

  return nullptr;
}

} // namespace core_abi3_internal

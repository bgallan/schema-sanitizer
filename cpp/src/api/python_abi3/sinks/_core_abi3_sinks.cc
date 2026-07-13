/*
 * Python ABI3 sink wrapper entry points.
 *
 * This file converts Python Arrow C Stream inputs into native context-to-sink
 * calls and wraps the resulting Arrow C Stream capsules. Text/path/reader
 * inputs are handled by the source-selected wrapper.
 */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <exception>
#include <string_view>
#include <utility>

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"

namespace core_abi3_internal {

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

  ArrowArrayStream *main_stream = nullptr;
  schema_sanitizer_diagnostics *diagnostics = nullptr;
  char *err = nullptr;

  try {
    sanitize::LogicalSchema input_schema;
    auto frontend_r = make_arrow_frontend(
        stream_obj, &input_schema,
        ArrowDirectOptions{.timestamp_precision =
                               prepared_options->spec.timestamp_precision});
    if (!frontend_r.ok()) {
      raise_status_error(code_for_status(frontend_r.status()),
                         dup_cstr(frontend_r.status().ToString()));
      return nullptr;
    }
    auto frontend = std::move(frontend_r).ValueOrDie();

    auto final_schema_r =
        finalize_direct_arrow_schema(input_schema, *prepared_options);
    if (!final_schema_r.ok()) {
      raise_status_error(code_for_status(final_schema_r.status()),
                         dup_cstr(final_schema_r.status().ToString()));
      return nullptr;
    }
    auto final_schema = std::move(final_schema_r).ValueOrDie();

    auto out_r =
        ingest_direct_arrow_stream(std::move(frontend), std::move(final_schema),
                                   std::move(prepared_options), ctx->ctx);
    if (!out_r.ok()) {
      raise_status_error(code_for_status(out_r.status()),
                         dup_cstr(out_r.status().ToString()));
      return nullptr;
    }

    SinkOutputs outputs{.stream = &main_stream, .diagnostics = &diagnostics};
    int rc = ingest_stream_to_streams(
        std::move(out_r).ValueOrDie(), outputs, &err,
        "schema_sanitizer_context_to_sink_arrow_stream");
    if (rc != SCHEMA_SANITIZER_STATUS_OK) {
      release_sink_outputs(main_stream, diagnostics);
      raise_status_error(rc, err);
      return nullptr;
    }
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

  return pack_stream_and_diagnostics(stream_obj, main_stream, diagnostics);
}

} // namespace core_abi3_internal

/*
 * Implements the Python ABI3 registry-backed Arrow stream-sink wrapper.
 *
 * This file keeps direct Arrow registry orchestration separate from the text,
 * path, and reader registry sink wrappers.
 */

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <exception>
#include <string>
#include <string_view>
#include <utility>

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"
#include "internal/abi/python_abi3/native_sink.hh"
namespace core_abi3_internal {

/// Converts a Python Arrow C stream through registry-backed native sinks.
PyObject *py_context_to_registry_sink_arrow_stream(PyObject *, PyObject *args) {
  static constexpr const char *kWhere = "context_to_registry_sink_arrow_stream";

  PyObject *ctx_obj = nullptr;
  const char *sink_name = nullptr;
  const char *frontend_name = nullptr;
  PyObject *stream_obj = nullptr;
  PyObject *prepared_obj = Py_None;
  const char *registry_json = nullptr;
  const char *field_name_policy = nullptr;
  const char *schema_mode = nullptr;

  if (!PyArg_ParseTuple(args, "OssOOsss:context_to_registry_sink_arrow_stream",
                        &ctx_obj, &sink_name, &frontend_name, &stream_obj,
                        &prepared_obj, &registry_json, &field_name_policy,
                        &schema_mode)) {
    return nullptr;
  }

  auto *ctx = unwrap_context(ctx_obj);
  if (!ctx) {
    return nullptr;
  }
  if (!sink_name || std::string_view(sink_name) != "stream") {
    PyErr_SetString(
        PyExc_ValueError,
        "Arrow direct registry sink currently requires sink='stream'");
    return nullptr;
  }
  if (!frontend_name || std::string_view(frontend_name) != "arrow") {
    PyErr_SetString(PyExc_ValueError,
                    "Arrow direct registry sink requires frontend='arrow'");
    return nullptr;
  }

  sanitize::PreparedOptionsPtr prepared_options;
  if (!resolve_prepared_options(prepared_obj, &prepared_options)) {
    return nullptr;
  }

  const auto valid =
      validate_registry_sink_mode(schema_mode, registry_json, kWhere);
  if (!valid.ok()) {
    raise_status_error(valid);
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

    auto merged_r = sanitize::merge_schema_registry(make_registry_merge_input(
        std::move(input_schema), registry_json, field_name_policy,
        prepared_options->spec.default_key_name,
        prepared_options->spec.field_order,
        prepared_options->operation_detected_at));
    if (!merged_r.ok()) {
      raise_status_error(merged_r.status());
      return nullptr;
    }
    auto merged = std::move(merged_r).ValueOrDie();

    auto out_r = ingest_direct_arrow_stream(
        std::move(frontend), std::move(merged.schema),
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
    return pack_registry_stream_result(
        stream_obj, output.stream.release(), output.diagnostics.release(),
        merged.registry_json, merged.drifts_json, merged.detected_at, nullptr);
  } catch (const std::bad_alloc &) {
    PyErr_NoMemory();
    return nullptr;
  } catch (const std::exception &e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError,
                    "unknown Arrow direct registry sink error");
    return nullptr;
  }

  return nullptr;
}

} // namespace core_abi3_internal

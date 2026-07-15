/*
 * Python ABI3 registry-backed Arrow stream sink wrapper.
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

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"
namespace core_abi3_internal {

// Converts a Python Arrow C stream through registry-backed native sinks.
PyObject *py_context_to_registry_sink_arrow_stream(PyObject *, PyObject *args) {
  static constexpr const char *kWhere =
      "schema_sanitizer_context_to_registry_sink_arrow_stream";

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

  char *err = nullptr;
  int rc =
      validate_registry_sink_mode(schema_mode, registry_json, &err, kWhere);
  if (rc != SCHEMA_SANITIZER_STATUS_OK) {
    raise_status_error(rc, err);
    return nullptr;
  }

  ArrowArrayStream *main_stream = nullptr;
  schema_sanitizer_diagnostics *diagnostics = nullptr;
  char *out_registry_json = nullptr;
  char *out_drifts_json = nullptr;
  char *out_conversion_timestamp = nullptr;

  try {
    sanitize::LogicalSchema input_schema;
    auto frontend_r = make_arrow_frontend(
        stream_obj, &input_schema,
        ArrowDirectOptions{.timestamp_precision =
                               prepared_options->spec.timestamp_precision,
                           .memory_limit_bytes =
                               prepared_options->spec.memory_limit_bytes});
    if (!frontend_r.ok()) {
      raise_status_error(code_for_status(frontend_r.status()),
                         dup_cstr(frontend_r.status().ToString()));
      return nullptr;
    }
    auto frontend = std::move(frontend_r).ValueOrDie();

    auto merged_r = sanitize::merge_schema_registry(make_registry_merge_input(
        std::move(input_schema), registry_json, field_name_policy,
        prepared_options->spec.default_key_name,
        prepared_options->spec.field_order));
    if (!merged_r.ok()) {
      raise_status_error(code_for_status(merged_r.status()),
                         dup_cstr(merged_r.status().ToString()));
      return nullptr;
    }
    auto merged = std::move(merged_r).ValueOrDie();

    RegistrySinkOutputs registry_outputs{.sink = SinkOutputs{},
                                         .registry_json = &out_registry_json,
                                         .drifts_json = &out_drifts_json,
                                         .conversion_timestamp =
                                             &out_conversion_timestamp};
    rc = copy_registry_json_outputs(merged, registry_outputs, &err, kWhere);
    if (rc != SCHEMA_SANITIZER_STATUS_OK) {
      raise_status_error(rc, err);
      return nullptr;
    }

    auto out_r = ingest_direct_arrow_stream(
        std::move(frontend), std::move(merged.schema),
        std::move(prepared_options), ctx->ctx);
    if (!out_r.ok()) {
      schema_sanitizer_free_string(out_registry_json);
      schema_sanitizer_free_string(out_drifts_json);
      schema_sanitizer_free_string(out_conversion_timestamp);
      raise_status_error(code_for_status(out_r.status()),
                         dup_cstr(out_r.status().ToString()));
      return nullptr;
    }

    SinkOutputs outputs{.stream = &main_stream, .diagnostics = &diagnostics};
    rc = ingest_stream_to_streams(std::move(out_r).ValueOrDie(), outputs, &err,
                                  kWhere);
    if (rc != SCHEMA_SANITIZER_STATUS_OK) {
      release_sink_outputs(main_stream, diagnostics);
      schema_sanitizer_free_string(out_registry_json);
      schema_sanitizer_free_string(out_drifts_json);
      schema_sanitizer_free_string(out_conversion_timestamp);
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
    PyErr_SetString(PyExc_RuntimeError,
                    "unknown Arrow direct registry sink error");
    return nullptr;
  }

  return pack_registry_stream_result(stream_obj, main_stream, diagnostics,
                                     out_registry_json, out_drifts_json,
                                     out_conversion_timestamp, nullptr);
}

} // namespace core_abi3_internal

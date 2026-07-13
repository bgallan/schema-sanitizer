// Declares Python ABI3 capsule ownership and sink-result packing helpers.

#pragma once

#include <memory>

#include "internal/abi/python_abi3/base.hh"

namespace sanitize {
struct PreparedOptions;
}

namespace core_abi3_internal {

schema_sanitizer_context *unwrap_context(PyObject *obj);
PyObject *wrap_context_capsule(schema_sanitizer_context *ctx);
schema_sanitizer_prepared_options *unwrap_prepared_options(PyObject *obj);
// Resolves None to the shared default options or unwraps a prepared capsule.
bool resolve_prepared_options(
    PyObject *obj,
    std::shared_ptr<const sanitize::PreparedOptions> *out_prepared);
schema_sanitizer_diagnostics *unwrap_diagnostics(PyObject *obj);
PyObject *
wrap_prepared_options_capsule(schema_sanitizer_prepared_options *prepared);
PyObject *wrap_diagnostics_capsule(schema_sanitizer_diagnostics *diagnostics);
PyObject *wrap_stream_capsule_with_keepalive(PyObject *keepalive_obj,
                                             ArrowArrayStream *stream);
void release_sink_outputs(ArrowArrayStream *main_stream,
                          schema_sanitizer_diagnostics *diagnostics);
PyObject *
pack_stream_and_diagnostics(PyObject *keepalive, ArrowArrayStream *main_stream,
                            schema_sanitizer_diagnostics *diagnostics);
// Packs the fixed six-field registry result. native_registry_state is borrowed;
// nullptr emits None in the final slot.
PyObject *pack_registry_stream_result(PyObject *keepalive,
                                      ArrowArrayStream *main_stream,
                                      schema_sanitizer_diagnostics *diagnostics,
                                      char *registry_json, char *drifts_json,
                                      char *conversion_timestamp,
                                      PyObject *native_registry_state);

} // namespace core_abi3_internal

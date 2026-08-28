// Declares Python ABI3 capsule ownership and sink-result packing helpers.

#pragma once

#include <memory>
#include <string_view>

#include "internal/abi/python_abi3/base.hh"

namespace sanitize {
struct PreparedOptions;
}

namespace core_abi3_internal {

struct NativeContext;
struct NativeDiagnostics;
struct NativePreparedOptions;

NativeContext *unwrap_context(PyObject *obj);
PyObject *wrap_context_capsule(NativeContext *ctx);
NativePreparedOptions *unwrap_prepared_options(PyObject *obj);
// Resolves None to the shared default options or unwraps a prepared capsule.
bool resolve_prepared_options(
    PyObject *obj,
    std::shared_ptr<const sanitize::PreparedOptions> *out_prepared);
NativeDiagnostics *unwrap_diagnostics(PyObject *obj);
PyObject *wrap_prepared_options_capsule(NativePreparedOptions *prepared);
PyObject *wrap_diagnostics_capsule(NativeDiagnostics *diagnostics);
PyObject *wrap_stream_capsule_with_keepalive(PyObject *keepalive_obj,
                                             ArrowArrayStream *stream);
void release_sink_outputs(ArrowArrayStream *main_stream,
                          NativeDiagnostics *diagnostics);
PyObject *pack_stream_and_diagnostics(PyObject *keepalive,
                                      ArrowArrayStream *main_stream,
                                      NativeDiagnostics *diagnostics);
// Packs the fixed six-field registry result. native_registry_state is borrowed;
// nullptr emits None in the final slot.
PyObject *pack_registry_stream_result(PyObject *keepalive,
                                      ArrowArrayStream *main_stream,
                                      NativeDiagnostics *diagnostics,
                                      std::string_view registry_json,
                                      std::string_view drifts_json,
                                      std::string_view conversion_timestamp,
                                      PyObject *native_registry_state);

} // namespace core_abi3_internal

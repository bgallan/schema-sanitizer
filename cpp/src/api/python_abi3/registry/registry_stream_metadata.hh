// Declares generated metadata and result-packing helpers for registry-backed
// streams. The interface carries compiled registry state alongside owned Arrow
// C Stream results.

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#ifndef Py_LIMITED_API
#define Py_LIMITED_API 0x030A0000
#endif
#include <Python.h>

#include "api/python_abi3/metadata/columns/api.hh"
#include "api/python_abi3/registry/plan/plan.hh"
#include "internal/abi/python_abi3/native_state.hh"

struct ArrowArrayStream;

namespace core_abi3_internal {

PyObject *pack_registry_stream_result_with_state(
    PyObject *keepalive, ArrowArrayStream *main_stream,
    NativeDiagnostics *diagnostics, std::string_view registry_json,
    std::string_view drifts_json, std::string_view conversion_timestamp,
    std::shared_ptr<const NativeRegistryPlan> registry_plan);

void append_registry_first_row_columns(std::vector<MetadataColumn> *columns,
                                       const std::string &registry_json,
                                       const std::string &drifts_json);

/// Appends the items from a JSON array to an in-progress array body.
void append_json_array_items(std::string *out, std::string_view array_json);

std::vector<MetadataColumn> registry_child_metadata_columns(
    const std::vector<MetadataColumn> &first_row_columns,
    const std::vector<MetadataColumn> &timestamp_columns,
    bool first_row_pending, std::string_view source_file,
    bool include_source_file);

} // namespace core_abi3_internal

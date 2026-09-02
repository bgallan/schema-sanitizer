// Builds generated metadata columns and Python results for registry-backed
// streams. It derives first-row values from native registry state and transfers
// stream ownership.

#include "api/python_abi3/registry/registry_stream_metadata.hh"

#include "api/python_abi3/registry/plan/plan.hh"

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

namespace core_abi3_internal {

namespace {

/// Removes leading and trailing ASCII whitespace without allocating a
/// replacement string.
std::string_view trim_ascii_whitespace(std::string_view value) {
  while (!value.empty() && static_cast<unsigned char>(value.front()) <= ' ') {
    value.remove_prefix(1);
  }
  while (!value.empty() && static_cast<unsigned char>(value.back()) <= ' ') {
    value.remove_suffix(1);
  }
  return value;
}

} // namespace

/// Packages a registry stream, diagnostics, and reusable native state for
/// Python.
PyObject *pack_registry_stream_result_with_state(
    PyObject *keepalive, ArrowArrayStream *main_stream,
    NativeDiagnostics *diagnostics, std::string_view registry_json,
    std::string_view drifts_json, std::string_view conversion_timestamp,
    std::shared_ptr<const NativeRegistryPlan> registry_plan) {
  PyObject *state = wrap_native_registry_state(std::move(registry_plan));
  if (!state) {
    release_sink_outputs(main_stream, diagnostics);
    return nullptr;
  }
  PyObject *out = pack_registry_stream_result(
      keepalive, main_stream, diagnostics, registry_json, drifts_json,
      conversion_timestamp, state);
  Py_DECREF(state);
  return out;
}

void append_json_array_items(std::string *out, std::string_view array_json) {
  if (!out) {
    return;
  }
  array_json = trim_ascii_whitespace(array_json);
  if (array_json.size() >= 2 && array_json.front() == '[' &&
      array_json.back() == ']') {
    array_json.remove_prefix(1);
    array_json.remove_suffix(1);
  }
  array_json = trim_ascii_whitespace(array_json);
  if (array_json.empty()) {
    return;
  }
  const std::size_t delimiter_size = out->size() > 1 ? 1 : 0;
  out->reserve(out->size() + delimiter_size + array_json.size());
  if (delimiter_size != 0) {
    out->push_back(',');
  }
  out->append(array_json);
}

/// Appends registry and drift payloads as first-row metadata columns.
void append_registry_first_row_columns(std::vector<MetadataColumn> *columns,
                                       const std::string &registry_json,
                                       const std::string &drifts_json) {
  if (!columns) {
    return;
  }
  columns->reserve(columns->size() + 2);

  MetadataColumn registry;
  registry.name = "schema_registry";
  registry.value = registry_json;
  registry.placement = MetadataColumnPlacement::FirstRowUtf8;
  columns->push_back(std::move(registry));

  MetadataColumn drifts;
  drifts.name = "schema_drifts";
  drifts.value = drifts_json;
  drifts.placement = MetadataColumnPlacement::FirstRowUtf8;
  columns->push_back(std::move(drifts));
}

/// Derives generated metadata columns for one child of the schema-registry
/// stream.
std::vector<MetadataColumn> registry_child_metadata_columns(
    const std::vector<MetadataColumn> &first_row_columns,
    const std::vector<MetadataColumn> &timestamp_columns,
    bool first_row_pending, std::string_view source_file,
    bool include_source_file) {
  std::vector<MetadataColumn> columns;
  columns.reserve(first_row_columns.size() + timestamp_columns.size() +
                  (include_source_file ? 1 : 0));

  for (const auto &source : first_row_columns) {
    MetadataColumn column;
    column.name = source.name;
    column.spans = source.spans;
    column.placement = source.placement;
    column.span_index = source.span_index;
    column.span_offset = source.span_offset;
    if (first_row_pending) {
      column.value = source.value;
      column.is_null = source.is_null;
    } else {
      column.is_null = true;
    }
    columns.push_back(std::move(column));
  }

  if (include_source_file) {
    MetadataColumn source_column;
    source_column.name = "source_file";
    source_column.value = std::string(source_file);
    source_column.placement = MetadataColumnPlacement::AllRowsUtf8;
    columns.push_back(std::move(source_column));
  }

  for (auto column : timestamp_columns) {
    columns.push_back(std::move(column));
  }
  return columns;
}

} // namespace core_abi3_internal

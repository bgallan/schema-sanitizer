// Shared native path-source helpers for Python ABI3 wrappers.

#include "api/python_abi3/_core_abi3_path_sources.hh"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/frontends/builtin_frontends.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/streaming/csv_streaming_scanner.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/registry/registry.hh"

namespace core_abi3_internal {
namespace {

constexpr const char *kPathSourcePlanCapsuleName =
    "schema_sanitizer.path_source_plan";

struct PathSourcePlanCapsule {
  std::vector<PathSourceSpec> sources;
};

void path_source_plan_capsule_destructor(PyObject *capsule) {
  auto *plan = static_cast<PathSourcePlanCapsule *>(
      PyCapsule_GetPointer(capsule, kPathSourcePlanCapsuleName));
  if (!plan) {
    PyErr_Clear();
    return;
  }
  delete plan;
}

PathSourcePlanCapsule *unwrap_path_source_plan(PyObject *obj) {
  if (!PyCapsule_CheckExact(obj)) {
    return nullptr;
  }
  return static_cast<PathSourcePlanCapsule *>(
      PyCapsule_GetPointer(obj, kPathSourcePlanCapsuleName));
}

bool py_path_source_text(PyObject *obj, const char *name, std::string *out) {
  if (!PyUnicode_Check(obj)) {
    PyErr_Format(PyExc_TypeError, "%s must be a string", name);
    return false;
  }
  Py_ssize_t size = 0;
  const char *data = PyUnicode_AsUTF8AndSize(obj, &size);
  if (!data) {
    return false;
  }
  out->assign(data, static_cast<std::size_t>(size));
  return true;
}

bool py_path_to_string(PyObject *obj, std::string *out) {
  PyObject *encoded = fsencode_path(obj);
  if (!encoded) {
    return false;
  }
  const char *path = PyBytes_AsString(encoded);
  const Py_ssize_t size = PyBytes_Size(encoded);
  if (!path || size < 0) {
    Py_DECREF(encoded);
    PyErr_SetString(PyExc_ValueError, "invalid source path");
    return false;
  }
  out->assign(path, static_cast<std::size_t>(size));
  Py_DECREF(encoded);
  return true;
}

sanitize::Result<std::optional<std::vector<std::string>>>
csv_header_from_path_source(const PathSourceSpec &source,
                            const sanitize::PreparedOptionsPtr &prepared) {
  SAN_ASSIGN_OR_RAISE(auto chunk_source,
                      sanitize::chunk_source_from_path_with_encoding(
                          source.path, prepared->spec.input_text_encoding));
  sanitize::internal::CsvStreamingScanner scanner(
      std::move(chunk_source), sanitize::internal::kDefaultCsvChunkBytes);
  SAN_RETURN_NOT_OK(scanner.Reset());

  sanitize::internal::PoolResource pmr_pool;
  sanitize::internal::BumpArena arena(pmr_pool.pool());
  for (;;) {
    arena.reset();
    auto next = scanner.next_record(&arena);
    if (!next.ok()) {
      return next.status();
    }
    const sanitize::internal::TextSlice record = *next;
    if (record.view.empty() && scanner.done()) {
      return std::optional<std::vector<std::string>>{};
    }
    if (record.view.empty()) {
      continue;
    }

    std::vector<std::string_view> views;
    sanitize::internal::parse_csv_cells(record.view,
                                        prepared->spec.csv_delimiter.empty()
                                            ? ','
                                            : prepared->spec.csv_delimiter[0],
                                        &views, &arena);
    std::vector<std::string> header;
    header.reserve(views.size());
    for (std::string_view value : views) {
      header.emplace_back(value);
    }
    return std::optional<std::vector<std::string>>(std::move(header));
  }
}

bool is_json_path_source(const PathSourceSpec &source) {
  return source.frontend == "json";
}

bool is_json_array_path_source(const PathSourceSpec &source) {
  return source.frontend == "json_array";
}

bool is_utf8_text_encoding(std::string_view encoding) {
  std::string normalized;
  normalized.reserve(encoding.size());
  for (const unsigned char ch : encoding) {
    if (ch == '_' || ch == ' ') {
      normalized.push_back('-');
    } else {
      normalized.push_back(static_cast<char>(std::tolower(ch)));
    }
  }
  return normalized.empty() || normalized == "utf-8" || normalized == "utf8" ||
         normalized == "utf-8-sig";
}

bool json_group_failure_should_retry_per_source(
    const sanitize::Status &status) {
  return status.message().find("trailing characters after top-level array") !=
         std::string::npos;
}

bool path_has_extension(const std::string &path, std::string_view extension) {
  return path.size() >= extension.size() &&
         path.compare(path.size() - extension.size(), extension.size(),
                      extension) == 0;
}

sanitize::Result<std::optional<char>>
first_non_ws_byte(const std::string &path) {
  std::ifstream in(path, std::ios::binary);
  if (!in.good()) {
    return sanitize::Status::Invalid("failed to open JSON source '", path, "'");
  }
  char ch = '\0';
  while (in.get(ch)) {
    const unsigned char c = static_cast<unsigned char>(ch);
    if (c != ' ' && c != '\n' && c != '\r' && c != '\t') {
      return ch;
    }
  }
  return std::optional<char>{};
}

sanitize::Result<std::optional<char>>
first_non_ws_byte_after_json_array_start(const std::string &path) {
  std::ifstream in(path, std::ios::binary);
  if (!in.good()) {
    return sanitize::Status::Invalid("failed to open JSON source '", path, "'");
  }
  char ch = '\0';
  bool saw_array_start = false;
  while (in.get(ch)) {
    const unsigned char c = static_cast<unsigned char>(ch);
    if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
      continue;
    }
    if (!saw_array_start) {
      if (ch != '[') {
        return std::optional<char>{};
      }
      saw_array_start = true;
      continue;
    }
    return ch;
  }
  return std::optional<char>{};
}

sanitize::Result<bool>
json_source_can_stream_concat(const PathSourceSpec &source) {
  if (!is_json_path_source(source)) {
    return false;
  }
  if (path_has_extension(source.path, ".jsonl") ||
      path_has_extension(source.path, ".ndjson")) {
    return true;
  }
  SAN_ASSIGN_OR_RAISE(auto first, first_non_ws_byte(source.path));
  if (!first) {
    return false;
  }
  return *first != '[';
}

sanitize::Result<bool>
json_source_is_groupable_array_document(const PathSourceSpec &source) {
  if (!is_json_path_source(source)) {
    return false;
  }
  SAN_ASSIGN_OR_RAISE(auto first,
                      first_non_ws_byte_after_json_array_start(source.path));
  if (!first) {
    return false;
  }
  return *first == '{';
}

sanitize::Result<std::size_t>
json_path_source_group_end(const std::vector<PathSourceSpec> &sources,
                           std::size_t start) {
  std::size_t end = start;
  while (end < sources.size()) {
    SAN_ASSIGN_OR_RAISE(const bool can_group,
                        json_source_can_stream_concat(sources[end]));
    if (!can_group) {
      break;
    }
    ++end;
  }
  return end;
}

sanitize::Result<std::size_t> json_array_document_path_source_group_end(
    const std::vector<PathSourceSpec> &sources, std::size_t start) {
  std::size_t end = start;
  while (end < sources.size()) {
    SAN_ASSIGN_OR_RAISE(const bool can_group,
                        json_source_is_groupable_array_document(sources[end]));
    if (!can_group) {
      break;
    }
    ++end;
  }
  return end;
}

std::size_t
csv_path_source_group_end(const std::vector<PathSourceSpec> &sources,
                          std::size_t start) {
  std::size_t end = start;
  while (end < sources.size() && sources[end].frontend == "csv") {
    ++end;
  }
  return end;
}

std::size_t
json_array_path_source_group_end(const std::vector<PathSourceSpec> &sources,
                                 std::size_t start) {
  std::size_t end = start;
  while (end < sources.size() && is_json_array_path_source(sources[end])) {
    ++end;
  }
  return end;
}

sanitize::Result<PathSourceInput>
make_path_source_group_input(const std::vector<PathSourceSpec> &sources,
                             const PathSourceGroupPlan &group,
                             std::string_view input_text_encoding) {
  const std::size_t start = group.start;
  const std::size_t end = group.end;
  if (start >= end || end > sources.size()) {
    return sanitize::Status::Invalid("path-source group is empty");
  }
  const std::string &source_frontend_name = sources[start].frontend;
  const std::string frontend_name =
      group.frontend.empty() ? source_frontend_name : group.frontend;
  std::vector<std::string> paths;
  std::vector<std::string> source_names;
  paths.reserve(end - start);
  source_names.reserve(end - start);
  for (std::size_t i = start; i < end; ++i) {
    if (sources[i].frontend != source_frontend_name) {
      return sanitize::Status::Invalid(
          "path-source group contains mixed frontends");
    }
    paths.push_back(sources[i].path);
    source_names.push_back(sources[i].source_file);
  }

  PathSourceInput input;
  input.frontend = frontend_name;
  if (frontend_name == "json_array" || frontend_name == "json_array_document") {
    input.paths = std::move(paths);
    input.source_names = std::move(source_names);
    return input;
  }

  SAN_ASSIGN_OR_RAISE(
      input.chunk_source,
      sanitize::chunk_source_from_paths_with_source_names_encoding(
          std::move(paths), std::move(source_names), "\n",
          input_text_encoding));
  return input;
}

} // namespace

bool parse_path_sources(PyObject *sources_obj,
                        std::vector<PathSourceSpec> *out) {
  if (PyCapsule_CheckExact(sources_obj)) {
    auto *plan = unwrap_path_source_plan(sources_obj);
    if (!plan) {
      return false;
    }
    *out = plan->sources;
    if (out->empty()) {
      PyErr_SetString(PyExc_ValueError, "sources must not be empty");
      return false;
    }
    return true;
  }
  if (!PySequence_Check(sources_obj) || PyUnicode_Check(sources_obj)) {
    PyErr_SetString(PyExc_TypeError, "sources must be a sequence");
    return false;
  }
  const Py_ssize_t size = PySequence_Size(sources_obj);
  if (size < 0) {
    return false;
  }
  if (size == 0) {
    PyErr_SetString(PyExc_ValueError, "sources must not be empty");
    return false;
  }
  out->clear();
  out->reserve(static_cast<std::size_t>(size));
  for (Py_ssize_t i = 0; i < size; ++i) {
    bool borrowed = false;
    PyObject *item = sequence_item_borrowed_or_new(sources_obj, i, &borrowed);
    if (!item) {
      return false;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> item_owner(
        borrowed ? nullptr : item, Py_DECREF);
    if (!PySequence_Check(item) || PyUnicode_Check(item) ||
        PySequence_Size(item) != 3) {
      PyErr_SetString(PyExc_TypeError,
                      "each source must be (frontend, path, source_file)");
      return false;
    }
    PyObject *frontend_obj = PySequence_GetItem(item, 0);
    PyObject *path_obj = PySequence_GetItem(item, 1);
    PyObject *source_file_obj = PySequence_GetItem(item, 2);
    if (!frontend_obj || !path_obj || !source_file_obj) {
      Py_XDECREF(frontend_obj);
      Py_XDECREF(path_obj);
      Py_XDECREF(source_file_obj);
      return false;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> frontend_owner(frontend_obj,
                                                                   Py_DECREF);
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> path_owner(path_obj,
                                                               Py_DECREF);
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> source_file_owner(
        source_file_obj, Py_DECREF);

    PathSourceSpec spec;
    if (!py_path_source_text(frontend_obj, "source frontend", &spec.frontend) ||
        !py_path_to_string(path_obj, &spec.path) ||
        !py_path_source_text(source_file_obj, "source_file",
                             &spec.source_file)) {
      return false;
    }
    out->push_back(std::move(spec));
  }
  return true;
}

PyObject *py_path_source_plan_create(PyObject *, PyObject *args) {
  PyObject *sources_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:path_source_plan_create", &sources_obj)) {
    return nullptr;
  }
  auto plan = std::make_unique<PathSourcePlanCapsule>();
  if (!parse_path_sources(sources_obj, &plan->sources)) {
    return nullptr;
  }
  PyObject *capsule =
      PyCapsule_New(static_cast<void *>(plan.get()), kPathSourcePlanCapsuleName,
                    path_source_plan_capsule_destructor);
  if (!capsule) {
    return nullptr;
  }
  plan.release();
  return capsule;
}

sanitize::Result<PathSourceInput>
path_source_input(const sanitize::PreparedOptionsPtr &prepared,
                  const PathSourceSpec &source) {
  (void)prepared;
  if (source.frontend == "json" || source.frontend == "json_array" ||
      source.frontend == "csv" || source.frontend == "xml") {
    SAN_ASSIGN_OR_RAISE(auto chunk_source,
                        sanitize::chunk_source_from_path_with_encoding(
                            source.path, prepared->spec.input_text_encoding));
    PathSourceInput input;
    input.frontend = source.frontend;
    input.chunk_source = std::move(chunk_source);
    return input;
  }
  return sanitize::Status::Invalid("unsupported native path source frontend: ",
                                   source.frontend);
}

sanitize::Result<PathSourceGroupPlan>
next_path_source_group_plan(const std::vector<PathSourceSpec> &sources,
                            std::size_t start, PathSourceGroupPurpose purpose,
                            std::string_view input_text_encoding) {
  if (start >= sources.size()) {
    return sanitize::Status::Invalid("path-source group start is out of range");
  }
  switch (purpose) {
  case PathSourceGroupPurpose::kProbe:
  case PathSourceGroupPurpose::kMaterialization:
    break;
  }

  const PathSourceSpec &source = sources[start];
  const bool utf8_input = is_utf8_text_encoding(input_text_encoding);
  if (utf8_input && is_json_path_source(source)) {
    SAN_ASSIGN_OR_RAISE(const std::size_t end,
                        json_path_source_group_end(sources, start));
    if (end > start + 1) {
      return PathSourceGroupPlan{.start = start,
                                 .end = end,
                                 .frontend = "json",
                                 .grouped = true,
                                 .source_file_in_inner = true};
    }
    SAN_ASSIGN_OR_RAISE(
        const std::size_t array_end,
        json_array_document_path_source_group_end(sources, start));
    if (array_end > start + 1) {
      return PathSourceGroupPlan{.start = start,
                                 .end = array_end,
                                 .frontend = "json_array_document",
                                 .grouped = true,
                                 .source_file_in_inner = true};
    }
  }
  if (utf8_input && is_json_array_path_source(source)) {
    const std::size_t end = json_array_path_source_group_end(sources, start);
    if (end > start + 1) {
      return PathSourceGroupPlan{.start = start,
                                 .end = end,
                                 .frontend = "json_array",
                                 .grouped = true,
                                 .source_file_in_inner = true};
    }
  }
  if (source.frontend == "csv") {
    const std::size_t end = csv_path_source_group_end(sources, start);
    if (end > start + 1) {
      return PathSourceGroupPlan{.start = start,
                                 .end = end,
                                 .frontend = "csv",
                                 .grouped = true,
                                 .source_file_in_inner = true};
    }
  }
  return PathSourceGroupPlan{.start = start,
                             .end = start + 1,
                             .frontend = source.frontend,
                             .grouped = false,
                             .source_file_in_inner = false};
}

sanitize::Result<PathSourceInput>
path_source_group_input(const std::vector<PathSourceSpec> &sources,
                        const PathSourceGroupPlan &group,
                        std::string_view input_text_encoding) {
  if (!group.grouped) {
    return sanitize::Status::Invalid("path-source group is not grouped");
  }
  return make_path_source_group_input(sources, group, input_text_encoding);
}

sanitize::Result<sanitize::FrontendHandle>
path_source_frontend(PathSourceInput input, const sanitize::Options &options) {
  if (input.frontend == "json_array" && !input.paths.empty()) {
    auto frontend = sanitize::internal::make_json_array_group_frontend(
        std::move(input.paths), std::move(input.source_names), options);
    if (!frontend) {
      return sanitize::Status::Invalid(
          "invalid grouped json_array path source");
    }
    return frontend;
  }
  if (input.frontend == "json_array_document" && !input.paths.empty()) {
    auto frontend = sanitize::internal::make_json_document_array_group_frontend(
        std::move(input.paths), std::move(input.source_names), options);
    if (!frontend) {
      return sanitize::Status::Invalid(
          "invalid grouped JSON array document path source");
    }
    return frontend;
  }

  auto frontend = sanitize::make_builtin_frontend(
      input.frontend.c_str(), std::move(input.chunk_source), options);
  if (!frontend) {
    return sanitize::Status::Invalid("frontend not registered: ",
                                     input.frontend);
  }
  return frontend;
}

std::string_view path_source_materializer_frontend(std::string_view frontend) {
  if (frontend == "json_array_document") {
    return "json";
  }
  return frontend;
}

sanitize::Status
validate_csv_path_source_headers(const std::vector<PathSourceSpec> &sources,
                                 const sanitize::PreparedOptionsPtr &prepared) {
  if (!prepared || !prepared->spec.csv_has_header) {
    return sanitize::Status::OK();
  }
  std::optional<std::vector<std::string>> expected;
  for (const PathSourceSpec &source : sources) {
    if (source.frontend != "csv") {
      continue;
    }
    SAN_ASSIGN_OR_RAISE(auto header,
                        csv_header_from_path_source(source, prepared));
    if (!header) {
      continue;
    }
    if (!expected) {
      expected = std::move(header);
      continue;
    }
    if (*header != *expected) {
      return sanitize::Status::Invalid("CSV directory header mismatch in ",
                                       source.path);
    }
  }
  return sanitize::Status::OK();
}

bool path_source_failure_is_skippable_json(const PathSourceSpec &source,
                                           const sanitize::Status &status) {
  if (source.frontend != "json" && source.frontend != "json_array") {
    return false;
  }
  const std::string &message = status.message();
  return message.find("JSON parse error") != std::string::npos ||
         message.find("Invalid JSON file") != std::string::npos;
}

void merge_path_source_diagnostics(sanitize::IngestDiagnostics &out,
                                   const sanitize::IngestDiagnostics &child) {
  out.inferred_rows += child.inferred_rows;
  out.inferred_bytes += child.inferred_bytes;
  out.arrow_schema_depth =
      std::max(out.arrow_schema_depth, child.arrow_schema_depth);
  out.parquet_schema_depth =
      std::max(out.parquet_schema_depth, child.parquet_schema_depth);
  out.flattened_fields += child.flattened_fields;
  out.scalar_wrappings += child.scalar_wrappings;
  out.skipped_rows += child.skipped_rows;
}

sanitize::Result<sanitize::PreparedIngest>
prepare_path_source_ingest(schema_sanitizer_context *ctx,
                           const sanitize::PreparedOptionsPtr &prepared,
                           const PathSourceSpec &source) {
  if (!ctx) {
    return sanitize::Status::Invalid("context is null");
  }
  SAN_ASSIGN_OR_RAISE(auto input, path_source_input(prepared, source));
  const std::string frontend_name(
      path_source_materializer_frontend(input.frontend));
  SAN_ASSIGN_OR_RAISE(auto frontend,
                      path_source_frontend(std::move(input), prepared->spec));
  return sanitize::prepare_ingest(frontend_name.c_str(), std::move(frontend),
                                  prepared, ctx->ctx.get());
}

sanitize::Result<sanitize::PreparedIngest>
prepare_path_source_group_ingest(schema_sanitizer_context *ctx,
                                 const sanitize::PreparedOptionsPtr &prepared,
                                 const std::vector<PathSourceSpec> &sources,
                                 std::size_t start, std::size_t end) {
  if (!ctx) {
    return sanitize::Status::Invalid("context is null");
  }
  if (start >= end || end > sources.size()) {
    return sanitize::Status::Invalid("path-source group is empty");
  }
  if (end == start + 1) {
    return prepare_path_source_ingest(ctx, prepared, sources[start]);
  }
  const PathSourceGroupPlan group{
      .start = start,
      .end = end,
      .frontend = sources[start].frontend,
      .grouped = true,
      .source_file_in_inner = true,
  };
  SAN_ASSIGN_OR_RAISE(
      auto input, path_source_group_input(sources, group,
                                          prepared->spec.input_text_encoding));
  const std::string frontend_name(
      path_source_materializer_frontend(input.frontend));
  SAN_ASSIGN_OR_RAISE(auto frontend,
                      path_source_frontend(std::move(input), prepared->spec));
  return sanitize::prepare_ingest(frontend_name.c_str(), std::move(frontend),
                                  prepared, ctx->ctx.get());
}

sanitize::Result<PathSourceRegistryProbeResult> merge_path_source_schemas(
    schema_sanitizer_context *ctx, const std::vector<PathSourceSpec> &sources,
    const sanitize::PreparedOptionsPtr &prepared, const char *registry_json,
    const char *field_name_policy, bool skip_invalid_json_sources,
    const sanitize::LogicalSchema *previous_schema) {
  if (!ctx) {
    return sanitize::Status::Invalid("context is null");
  }
  SAN_RETURN_NOT_OK(validate_csv_path_source_headers(sources, prepared));

  std::string combined_registry = "{}";
  sanitize::LogicalSchema combined_schema;
  sanitize::IngestDiagnostics diagnostics;
  bool has_schema = false;
  std::vector<std::string> skipped_errors;
  auto merge_ingest = [&](sanitize::PreparedIngest ingest) -> sanitize::Status {
    if (ingest.diagnostics) {
      merge_path_source_diagnostics(diagnostics, *ingest.diagnostics);
    }
    auto merged = sanitize::merge_schema_registry(make_registry_merge_input(
        std::move(ingest.logical_schema), combined_registry.c_str(),
        field_name_policy, ingest.opts->spec.default_key_name));
    if (!merged.ok()) {
      return merged.status();
    }
    auto merged_value = std::move(merged).ValueOrDie();
    combined_schema = std::move(merged_value.schema);
    combined_registry = std::move(merged_value.registry_json);
    has_schema = true;
    return sanitize::Status::OK();
  };

  auto merge_single_source =
      [&](const PathSourceSpec &source) -> sanitize::Status {
    auto prepared_ingest = prepare_path_source_ingest(ctx, prepared, source);
    if (!prepared_ingest.ok()) {
      if (skip_invalid_json_sources && path_source_failure_is_skippable_json(
                                           source, prepared_ingest.status())) {
        skipped_errors.push_back(source.path + ": " +
                                 prepared_ingest.status().ToString());
        return sanitize::Status::OK();
      }
      return prepared_ingest.status();
    }
    return merge_ingest(std::move(prepared_ingest).ValueOrDie());
  };

  auto merge_group_sources =
      [&](const PathSourceGroupPlan &group,
          sanitize::Status group_status) -> sanitize::Status {
    const std::string &frontend = sources[group.start].frontend;
    if (frontend == "json") {
      if (!skip_invalid_json_sources &&
          !json_group_failure_should_retry_per_source(group_status)) {
        return group_status;
      }
    } else if (frontend == "json_array") {
      if (!skip_invalid_json_sources) {
        return group_status;
      }
    } else {
      return group_status;
    }
    for (std::size_t single = group.start; single < group.end; ++single) {
      SAN_RETURN_NOT_OK(merge_single_source(sources[single]));
    }
    return sanitize::Status::OK();
  };

  for (std::size_t i = 0; i < sources.size();) {
    if (!check_python_signals()) {
      return sanitize::Status::Cancelled("Python signal received");
    }
    SAN_ASSIGN_OR_RAISE(
        auto group,
        next_path_source_group_plan(sources, i, PathSourceGroupPurpose::kProbe,
                                    prepared->spec.input_text_encoding));
    if (group.grouped) {
      auto prepared_ingest = prepare_path_source_group_ingest(
          ctx, prepared, sources, group.start, group.end);
      if (prepared_ingest.ok()) {
        SAN_RETURN_NOT_OK(
            merge_ingest(std::move(prepared_ingest).ValueOrDie()));
      } else {
        SAN_RETURN_NOT_OK(merge_group_sources(group, prepared_ingest.status()));
      }
      i = group.end;
      continue;
    }
    SAN_RETURN_NOT_OK(merge_single_source(sources[i]));
    i = group.end;
  }
  if (!has_schema) {
    if (skipped_errors.empty()) {
      return sanitize::Status::Invalid("sources must not be empty");
    }
    std::string detail;
    const std::size_t first =
        skipped_errors.size() > 3 ? skipped_errors.size() - 3 : 0;
    for (std::size_t i = first; i < skipped_errors.size(); ++i) {
      if (!detail.empty()) {
        detail.append("; ");
      }
      detail.append(skipped_errors[i]);
    }
    return sanitize::Status::Invalid(
        "Schema warm-up found no valid JSON sources: ", detail);
  }

  auto merge_input = make_registry_merge_input(std::move(combined_schema),
                                               registry_json, field_name_policy,
                                               prepared->spec.default_key_name);
  sanitize::Result<sanitize::SchemaRegistryMergeResult> final_merged_r =
      previous_schema ? sanitize::merge_schema_registry_with_previous_schema(
                            merge_input, *previous_schema)
                      : sanitize::merge_schema_registry(merge_input);
  if (!final_merged_r.ok()) {
    return final_merged_r.status();
  }
  auto final_merged = std::move(final_merged_r).ValueOrDie();
  return PathSourceRegistryProbeResult{std::move(final_merged), diagnostics};
}

} // namespace core_abi3_internal

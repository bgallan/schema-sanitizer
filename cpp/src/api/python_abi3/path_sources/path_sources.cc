// Shared native path-source helpers for Python ABI3 wrappers.

#include "api/python_abi3/path_sources/path_sources.hh"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "api/c/schema_sanitizer_c_sink_internal.hh"
#include "frontends/builtin_frontends.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/streaming/csv/scanner.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/registry/registry.hh"

namespace core_abi3_internal {
namespace {
constexpr std::array<std::string_view, 5> kDirectPathSourceFrontends = {
    "json", "jsonl", "json_array", "csv", "xml"};

std::int64_t
input_size_hint_from_paths(const std::vector<std::string> &paths) noexcept {
  std::uintmax_t total = 0;
  constexpr auto maximum =
      static_cast<std::uintmax_t>(std::numeric_limits<std::int64_t>::max());
  for (const auto &path : paths) {
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (error) {
      return 0;
    }
    if (size > maximum - total) {
      return std::numeric_limits<std::int64_t>::max();
    }
    total += size;
  }
  return static_cast<std::int64_t>(total);
}

sanitize::Result<std::optional<std::vector<std::string>>>
csv_header_from_path_source(const PathSourceSpec &source,
                            const sanitize::PreparedOptionsPtr &prepared) {
  SAN_ASSIGN_OR_RAISE(auto chunk_source,
                      sanitize::chunk_source_from_path_with_encoding(
                          source.path, prepared->spec.input_text_encoding,
                          prepared->spec.memory_limit_bytes));
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
    SAN_RETURN_NOT_OK(sanitize::internal::parse_csv_cells(
        record.view,
        prepared->spec.csv_delimiter.empty() ? ','
                                             : prepared->spec.csv_delimiter[0],
        &views, &arena));
    std::vector<std::string> header;
    header.reserve(views.size());
    for (std::string_view value : views) {
      header.emplace_back(value);
    }
    return std::optional<std::vector<std::string>>(std::move(header));
  }
}

bool is_json_path_source(const PathSourceSpec &source) {
  return source.frontend == "json" || source.frontend == "jsonl";
}

bool is_json_array_path_source(const PathSourceSpec &source) {
  return source.frontend == "json_array";
}

bool is_utf8_text_encoding(std::string_view encoding) {
  return encoding == "utf-8";
}

bool path_has_extension(const std::string &path, std::string_view extension) {
  return path.size() >= extension.size() &&
         path.compare(path.size() - extension.size(), extension.size(),
                      extension) == 0;
}

bool is_json_lines_path_source(const PathSourceSpec &source) {
  return source.frontend == "jsonl" ||
         (source.frontend == "json" &&
          (path_has_extension(source.path, ".jsonl") ||
           path_has_extension(source.path, ".ndjson")));
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
                             std::string_view input_text_encoding,
                             std::int64_t memory_limit_bytes) {
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
  input.input_size_hint_bytes = input_size_hint_from_paths(paths);
  if (frontend_name == "jsonl") {
    input.paths = std::move(paths);
    input.source_names = std::move(source_names);
    return input;
  }
  if (frontend_name == "json_array" || frontend_name == "json_array_document") {
    input.paths = std::move(paths);
    input.source_names = std::move(source_names);
    return input;
  }

  SAN_ASSIGN_OR_RAISE(
      input.chunk_source,
      sanitize::chunk_source_from_paths_with_source_names_encoding(
          std::move(paths), std::move(source_names), "\n", input_text_encoding,
          memory_limit_bytes));
  return input;
}

} // namespace

sanitize::Result<sanitize::FrontendHandle> path_source_frontend(
    PathSourceInput input, const sanitize::Options &options,
    std::shared_ptr<sanitize::internal::OperationTaskArena> task_arena) {
  if (input.frontend == "jsonl" && !input.paths.empty()) {
    return sanitize::internal::make_jsonl_path_group_frontend(
        std::move(input.paths), std::move(input.source_names), options,
        std::move(task_arena));
  }
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
  if (source.frontend != "json" && source.frontend != "jsonl" &&
      source.frontend != "json_array") {
    return false;
  }
  const std::string &message = status.message();
  return message.contains("JSON parse error") ||
         message.contains("Invalid JSON file");
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

sanitize::Result<PathSourceInput>
path_source_input(const sanitize::PreparedOptionsPtr &prepared,
                  const PathSourceSpec &source) {
  (void)prepared;
  if (std::find(kDirectPathSourceFrontends.cbegin(),
                kDirectPathSourceFrontends.cend(),
                std::string_view(source.frontend)) !=
      kDirectPathSourceFrontends.cend()) {
    SAN_ASSIGN_OR_RAISE(auto chunk_source,
                        sanitize::chunk_source_from_path_with_encoding(
                            source.path, prepared->spec.input_text_encoding,
                            prepared->spec.memory_limit_bytes));
    PathSourceInput input;
    input.frontend =
        is_json_lines_path_source(source) ? "jsonl" : source.frontend;
    input.input_size_hint_bytes =
        input_size_hint_from_paths(std::vector<std::string>{source.path});
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
      const bool all_json_lines =
          std::all_of(sources.begin() + static_cast<std::ptrdiff_t>(start),
                      sources.begin() + static_cast<std::ptrdiff_t>(end),
                      is_json_lines_path_source);
      return PathSourceGroupPlan{.start = start,
                                 .end = end,
                                 .frontend = all_json_lines ? "jsonl" : "json",
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
                        std::string_view input_text_encoding,
                        std::int64_t memory_limit_bytes) {
  if (!group.grouped) {
    return sanitize::Status::Invalid("path-source group is not grouped");
  }
  return make_path_source_group_input(sources, group, input_text_encoding,
                                      memory_limit_bytes);
}

} // namespace core_abi3_internal

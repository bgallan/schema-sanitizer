// Declares built-in frontend factories used by registry dispatch.

#pragma once

#include "sanitize/core/row_stream.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/options/options.hh"

#include <memory>

#include "frontends/csv/source_projection.hh"
#include <string>
#include <vector>

namespace sanitize::internal {

class OperationTaskArena;

// Creates the built-in CSV frontend.
FrontendHandle
make_csv_frontend(ChunkSourcePtr csv, const Options &options,
                  CsvSourceProjectionSetPtr source_projections = nullptr);

// Creates the built-in JSON frontend.
FrontendHandle make_json_frontend(ChunkSourcePtr json, const Options &options);

// Creates the built-in newline-delimited JSON frontend.
FrontendHandle make_jsonl_frontend(ChunkSourcePtr json, const Options &options);

// Creates an ordered JSONL frontend that prefetches independent paths.
sanitize::Result<FrontendHandle> make_jsonl_path_group_frontend(
    std::vector<std::string> paths, std::vector<std::string> source_names,
    const Options &options, std::shared_ptr<OperationTaskArena> task_arena);

// Creates the built-in JSON-array frontend.
FrontendHandle make_json_array_frontend(ChunkSourcePtr json,
                                        const Options &options);

// Creates a JSON-array frontend over multiple files, preserving source names
// while parsing each path as its own top-level JSON array.
FrontendHandle
make_json_array_group_frontend(std::vector<std::string> paths,
                               std::vector<std::string> source_names,
                               const Options &options);

// Creates a JSON frontend over multiple top-level array files, preserving
// source names while retaining the broader input_format=json row semantics.
FrontendHandle
make_json_document_array_group_frontend(std::vector<std::string> paths,
                                        std::vector<std::string> source_names,
                                        const Options &options);

// Creates the built-in XML frontend.
FrontendHandle make_xml_frontend(ChunkSourcePtr xml, const Options &options);

} // namespace sanitize::internal

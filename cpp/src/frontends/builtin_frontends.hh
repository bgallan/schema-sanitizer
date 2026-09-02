// Declares built-in frontend factories used by registry dispatch. These
// factories hide format-specific lifecycle state behind the common
// `FrontendHandle` vtable.

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

/// Creates the built-in CSV frontend.
FrontendHandle
make_csv_frontend(ChunkSourcePtr csv, const Options &options,
                  CsvSourceProjectionSetPtr source_projections = nullptr);

/// Creates the built-in JSON frontend.
FrontendHandle make_json_frontend(ChunkSourcePtr json, const Options &options);

/// Creates the built-in newline-delimited JSON frontend.
FrontendHandle make_jsonl_frontend(ChunkSourcePtr json, const Options &options);

/// Creates an ordered JSONL frontend that prefetches independent paths.
sanitize::Result<FrontendHandle> make_jsonl_path_group_frontend(
    std::vector<std::string> paths, std::vector<std::string> source_names,
    const Options &options, std::shared_ptr<OperationTaskArena> task_arena);

/// Creates the built-in JSON-array frontend.
FrontendHandle make_json_array_frontend(ChunkSourcePtr json,
                                        const Options &options);

/// Creates a multi-file frontend that parses each path as a top-level JSON
/// array. The frontend preserves the source name associated with each path.
FrontendHandle
make_json_array_group_frontend(std::vector<std::string> paths,
                               std::vector<std::string> source_names,
                               const Options &options);

/// Creates a multi-file JSON frontend over top-level array documents.
/// It preserves source names while retaining general JSON row semantics.
FrontendHandle
make_json_document_array_group_frontend(std::vector<std::string> paths,
                                        std::vector<std::string> source_names,
                                        const Options &options);

/// Creates the built-in XML frontend.
FrontendHandle make_xml_frontend(ChunkSourcePtr xml, const Options &options);

} // namespace sanitize::internal

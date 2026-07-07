// Declares generated file metadata helpers shared by file sinks.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "sanitize/core/status.hh"

namespace sanitize {

struct FileMetadataInput {
  std::string input_path;
};

using FileMetadataColumns = std::vector<std::pair<std::string, std::string>>;

// Builds generated per-file metadata columns.
Result<FileMetadataColumns>
generated_file_metadata_columns(const FileMetadataInput &input);

// Returns the current UTC timestamp as an ISO-8601 string with microseconds.
Result<std::string> current_utc_iso_timestamp();

} // namespace sanitize

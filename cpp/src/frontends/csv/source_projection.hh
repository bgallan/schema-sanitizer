// Immutable CSV source-header and physical-column projection metadata.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace sanitize::internal {

// Header bytes observed for one physical CSV source before schema inference.
struct CsvSourceHeader {
  std::size_t source_index = 0;
  std::vector<std::string> fields;
};

// Maps physical cells from one source to stable logical source keys.
struct CsvSourceProjection {
  std::size_t source_index = 0;
  std::vector<std::string> column_keys;
  std::vector<std::uint64_t> column_hashes;
};

// Shared immutable metadata for every source in one grouped CSV frontend.
struct CsvSourceProjectionSet {
  std::vector<CsvSourceHeader> headers;
  std::vector<CsvSourceProjection> projections;
  std::size_t max_columns = 0;
  std::size_t resident_bytes = 0;
  std::shared_ptr<void> resident_memory_lease;
};

using CsvSourceProjectionSetPtr = std::shared_ptr<const CsvSourceProjectionSet>;

} // namespace sanitize::internal

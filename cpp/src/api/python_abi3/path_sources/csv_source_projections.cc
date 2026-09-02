// Pre-reads immutable CSV headers and builds per-source physical projections.
// The helpers preserve source order, format grouping, and bounded ownership
// across multi-file operations.

#include "api/python_abi3/path_sources/path_sources.hh"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "frontends/csv/column_projection.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/streaming/csv/scanner.hh"
#include "sanitize/detail/hash.hh"
#include "sanitize/ingest/chunk_source.hh"

namespace core_abi3_internal {
namespace {

/// Reads the first CSV record from one path source as an optional header.
sanitize::Result<std::optional<std::vector<std::string>>>
csv_header_from_path_source(const PathSourceSpec &source,
                            const sanitize::PreparedOptionsPtr &prepared) {
  SAN_ASSIGN_OR_RAISE(auto chunk_source,
                      sanitize::chunk_source_from_path_with_encoding(
                          source.path, prepared->spec.input_text_encoding,
                          prepared->spec.memory_limit_bytes));
  sanitize::internal::PoolResource pmr_pool;
  sanitize::internal::CsvStreamingScanner scanner(
      std::move(chunk_source), sanitize::internal::kDefaultCsvChunkBytes,
      sanitize::internal::kMaxCsvRecordBytes,
      sanitize::internal::kMaxCsvRecordSegments, pmr_pool.pool(),
      prepared->spec.csv_escape_char.empty()
          ? '\0'
          : prepared->spec.csv_escape_char[0]);
  SAN_RETURN_NOT_OK(scanner.Reset());

  sanitize::internal::BumpArena arena(pmr_pool.pool());
  for (;;) {
    arena.reset();
    SAN_ASSIGN_OR_RAISE(auto record, scanner.next_record(&arena));
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
        &views, &arena, record.base_offset,
        sanitize::internal::kMaxCsvFieldBytes,
        sanitize::internal::kMaxCsvDecodedRecordBytes,
        prepared->spec.csv_escape_char.empty()
            ? '\0'
            : prepared->spec.csv_escape_char[0]));
    std::vector<std::string> header;
    header.reserve(views.size());
    for (std::string_view value : views) {
      header.emplace_back(value);
    }
    return std::optional<std::vector<std::string>>(std::move(header));
  }
}

/// Parses a decimal CSV column key into its zero-based physical index.
std::string numeric_column_key(std::size_t index) {
  char buffer[std::numeric_limits<std::size_t>::digits10 + 2];
  const auto [end, error] =
      std::to_chars(std::begin(buffer), std::end(buffer), index);
  if (error != std::errc{}) [[unlikely]] {
    std::unreachable();
  }
  return std::string(buffer, end);
}

/// Adds byte or item counts while clamping overflow to the representable
/// maximum.
std::size_t saturating_add(std::size_t left, std::size_t right) noexcept {
  if (right > std::numeric_limits<std::size_t>::max() - left) {
    return std::numeric_limits<std::size_t>::max();
  }
  return left + right;
}

/// Multiplies byte or item counts while clamping overflow to the representable
/// maximum.
std::size_t saturating_multiply(std::size_t count, std::size_t width) noexcept {
  if (width != 0U && count > std::numeric_limits<std::size_t>::max() / width) {
    return std::numeric_limits<std::size_t>::max();
  }
  return count * width;
}

/// Estimates retained bytes for immutable per-source CSV projection metadata.
std::size_t projection_set_resident_bytes(
    const sanitize::internal::CsvSourceProjectionSet &set) noexcept {
  auto total = sizeof(set);
  total = saturating_add(
      total, saturating_multiply(set.headers.capacity(),
                                 sizeof(sanitize::internal::CsvSourceHeader)));
  total = saturating_add(
      total,
      saturating_multiply(set.projections.capacity(),
                          sizeof(sanitize::internal::CsvSourceProjection)));
  for (const auto &header : set.headers) {
    total = saturating_add(total, saturating_multiply(header.fields.capacity(),
                                                      sizeof(std::string)));
    for (const auto &field : header.fields) {
      total = saturating_add(total, field.capacity());
    }
  }
  for (const auto &projection : set.projections) {
    total = saturating_add(
        total, saturating_multiply(projection.column_keys.capacity(),
                                   sizeof(std::string)));
    total = saturating_add(
        total, saturating_multiply(projection.column_hashes.capacity(),
                                   sizeof(std::uint64_t)));
    for (const auto &key : projection.column_keys) {
      total = saturating_add(total, key.capacity());
    }
  }
  return total;
}

class ProjectionMemoryLease final {
public:
  /// Holds a projection metadata charge against the operation memory ledger.
  ProjectionMemoryLease(
      std::shared_ptr<sanitize::internal::OperationMemoryLedger> ledger,
      std::int64_t bytes) noexcept
      : ledger_(std::move(ledger)), bytes_(bytes) {}

  /// Disables copying so the memory charge is released exactly once.
  ProjectionMemoryLease(const ProjectionMemoryLease &) = delete;

  /// Disables copy assignment so ledger ownership cannot be duplicated.
  ProjectionMemoryLease &operator=(const ProjectionMemoryLease &) = delete;

  /// Returns the retained projection charge to its operation ledger.
  ~ProjectionMemoryLease() {
    if (ledger_) {
      ledger_->Release(bytes_);
    }
  }

private:
  std::shared_ptr<sanitize::internal::OperationMemoryLedger> ledger_;
  std::int64_t bytes_ = 0;
};

} // namespace

/// Pre-reads CSV headers and builds immutable per-source union projections.
sanitize::Result<sanitize::internal::CsvSourceProjectionSetPtr>
csv_source_projections_from_path_sources(
    std::span<const PathSourceSpec> sources,
    const sanitize::PreparedOptionsPtr &prepared) try {
  if (!prepared || !prepared->spec.csv_has_header ||
      prepared->spec.csv_header_mode != "union") {
    return sanitize::internal::CsvSourceProjectionSetPtr{};
  }

  auto mutable_set =
      std::make_shared<sanitize::internal::CsvSourceProjectionSet>();
  mutable_set->headers.reserve(sources.size());
  mutable_set->projections.reserve(sources.size());
  sanitize::internal::CsvColumnProjection validator(
      prepared->spec, prepared->spec.csv_delimiter.empty()
                          ? ','
                          : prepared->spec.csv_delimiter[0]);

  bool observed_header = false;
  bool observed_missing_header = false;
  for (std::size_t source_index = 0; source_index < sources.size();
       ++source_index) {
    const auto &source = sources[source_index];
    if (source.frontend != "csv") {
      return sanitize::Status::Invalid(
          "CSV union projection group contains a non-CSV source");
    }
    SAN_ASSIGN_OR_RAISE(auto header,
                        csv_header_from_path_source(source, prepared));
    if (!header) {
      observed_missing_header = true;
      continue;
    }
    observed_header = true;
    std::vector<std::string_view> views;
    views.reserve(header->size());
    for (const auto &field : *header) {
      views.emplace_back(field);
    }
    SAN_RETURN_NOT_OK(validator.validate_header_cells(views));

    sanitize::internal::CsvSourceHeader source_header{
        .source_index = source_index, .fields = std::move(*header)};
    sanitize::internal::CsvSourceProjection projection;
    projection.source_index = source_index;
    projection.column_keys.reserve(source_header.fields.size());
    projection.column_hashes.reserve(source_header.fields.size());
    for (std::size_t column = 0; column < source_header.fields.size();
         ++column) {
      std::string key = source_header.fields[column].empty()
                            ? numeric_column_key(column)
                            : source_header.fields[column];
      projection.column_hashes.push_back(sanitize::detail::hash_key64(key));
      projection.column_keys.push_back(std::move(key));
    }
    mutable_set->max_columns =
        std::max(mutable_set->max_columns, source_header.fields.size());
    mutable_set->headers.push_back(std::move(source_header));
    mutable_set->projections.push_back(std::move(projection));
  }

  if (observed_header && observed_missing_header) {
    return sanitize::Status::Invalid(
        "CSV union mode cannot mix sources with and without headers");
  }
  if (!observed_header) {
    return sanitize::internal::CsvSourceProjectionSetPtr{};
  }
  mutable_set->resident_bytes = projection_set_resident_bytes(*mutable_set);
  if (prepared->spec.memory_limit_bytes > 0 &&
      mutable_set->resident_bytes >
          static_cast<std::size_t>(prepared->spec.memory_limit_bytes)) {
    return sanitize::Status::OutOfMemory(
        "CSV union source projections exceed memory_limit_bytes");
  }
  if (prepared->operation_memory_ledger) {
    auto ledger =
        std::static_pointer_cast<sanitize::internal::OperationMemoryLedger>(
            prepared->operation_memory_ledger);
    const auto resident_bytes =
        mutable_set->resident_bytes >
                static_cast<std::size_t>(
                    std::numeric_limits<std::int64_t>::max())
            ? std::numeric_limits<std::int64_t>::max()
            : static_cast<std::int64_t>(mutable_set->resident_bytes);
    SAN_RETURN_NOT_OK(
        ledger->Reserve(resident_bytes, "CSV union source projections"));
    try {
      mutable_set->resident_memory_lease =
          std::make_shared<ProjectionMemoryLease>(ledger, resident_bytes);
    } catch (const std::bad_alloc &) {
      ledger->Release(resident_bytes);
      return sanitize::Status::OutOfMemory(
          "CSV union projection memory lease allocation failed");
    }
  }
  return sanitize::internal::CsvSourceProjectionSetPtr(std::move(mutable_set));
} catch (const std::bad_alloc &) {
  return sanitize::Status::OutOfMemory(
      "CSV union source projection allocation failed");
}

/// Preflights CSV headers for union projection or exact cross-file equality.
sanitize::Status
validate_csv_path_source_headers(const std::vector<PathSourceSpec> &sources,
                                 const sanitize::PreparedOptionsPtr &prepared) {
  if (!prepared || !prepared->spec.csv_has_header) {
    return sanitize::Status::OK();
  }
  if (prepared->spec.csv_header_mode == "union") {
    for (std::size_t start = 0; start < sources.size();) {
      if (sources[start].frontend != "csv") {
        ++start;
        continue;
      }
      std::size_t end = start + 1;
      while (end < sources.size() && sources[end].frontend == "csv") {
        ++end;
      }
      SAN_ASSIGN_OR_RAISE(auto projections,
                          csv_source_projections_from_path_sources(
                              std::span<const PathSourceSpec>(sources).subspan(
                                  start, end - start),
                              prepared));
      (void)projections;
      start = end;
    }
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

} // namespace core_abi3_internal

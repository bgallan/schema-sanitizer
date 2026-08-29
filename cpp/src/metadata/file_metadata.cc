// Implements generated per-file metadata shared by native file sinks.
// Timestamp formatting and source-column construction use stable UTC and field
// naming contracts before data reaches an output adapter.

#include "sanitize/metadata/file_metadata.hh"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <string>
#include <utility>

namespace sanitize {

Result<std::string> current_utc_iso_timestamp() {
  const auto now = std::chrono::system_clock::now();
  const auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(now);
  const auto micros =
      std::chrono::duration_cast<std::chrono::microseconds>(now - seconds)
          .count();
  const std::time_t now_seconds = std::chrono::system_clock::to_time_t(seconds);
  std::tm utc{};

#if defined(_WIN32)
  if (gmtime_s(&utc, &now_seconds) != 0) {
    return Status::Invalid("could not compute UTC conversion timestamp");
  }
#else
  if (gmtime_r(&now_seconds, &utc) == nullptr) {
    return Status::Invalid("could not compute UTC conversion timestamp");
  }
#endif

  char buffer[28] = {};
  const int written = std::snprintf(
      buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d.%06lldZ",
      utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday, utc.tm_hour, utc.tm_min,
      utc.tm_sec, static_cast<long long>(micros));
  if (written != 27) {
    return Status::Invalid("could not format UTC conversion timestamp");
  }
  return std::string(buffer, 27);
}

Result<FileMetadataColumns>
generated_file_metadata_columns(const FileMetadataInput &input) {
  FileMetadataColumns columns;
  columns.reserve(1);
  columns.emplace_back("source_file", input.input_path);
  return columns;
}

} // namespace sanitize

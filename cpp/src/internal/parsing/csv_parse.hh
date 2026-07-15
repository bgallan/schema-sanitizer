// Provides the internal lightweight CSV cell parser.

#pragma once

#include <cstddef>
#include <cstring>
#include <string_view>
#include <vector>

#include "internal/memory/arena.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal {

inline constexpr std::size_t kMaxCsvCellsPerRecord = 65'536;

// Trims spaces and tabs from an unquoted CSV cell.
inline std::string_view trim_csv_cell(std::string_view cell) {
  while (!cell.empty() && (cell.front() == ' ' || cell.front() == '\t')) {
    cell.remove_prefix(1);
  }
  while (!cell.empty() && (cell.back() == ' ' || cell.back() == '\t')) {
    cell.remove_suffix(1);
  }
  return cell;
}

// Returns the decoded size and advances to the first byte after the closing
// quote (or the end of a malformed unterminated field).
inline std::size_t scan_quoted_csv_cell(std::string_view row, std::size_t start,
                                        std::size_t *end_position) {
  std::size_t decoded_size = 0;
  std::size_t position = start + 1;
  while (position < row.size()) {
    const char character = row[position];
    if (character == '\r') {
      ++decoded_size;
      if (position + 1 < row.size() && row[position + 1] == '\n') {
        position += 2;
      } else {
        ++position;
      }
      continue;
    }
    if (character != '"') {
      ++decoded_size;
      ++position;
      continue;
    }
    if (position + 1 < row.size() && row[position + 1] == '"') {
      ++decoded_size;
      position += 2;
      continue;
    }
    ++position;
    break;
  }
  *end_position = position;
  return decoded_size;
}

// Parses and unescapes one quoted CSV cell directly into arena storage. The
// previous implementation built a temporary std::string and then copied it
// into the arena, doubling peak memory for exceptionally large quoted cells.
inline std::string_view parse_quoted_csv_cell(std::string_view row,
                                              std::size_t *position,
                                              BumpArena *arena) {
  const std::size_t start = *position;
  std::size_t end_position = start;
  const std::size_t decoded_size =
      scan_quoted_csv_cell(row, start, &end_position);
  *position = end_position;
  if (arena == nullptr || decoded_size == 0) {
    return {};
  }

  auto *destination =
      static_cast<char *>(arena->alloc(decoded_size, alignof(char)));
  std::size_t source = start + 1;
  std::size_t written = 0;
  while (source < end_position && written < decoded_size) {
    const char character = row[source];
    if (character == '\r') {
      destination[written++] = '\n';
      if (source + 1 < end_position && row[source + 1] == '\n') {
        source += 2;
      } else {
        ++source;
      }
      continue;
    }
    if (character != '"') {
      destination[written++] = character;
      ++source;
      continue;
    }
    if (source + 1 < end_position && row[source + 1] == '"') {
      destination[written++] = '"';
      source += 2;
      continue;
    }
    break;
  }
  return {destination, written};
}

// Advances past whitespace and an optional delimiter after a quoted cell.
inline void advance_after_quoted_csv_cell(std::string_view row,
                                          std::size_t *position,
                                          char delimiter) {
  while (*position < row.size() && row[*position] != delimiter &&
         (row[*position] == ' ' || row[*position] == '\t')) {
    ++*position;
  }
  if (*position < row.size() && row[*position] == delimiter) {
    ++*position;
  }
}

inline sanitize::Status append_csv_cell(std::vector<std::string_view> *out,
                                        std::string_view value) {
  if (out->size() >= kMaxCsvCellsPerRecord) {
    return sanitize::Status::Invalid(
        "CSV record cell count exceeds safety limit: ", out->size() + 1U, " > ",
        kMaxCsvCellsPerRecord);
  }
  out->push_back(value);
  return sanitize::Status::OK();
}

// Small CSV cell parser supporting quotes and escaped quotes.
// - unquoted cells are returned as views into `row`
// - quoted cells are unescaped directly into `arena` and returned as views
// - leading/trailing spaces/tabs in unquoted cells are trimmed
inline sanitize::Status parse_csv_cells(std::string_view row, char delimiter,
                                        std::vector<std::string_view> *out,
                                        BumpArena *arena) {
  if (out == nullptr) {
    return sanitize::Status::Invalid("CSV cell output vector is null");
  }
  out->clear();

  const char delim = (delimiter == '\0') ? ',' : delimiter;

  std::size_t i = 0;
  while (i < row.size()) {
    if (row[i] == delim) {
      SAN_RETURN_NOT_OK(append_csv_cell(out, {}));
      ++i;
      continue;
    }

    if (row[i] == '"') {
      const std::string_view value = parse_quoted_csv_cell(row, &i, arena);
      SAN_RETURN_NOT_OK(append_csv_cell(out, value));
      advance_after_quoted_csv_cell(row, &i, delim);
      continue;
    }

    const std::size_t start = i;
    while (i < row.size() && row[i] != delim) {
      ++i;
    }
    std::string_view cell = row.substr(start, i - start);
    if (i < row.size() && row[i] == delim) {
      ++i;
    }
    SAN_RETURN_NOT_OK(append_csv_cell(out, trim_csv_cell(cell)));
  }

  if (!row.empty() && row.back() == delim) {
    SAN_RETURN_NOT_OK(append_csv_cell(out, {}));
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal

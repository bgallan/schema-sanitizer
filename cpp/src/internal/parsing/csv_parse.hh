// Provides the internal lightweight CSV cell parser.

#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "internal/memory/arena.hh"

namespace sanitize::internal {

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

// Parses and unescapes one quoted CSV cell.
inline std::string_view parse_quoted_csv_cell(std::string_view row,
                                              std::size_t *position,
                                              BumpArena *arena) {
  ++*position;
  std::string value;
  value.reserve(32);
  while (*position < row.size()) {
    const char character = row[*position];
    if (character == '\r') {
      value.push_back('\n');
      if (*position + 1 < row.size() && row[*position + 1] == '\n') {
        *position += 2;
      } else {
        ++*position;
      }
      continue;
    }
    if (character != '"') {
      value.push_back(character);
      ++*position;
      continue;
    }
    if (*position + 1 < row.size() && row[*position + 1] == '"') {
      value.push_back('"');
      *position += 2;
      continue;
    }
    ++*position;
    break;
  }

  if (arena == nullptr) {
    return {};
  }
  return arena->append(value);
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

// Small CSV cell parser supporting quotes and escaped quotes.
// - unquoted cells are returned as views into `row`
// - quoted cells are unescaped into `arena` (if non-null) and returned as views
// there
// - leading/trailing spaces/tabs in unquoted cells are trimmed
inline void parse_csv_cells(std::string_view row, char delimiter,
                            std::vector<std::string_view> *out,
                            BumpArena *arena) {
  out->clear();

  const char delim = (delimiter == '\0') ? ',' : delimiter;

  std::size_t i = 0;
  while (i < row.size()) {
    if (row[i] == delim) {
      out->emplace_back();
      ++i;
      continue;
    }

    if (row[i] == '"') {
      out->push_back(parse_quoted_csv_cell(row, &i, arena));
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
    out->push_back(trim_csv_cell(cell));
  }

  if (!row.empty() && row.back() == delim) {
    out->emplace_back();
  }
}

} // namespace sanitize::internal

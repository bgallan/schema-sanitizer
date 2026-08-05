// Provides the internal strict, budget-aware CSV cell parser.

#pragma once

#include <cstddef>
#include <limits>
#include <new>
#include <string_view>
#include <vector>

#include "internal/memory/arena.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal {

inline constexpr std::size_t kMaxCsvCellsPerRecord = 65'536;
inline constexpr std::size_t kMaxCsvFieldBytes =
    static_cast<std::size_t>(64) * 1024U * 1024U;
inline constexpr std::size_t kMaxCsvDecodedRecordBytes =
    static_cast<std::size_t>(256) * 1024U * 1024U;

inline sanitize::Status validate_csv_utf8(std::string_view text,
                                          std::size_t base_offset = 0) {
  std::size_t position = 0;
  while (position < text.size()) {
    const auto lead = static_cast<unsigned char>(text[position]);
    if (lead < 0x80U) {
      ++position;
      continue;
    }

    std::size_t length = 0;
    std::uint32_t code_point = 0;
    std::uint32_t minimum = 0;
    if (lead >= 0xC2U && lead <= 0xDFU) {
      length = 2;
      code_point = lead & 0x1FU;
      minimum = 0x80U;
    } else if (lead >= 0xE0U && lead <= 0xEFU) {
      length = 3;
      code_point = lead & 0x0FU;
      minimum = 0x800U;
    } else if (lead >= 0xF0U && lead <= 0xF4U) {
      length = 4;
      code_point = lead & 0x07U;
      minimum = 0x10000U;
    } else {
      return sanitize::Status::Invalid("CSV parse error at byte ",
                                       base_offset + position,
                                       ": invalid UTF-8 leading byte");
    }

    if (length > text.size() - position) {
      return sanitize::Status::Invalid("CSV parse error at byte ",
                                       base_offset + position,
                                       ": truncated UTF-8 sequence");
    }
    for (std::size_t offset = 1; offset < length; ++offset) {
      const auto continuation =
          static_cast<unsigned char>(text[position + offset]);
      if ((continuation & 0xC0U) != 0x80U) {
        return sanitize::Status::Invalid("CSV parse error at byte ",
                                         base_offset + position + offset,
                                         ": invalid UTF-8 continuation byte");
      }
      code_point = (code_point << 6U) | (continuation & 0x3FU);
    }
    if (code_point < minimum || code_point > 0x10FFFFU ||
        (code_point >= 0xD800U && code_point <= 0xDFFFU)) {
      return sanitize::Status::Invalid("CSV parse error at byte ",
                                       base_offset + position,
                                       ": invalid UTF-8 scalar value");
    }
    position += length;
  }
  return sanitize::Status::OK();
}

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

struct QuotedCsvCellScan {
  std::size_t decoded_size = 0;
  std::size_t end_position = 0;
};

// Returns the decoded size and the first byte after a required closing quote.
inline sanitize::Result<QuotedCsvCellScan>
scan_quoted_csv_cell(std::string_view row, std::size_t start,
                     std::size_t base_offset = 0,
                     std::size_t max_field_bytes = kMaxCsvFieldBytes) {
  QuotedCsvCellScan out;
  std::size_t position = start + 1U;
  while (position < row.size()) {
    const char character = row[position];
    if (character == '\r') {
      if (out.decoded_size == std::numeric_limits<std::size_t>::max()) {
        return sanitize::Status::OutOfMemory(
            "CSV decoded field size overflow at byte ", base_offset + position);
      }
      ++out.decoded_size;
      if (out.decoded_size > max_field_bytes) {
        return sanitize::Status::OutOfMemory(
            "CSV decoded field size exceeds effective limit at byte ",
            base_offset + position, ": ", out.decoded_size, " > ",
            max_field_bytes);
      }
      position +=
          (position + 1U < row.size() && row[position + 1U] == '\n') ? 2U : 1U;
      continue;
    }
    if (character != '"') {
      if (out.decoded_size == std::numeric_limits<std::size_t>::max()) {
        return sanitize::Status::OutOfMemory(
            "CSV decoded field size overflow at byte ", base_offset + position);
      }
      ++out.decoded_size;
      if (out.decoded_size > max_field_bytes) {
        return sanitize::Status::OutOfMemory(
            "CSV decoded field size exceeds effective limit at byte ",
            base_offset + position, ": ", out.decoded_size, " > ",
            max_field_bytes);
      }
      ++position;
      continue;
    }
    if (position + 1U < row.size() && row[position + 1U] == '"') {
      if (out.decoded_size == std::numeric_limits<std::size_t>::max()) {
        return sanitize::Status::OutOfMemory(
            "CSV decoded field size overflow at byte ", base_offset + position);
      }
      ++out.decoded_size;
      if (out.decoded_size > max_field_bytes) {
        return sanitize::Status::OutOfMemory(
            "CSV decoded field size exceeds effective limit at byte ",
            base_offset + position, ": ", out.decoded_size, " > ",
            max_field_bytes);
      }
      position += 2U;
      continue;
    }
    out.end_position = position + 1U;
    return out;
  }
  return sanitize::Status::Invalid(
      "CSV parse error at byte ", base_offset + start,
      ": unterminated quoted field at end of record");
}

// Parses and unescapes one quoted CSV cell directly into arena storage.
inline sanitize::Result<std::string_view>
parse_quoted_csv_cell(std::string_view row, std::size_t *position,
                      BumpArena *arena, std::size_t base_offset = 0,
                      std::size_t max_field_bytes = kMaxCsvFieldBytes) {
  if (!position) {
    return sanitize::Status::Invalid("CSV quoted field position is null");
  }
  const std::size_t start = *position;
  SAN_ASSIGN_OR_RAISE(
      const auto scan,
      scan_quoted_csv_cell(row, start, base_offset, max_field_bytes));
  *position = scan.end_position;
  const std::size_t decoded_size = scan.decoded_size;
  if (decoded_size == 0U) {
    return std::string_view{};
  }
  if (!arena) {
    return sanitize::Status::Invalid(
        "CSV quoted field arena is null for non-empty decoded value");
  }

  auto *destination =
      static_cast<char *>(arena->alloc(decoded_size, alignof(char)));
  std::size_t source = start + 1U;
  std::size_t written = 0;
  while (source < scan.end_position - 1U) {
    const char character = row[source];
    if (character == '\r') {
      destination[written++] = '\n';
      source +=
          (source + 1U < scan.end_position - 1U && row[source + 1U] == '\n')
              ? 2U
              : 1U;
      continue;
    }
    if (character == '"' && source + 1U < scan.end_position - 1U &&
        row[source + 1U] == '"') {
      destination[written++] = '"';
      source += 2U;
      continue;
    }
    destination[written++] = character;
    ++source;
  }
  return std::string_view(destination, written);
}

// Validates bytes after a closing quote and advances over one delimiter.
inline sanitize::Status
advance_after_quoted_csv_cell(std::string_view row, std::size_t *position,
                              char delimiter, std::size_t base_offset = 0) {
  while (*position < row.size() &&
         (row[*position] == ' ' || row[*position] == '\t')) {
    ++*position;
  }
  if (*position == row.size()) {
    return sanitize::Status::OK();
  }
  if (row[*position] != delimiter) {
    return sanitize::Status::Invalid("CSV parse error at byte ",
                                     base_offset + *position,
                                     ": unexpected byte after closing quote");
  }
  ++*position;
  return sanitize::Status::OK();
}

template <typename CellVector>
inline sanitize::Status append_csv_cell(CellVector *out,
                                        std::string_view value) {
  if (out->size() >= kMaxCsvCellsPerRecord) {
    return sanitize::Status::Invalid(
        "CSV record cell count exceeds safety limit: ", out->size() + 1U, " > ",
        kMaxCsvCellsPerRecord);
  }
  out->push_back(value);
  return sanitize::Status::OK();
}

// Parses one strict CSV record. Quoted fields must close, doubled quotes are
// decoded, and only whitespace, a delimiter, or record end may follow a quote.
template <typename CellVector>
inline sanitize::Status parse_csv_cells(
    std::string_view row, char delimiter, CellVector *out, BumpArena *arena,
    std::size_t base_offset = 0,
    std::size_t max_field_bytes = kMaxCsvFieldBytes,
    std::size_t max_decoded_record_bytes = kMaxCsvDecodedRecordBytes) {
  if (!out) {
    return sanitize::Status::Invalid("CSV cell output vector is null");
  }
  out->clear();
  SAN_RETURN_NOT_OK(validate_csv_utf8(row, base_offset));
  const char delim = delimiter == '\0' ? ',' : delimiter;

  try {
    std::size_t position = 0;
    std::size_t decoded_record_bytes = 0;
    const auto charge_decoded = [&](std::size_t bytes,
                                    std::size_t offset) -> sanitize::Status {
      if (bytes > max_field_bytes) {
        return sanitize::Status::OutOfMemory(
            "CSV field size exceeds effective limit at byte ",
            base_offset + offset, ": ", bytes, " > ", max_field_bytes);
      }
      if (bytes > max_decoded_record_bytes ||
          decoded_record_bytes > max_decoded_record_bytes - bytes) {
        return sanitize::Status::OutOfMemory(
            "CSV decoded record size exceeds effective limit at byte ",
            base_offset + offset, ": limit ", max_decoded_record_bytes);
      }
      decoded_record_bytes += bytes;
      return sanitize::Status::OK();
    };
    while (position < row.size()) {
      if (row[position] == delim) {
        SAN_RETURN_NOT_OK(append_csv_cell(out, {}));
        ++position;
        continue;
      }

      if (row[position] == '"') {
        const std::size_t start = position;
        SAN_ASSIGN_OR_RAISE(const auto value,
                            parse_quoted_csv_cell(row, &position, arena,
                                                  base_offset,
                                                  max_field_bytes));
        SAN_RETURN_NOT_OK(charge_decoded(value.size(), start));
        SAN_RETURN_NOT_OK(append_csv_cell(out, value));
        SAN_RETURN_NOT_OK(
            advance_after_quoted_csv_cell(row, &position, delim, base_offset));
        continue;
      }

      const std::size_t start = position;
      while (position < row.size() && row[position] != delim) {
        if (row[position] == '"') {
          return sanitize::Status::Invalid(
              "CSV parse error at byte ", base_offset + position,
              ": quote is only valid at the start of a quoted field");
        }
        ++position;
      }
      const std::string_view cell =
          trim_csv_cell(row.substr(start, position - start));
      SAN_RETURN_NOT_OK(charge_decoded(cell.size(), start));
      SAN_RETURN_NOT_OK(append_csv_cell(out, cell));
      if (position < row.size()) {
        ++position;
      }
    }

    if (!row.empty() && row.back() == delim) {
      SAN_RETURN_NOT_OK(append_csv_cell(out, {}));
    }
    return sanitize::Status::OK();
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "CSV decoded field allocation failed near byte ", base_offset);
  }
}

} // namespace sanitize::internal

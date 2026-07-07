// Declares Python metadata-column parsing helpers for ABI3 streams.

#pragma once

#include "internal/abi/core_abi3_internal.hh"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace core_abi3_internal {

enum class MetadataColumnPlacement {
  FirstRowUtf8,
  AllRowsUtf8,
  RowSpanUtf8,
  AllRowsTimestampMicros,
};

struct MetadataSpan {
  std::int64_t row_count = 0;
  std::string value;
  bool is_null = false;
};

struct MetadataColumn {
  std::string name;
  std::string value;
  std::vector<MetadataSpan> spans;
  MetadataColumnPlacement placement = MetadataColumnPlacement::FirstRowUtf8;
  std::size_t span_index = 0;
  std::int64_t span_offset = 0;
  bool is_null = false;
};

// Appends first-row metadata column specs parsed from a Python dict.
bool append_first_row_columns_from_dict(PyObject *dict,
                                        std::vector<MetadataColumn> *out);
// Appends all-row metadata column specs parsed from a Python dict.
bool append_all_row_columns_from_dict(PyObject *dict,
                                      std::vector<MetadataColumn> *out);
// Appends row-span metadata column specs parsed from a Python dict.
bool append_row_span_columns_from_dict(PyObject *dict,
                                       std::vector<MetadataColumn> *out);
// Appends dynamic timestamp[us] metadata column specs parsed from a sequence.
bool append_timestamp_columns_from_sequence(PyObject *sequence,
                                            std::vector<MetadataColumn> *out);

} // namespace core_abi3_internal

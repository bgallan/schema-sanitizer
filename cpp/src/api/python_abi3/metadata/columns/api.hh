// Declares Python metadata-column models and parsing entry points.

#pragma once

#include "internal/abi/python_abi3/base.hh"

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

bool append_first_row_columns_from_dict(PyObject *dict,
                                        std::vector<MetadataColumn> *out);
bool append_all_row_columns_from_dict(PyObject *dict,
                                      std::vector<MetadataColumn> *out);
bool append_row_span_columns_from_dict(PyObject *dict,
                                       std::vector<MetadataColumn> *out);
bool append_timestamp_columns_from_sequence(PyObject *sequence,
                                            std::vector<MetadataColumn> *out);
// Parses the two metadata groups shared by registry entry points in order.
bool append_registry_metadata_columns(
    PyObject *first_row_columns, PyObject *timestamp_columns,
    std::vector<MetadataColumn> *first_row_out,
    std::vector<MetadataColumn> *timestamp_out);

} // namespace core_abi3_internal

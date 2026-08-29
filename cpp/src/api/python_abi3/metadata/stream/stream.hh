// Declares the Arrow C stream wrapper that appends generated metadata columns.
// The implementation accounts retained buffers before exposing generated
// columns through Arrow C Data.

#pragma once

#include "api/python_abi3/metadata/columns/api.hh"

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace core_abi3_internal {

inline constexpr std::int64_t kMaxMetadataBatchRows = std::int64_t{1} << 24;

struct MetadataStreamState {
  ArrowArrayStream *inner = nullptr;
  PyObject *stream_obj = nullptr;
  PyObject *stream_capsule = nullptr;
  std::vector<MetadataColumn> columns;
  std::vector<std::size_t> base_child_output_indices;
  std::vector<std::size_t> metadata_child_output_indices;
  std::string last_error;
  std::int64_t configured_memory_limit_bytes = -1;
  std::int64_t max_generated_metadata_bytes = 0;
  std::int64_t max_logical_slots = 0;
  bool child_layout_ready = false;
  bool first_row_pending = true;
  bool closed = false;
};

void configure_metadata_stream_budget(MetadataStreamState *stream_state,
                                      std::int64_t memory_limit_bytes) noexcept;

sanitize::Status
prepare_metadata_child_layout(MetadataStreamState *stream_state,
                              const ArrowSchema &base_schema);

sanitize::Status
validate_metadata_base_array(const MetadataStreamState &stream_state,
                             const ArrowArray &base);

sanitize::Status
validate_generated_metadata_budget(const MetadataStreamState &stream_state,
                                   const ArrowArray &base,
                                   std::size_t timestamp_count);

sanitize::Status build_metadata_schema(MetadataStreamState *stream_state,
                                       ArrowSchema *out);
sanitize::Status build_metadata_array(MetadataStreamState *stream_state,
                                      ArrowArray *out);

ArrowArrayStream *make_metadata_stream_wrapper(
    PyObject *stream_obj, PyObject *first_row_columns,
    PyObject *all_row_columns, PyObject *row_span_columns,
    PyObject *timestamp_columns, std::int64_t memory_limit_bytes = -1);
ArrowArrayStream *make_metadata_stream_wrapper_from_stream(
    ArrowArrayStream *inner, PyObject *first_row_columns,
    PyObject *all_row_columns, PyObject *row_span_columns,
    PyObject *timestamp_columns, std::int64_t memory_limit_bytes = -1);

} // namespace core_abi3_internal

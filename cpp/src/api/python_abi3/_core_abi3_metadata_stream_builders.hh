// Declares Arrow metadata stream schema and batch builders.

#pragma once

#include "api/python_abi3/_core_abi3_metadata_columns.hh"

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

#include <string>
#include <vector>

namespace core_abi3_internal {

struct MetadataStreamState {
  ArrowArrayStream *inner = nullptr;
  PyObject *stream_obj = nullptr;
  PyObject *stream_capsule = nullptr;
  std::vector<MetadataColumn> columns;
  std::string last_error;
  bool first_row_pending = true;
  bool closed = false;
};

// Builds an Arrow schema that appends metadata columns to the inner stream.
sanitize::Status build_metadata_schema(MetadataStreamState *stream_state,
                                       ArrowSchema *out);

// Builds an Arrow array that appends metadata columns to the next inner batch.
sanitize::Status build_metadata_array(MetadataStreamState *stream_state,
                                      ArrowArray *out);

// Builds an owning Arrow C stream wrapper that appends metadata columns to a
// Python Arrow C stream export. The returned stream must be released with
// schema_sanitizer_stream_free().
ArrowArrayStream *make_metadata_stream_wrapper(PyObject *stream_obj,
                                               PyObject *first_row_columns,
                                               PyObject *all_row_columns,
                                               PyObject *row_span_columns,
                                               PyObject *timestamp_columns);

// Builds an owning Arrow C stream wrapper around an existing native stream.
// The wrapper takes ownership of inner and releases it with
// schema_sanitizer_stream_free().
ArrowArrayStream *make_metadata_stream_wrapper_from_stream(
    ArrowArrayStream *inner, PyObject *first_row_columns,
    PyObject *all_row_columns, PyObject *row_span_columns,
    PyObject *timestamp_columns);

} // namespace core_abi3_internal

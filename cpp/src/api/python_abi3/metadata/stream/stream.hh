// Arrow C stream wrapper that appends generated metadata columns.
#pragma once

#include "api/python_abi3/metadata/columns/api.hh"

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

sanitize::Status build_metadata_schema(MetadataStreamState *stream_state,
                                       ArrowSchema *out);
sanitize::Status build_metadata_array(MetadataStreamState *stream_state,
                                      ArrowArray *out);

ArrowArrayStream *make_metadata_stream_wrapper(PyObject *stream_obj,
                                               PyObject *first_row_columns,
                                               PyObject *all_row_columns,
                                               PyObject *row_span_columns,
                                               PyObject *timestamp_columns);
ArrowArrayStream *make_metadata_stream_wrapper_from_stream(
    ArrowArrayStream *inner, PyObject *first_row_columns,
    PyObject *all_row_columns, PyObject *row_span_columns,
    PyObject *timestamp_columns);

} // namespace core_abi3_internal

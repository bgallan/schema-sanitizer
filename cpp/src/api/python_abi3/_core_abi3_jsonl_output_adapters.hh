/*
 * ABI3 JSONL output adapter helpers.
 *
 * These helpers own the concrete JSONL Output implementations used by the
 * Python-facing JSONL wrappers.
 */
#pragma once

#include <Python.h>

#include <string>

#include "internal/json/jsonl_stream_writer.hh"
#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

// Writes a Python Arrow stream to a local JSONL path.
sanitize::Result<sanitize::internal::jsonl_stream_writer::WriteStats>
jsonl_write_stream_to_path(PyObject *stream_obj, std::string path);

// Writes an Arrow C stream to a local JSONL path.
sanitize::Result<sanitize::internal::jsonl_stream_writer::WriteStats>
jsonl_write_arrow_stream_to_path(ArrowArrayStream *stream, std::string path);

// Writes a Python Arrow stream to a Python object exposing write(bytes).
sanitize::Result<sanitize::internal::jsonl_stream_writer::WriteStats>
jsonl_write_stream_to_python(PyObject *stream_obj, PyObject *output_obj);

// Writes one Arrow C batch into a string buffer as JSON Lines bytes.
sanitize::Status jsonl_write_batch_to_string(ArrowSchema &schema,
                                             ArrowArray &array,
                                             std::string *out);

} // namespace core_abi3_internal

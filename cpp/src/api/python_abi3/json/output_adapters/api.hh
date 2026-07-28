/*
 * ABI3 JSONL output adapter API.
 *
 * File, Python-object, and string destinations share one bounded lifecycle
 * owner.
 */
#pragma once

#include <Python.h>

#include <string>

#include "internal/json_output/jsonl_stream_writer.hh"
#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

// Writes a Python Arrow stream to a local JSONL path.
sanitize::Result<sanitize::internal::jsonl_stream_writer::WriteStats>
jsonl_write_stream_to_path(PyObject *stream_obj, std::string path,
                           std::int64_t memory_limit_bytes,
                           sanitize::ThreadingMode threading_mode);

// Writes an Arrow C stream to a local JSONL path.
sanitize::Result<sanitize::internal::jsonl_stream_writer::WriteStats>
jsonl_write_arrow_stream_to_path(ArrowArrayStream *stream, std::string path,
                                 std::int64_t memory_limit_bytes,
                                 sanitize::ThreadingMode threading_mode);

// Writes a Python Arrow stream to a Python object exposing write(bytes).
sanitize::Result<sanitize::internal::jsonl_stream_writer::WriteStats>
jsonl_write_stream_to_python(PyObject *stream_obj, PyObject *output_obj,
                             std::int64_t memory_limit_bytes,
                             sanitize::ThreadingMode threading_mode);

// Writes one Arrow C batch into a string buffer as JSON Lines bytes.
sanitize::Status jsonl_write_batch_to_string(ArrowSchema &schema,
                                             ArrowArray &array,
                                             std::string *out,
                                             std::int64_t memory_limit_bytes);

} // namespace core_abi3_internal

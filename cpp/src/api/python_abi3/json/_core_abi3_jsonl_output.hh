/*
 * ABI3 JSONL output and batch encoding helpers.
 *
 * The Python method wrappers use these helpers so output adapters and Arrow
 * batch extraction stay outside the method dispatch translation unit.
 */
#pragma once

#include <Python.h>

#include <string>

#include "internal/json_output/jsonl_stream_writer.hh"
#include "sanitize/core/status.hh"

namespace core_abi3_internal {

// Writes an Arrow C stream object to a Python path or output object.
sanitize::Result<sanitize::internal::jsonl_stream_writer::WriteStats>
jsonl_write_stream_to_output(PyObject *stream_obj, PyObject *output_obj);

// Appends one record batch's JSONL bytes to out.
sanitize::Status jsonl_append_batch_bytes(PyObject *batch_obj,
                                          std::string *out);

// Appends a Python sequence of record batches as JSONL bytes to out.
sanitize::Status jsonl_append_batches_bytes(PyObject *batches_obj,
                                            std::string *out);

} // namespace core_abi3_internal

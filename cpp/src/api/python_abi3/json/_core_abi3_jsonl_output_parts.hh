/*
 * Declares private helpers for ABI3 JSONL batch export.
 *
 * The JSONL output wrapper and batch-byte encoder share Arrow C Data capsule
 * extraction without exposing these details outside the ABI3 implementation.
 */

#pragma once

#include <Python.h>

#include "sanitize/core/status.hh"

struct ArrowArray;
struct ArrowSchema;

namespace core_abi3_internal::jsonl_output {

inline constexpr const char *kArrowSchemaCapsuleName = "arrow_schema";
inline constexpr const char *kArrowArrayCapsuleName = "arrow_array";

/// Extracts Arrow C schema/array capsules from a PyArrow record batch.
sanitize::Status batch_capsules(PyObject *batch_obj, ArrowSchema **schema_out,
                                ArrowArray **array_out, PyObject **owner_out);

} // namespace core_abi3_internal::jsonl_output

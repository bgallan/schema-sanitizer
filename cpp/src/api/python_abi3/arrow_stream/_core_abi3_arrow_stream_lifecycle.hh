// Declares ABI3 Arrow stream capsule lifecycle helpers. They centralize capsule
// ownership and callback release rules for every Python-facing stream.

#pragma once

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include "nanoarrow/nanoarrow.h"

namespace core_abi3_internal {

/// Returns the Python capsule name used by Arrow C stream exports.
const char *arrow_stream_capsule_name() noexcept;

// Releases a Python object while holding the GIL.
void decref_with_gil(PyObject *obj) noexcept;

// Acquires an Arrow C stream capsule from a Python stream object.
bool acquire_arrow_stream(PyObject *stream_obj, PyObject **capsule_out,
                          ArrowArrayStream **inner_out);

// Acquires an Arrow C schema capsule from a Python schema object.
bool acquire_arrow_schema(PyObject *schema_obj, PyObject **capsule_out,
                          ArrowSchema **schema_out);

// Releases stream object/capsule keepalives for wrapper stream states.
void close_arrow_stream_keepalive(ArrowArrayStream **inner,
                                  PyObject **stream_obj,
                                  PyObject **stream_capsule,
                                  bool *closed) noexcept;

} // namespace core_abi3_internal

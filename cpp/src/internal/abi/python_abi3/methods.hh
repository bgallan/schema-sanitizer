// Declares the complete internal Python ABI3 method catalogue.
// These definitions keep interpreter ownership and method-table details behind
// the private extension boundary.

#pragma once

#include "internal/abi/python_abi3/base.hh"

namespace core_abi3_internal {

#define SCHEMA_SANITIZER_ABI3_METHOD(name, flags, doc)                         \
  PyObject *py_##name(PyObject *, PyObject *);
#include "internal/abi/python_abi3/method_catalog.inc"
#undef SCHEMA_SANITIZER_ABI3_METHOD

} // namespace core_abi3_internal

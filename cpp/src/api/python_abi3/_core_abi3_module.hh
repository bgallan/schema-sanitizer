/*
 * ABI3 Python module declaration helpers.
 *
 * This header exposes the module definition used by the limited-API extension
 * initializer while keeping the method table in its own translation unit.
 */
#pragma once

#include <Python.h>

namespace core_abi3_internal {

// Returns the static module definition used to create
// schema_sanitizer._core_abi3.
PyModuleDef *module_definition() noexcept;

} // namespace core_abi3_internal

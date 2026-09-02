// Declares ABI3 conversion helpers for native logical-schema payloads. The
// bridge validates required fields and transfers Arrow schema ownership back to
// Python safely.

#pragma once

#include "internal/abi/python_abi3/base.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

namespace core_abi3_internal::logical_schema_payload {

sanitize::Result<sanitize::LogicalSchema> read_required(PyObject *obj);

} // namespace core_abi3_internal::logical_schema_payload

// Declares reusable Python Arrow C Stream ingestion frontend helpers. These
// routines keep Arrow schema interpretation and buffer ownership explicit at
// the ABI boundary.

#pragma once

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"

#include <memory>
#include <string_view>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/options/options.hh"
#include "sanitize/runtime/execution_context.hh"

struct ArrowSchema;

namespace core_abi3_internal {

struct ArrowDirectOptions {
  std::string_view timestamp_precision = "TIMESTAMP_MICROS";
  std::int64_t memory_limit_bytes = -1;
};

/// Returns whether an Arrow C schema can be consumed by the direct frontend.
bool arrow_direct_schema_is_supported(const ArrowSchema &schema);

/// Builds a native frontend from a Python object exposing `__arrow_c_stream__`.
/// Writes the logical schema observed from the imported stream schema.
sanitize::Result<sanitize::FrontendHandle>
make_arrow_frontend(PyObject *stream_obj, sanitize::LogicalSchema *schema,
                    ArrowDirectOptions options);

/// Applies normal inference's schema-contract handling to a direct Arrow
/// schema.
sanitize::Result<sanitize::LogicalSchema>
finalize_direct_arrow_schema(const sanitize::LogicalSchema &input_schema,
                             const sanitize::PreparedOptions &opts);

/// Compiles a direct Arrow logical schema and returns a streaming ingest
/// result.
sanitize::Result<sanitize::IngestStream> ingest_direct_arrow_stream(
    sanitize::FrontendHandle frontend, sanitize::LogicalSchema final_schema,
    sanitize::PreparedOptionsPtr opts,
    std::shared_ptr<sanitize::ExecutionContext> owned_ctx);

/// Returns whether a PyArrow schema is supported by direct Arrow ingestion.
PyObject *py_arrow_direct_schema_supported(PyObject *, PyObject *);

/// Encodes a PyArrow schema as the native logical-schema contract payload.
PyObject *py_arrow_schema_contract_payload(PyObject *, PyObject *);

} // namespace core_abi3_internal

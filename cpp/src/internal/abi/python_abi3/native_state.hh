// Owns native state exposed through private Python ABI3 capsules.
// These definitions keep interpreter ownership and method-table details behind
// the private extension boundary.

#pragma once

#include <memory>

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/runtime/execution_context.hh"

namespace core_abi3_internal {

struct NativeContext {
  std::shared_ptr<sanitize::ExecutionContext> ctx;
};

struct NativeDiagnostics {
  std::shared_ptr<sanitize::IngestDiagnostics> diagnostics;
  sanitize::IngestDiagnostics inference_snapshot;
  bool has_inference_snapshot = false;
};

struct NativePreparedOptions {
  sanitize::PreparedOptionsPtr prepared;
};

sanitize::Result<sanitize::PreparedOptionsPtr> default_prepared_options();

} // namespace core_abi3_internal

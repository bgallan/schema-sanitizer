// Declares internal Arrow C Stream export helpers.

#pragma once

#include <memory>

#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/status.hh"

struct ArrowArray;
struct ArrowSchema;

namespace sanitize {

// Internal source interface for streaming Arrow C Data batches.
class ExportBatchSource {
public:
  // Destroys the ExportBatchSource.
  virtual ~ExportBatchSource() = default;
  virtual sanitize::Status GetSchema(struct ArrowSchema *out) = 0;
  virtual sanitize::Status GetNext(struct ArrowArray *out) = 0;
  virtual sanitize::Status Close() = 0;
};

// Internal-only helper for exporting batch sources to Arrow C streams.
sanitize::Result<UniqueCStream>
export_stream_c(std::shared_ptr<ExportBatchSource> source);

} // namespace sanitize

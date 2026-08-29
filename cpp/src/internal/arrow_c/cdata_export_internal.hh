// Declares internal Arrow C Stream export helpers.
// The implementation preserves Arrow ownership and error contracts without
// depending on the Arrow C++ library.

#pragma once

#include <memory>

#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/status.hh"

struct ArrowArray;
struct ArrowSchema;

namespace sanitize::internal {
class OperationTaskArena;
}

namespace sanitize {

// Internal source interface for streaming Arrow C Data batches.
class ExportBatchSource {
public:
  /// Releases implementation-owned resources through the polymorphic source
  /// interface.
  virtual ~ExportBatchSource() = default;
  /// Exports a fresh Arrow schema without transferring source ownership.
  virtual sanitize::Status GetSchema(struct ArrowSchema *out) = 0;
  /// Exports the next Arrow batch, or a released array at end of stream.
  virtual sanitize::Status GetNext(struct ArrowArray *out) = 0;
  /// Closes the source, cancelling pending work and making callbacks harmless.
  virtual sanitize::Status Close() = 0;
  /// Returns the operation task arena associated with this stream or source
  /// instance.
  [[nodiscard]] virtual std::shared_ptr<internal::OperationTaskArena>
  TaskArena() const noexcept {
    return {};
  }
};

/// Wraps an internal batch source in an owning Arrow C Stream.
sanitize::Result<UniqueCStream>
export_stream_c(std::shared_ptr<ExportBatchSource> source);

} // namespace sanitize

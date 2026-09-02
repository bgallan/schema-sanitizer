// Defines RAII wrappers for Arrow C Data interface schemas, arrays, and
// streams. The guards centralize release-callback ownership so native and
// Python-facing paths cannot leak or double-release exported Arrow resources.

#pragma once

#include "nanoarrow/nanoarrow.h"

#include <memory>

namespace sanitize {

class CSchemaGuard {
public:
  /// Creates an empty ArrowSchema guard.
  CSchemaGuard() = default;
  /// Releases the owned ArrowSchema if it is still live.
  ~CSchemaGuard() noexcept;
  /// Prevents copying an owned ArrowSchema release callback.
  CSchemaGuard(const CSchemaGuard &) = delete;
  /// Prevents copy-assignment of an owned ArrowSchema release callback.
  CSchemaGuard &operator=(const CSchemaGuard &) = delete;

  /// Returns the mutable ArrowSchema pointer.
  [[nodiscard]] ArrowSchema *get() noexcept { return &schema_; }
  /// Returns the immutable ArrowSchema pointer.
  [[nodiscard]] const ArrowSchema *get() const noexcept { return &schema_; }
  /// Returns a mutable reference to the guarded schema.
  [[nodiscard]] ArrowSchema &value() noexcept { return schema_; }
  /// Returns an immutable reference to the guarded schema.
  [[nodiscard]] const ArrowSchema &value() const noexcept { return schema_; }
  /// Releases the current schema and clears it to an empty state.
  void reset() noexcept;

private:
  ArrowSchema schema_{};
};

class CArrayGuard {
public:
  /// Creates an empty ArrowArray guard.
  CArrayGuard() = default;
  /// Releases the owned ArrowArray if it is still live.
  ~CArrayGuard() noexcept;
  /// Prevents copying an owned ArrowArray release callback.
  CArrayGuard(const CArrayGuard &) = delete;
  /// Prevents copy-assignment of an owned ArrowArray release callback.
  CArrayGuard &operator=(const CArrayGuard &) = delete;

  /// Returns the mutable ArrowArray pointer.
  [[nodiscard]] ArrowArray *get() noexcept { return &array_; }
  /// Returns the immutable ArrowArray pointer.
  [[nodiscard]] const ArrowArray *get() const noexcept { return &array_; }
  /// Returns a mutable reference to the guarded array.
  [[nodiscard]] ArrowArray &value() noexcept { return array_; }
  /// Returns an immutable reference to the guarded array.
  [[nodiscard]] const ArrowArray &value() const noexcept { return array_; }
  /// Releases the current array and clears it to an empty state.
  void reset() noexcept;

private:
  ArrowArray array_{};
};

struct ArrowArrayStreamDeleter {
  /// Releases the Arrow C stream callback state before deleting the stream.
  void operator()(ArrowArrayStream *p) const noexcept;
};

using UniqueCStream =
    std::unique_ptr<ArrowArrayStream, ArrowArrayStreamDeleter>;

} // namespace sanitize

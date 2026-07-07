// Defines frontend row streams and ownership handles.

#pragma once

#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "sanitize/core/value_view.hh"

namespace sanitize {

struct CompiledPlan;

struct FieldRef {
  std::string_view key;
  uint64_t key_hash = 0; // optional (0 => compute)
  ValueView value;
};

enum class RowFlags : uint8_t {
  kNone = 0,
  // Row is provided as raw text only; field parsing is deferred to the
  // materializer.
  kRawOnly = 1,
};

struct RowRef {
  const FieldRef *fields = nullptr;
  std::size_t size = 0;

  // Best-effort raw representation of the row (slice of the original source).
  std::string_view raw;

  // Byte offset of `raw.data()` within the original source.
  std::size_t base_offset = 0;

  // Optional: frontend-specific context required for raw-only materialization.
  // This is typically a stable pointer into the frontend instance (e.g. CSV
  // header mapping).
  const void *direct_ctx = nullptr;

  // Optional generated metadata value for the source file that produced the
  // row. Native multi-source frontends populate this without pre-counting rows.
  std::string_view source_file;

  uint8_t flags = std::to_underlying(RowFlags::kNone);
};

struct RowBatch {
  std::vector<RowRef> rows;
  std::shared_ptr<const void> owner;
};

struct FrontendVTable {
  void (*reset)(void *) noexcept;
  sanitize::Result<RowBatch> (*next_batch)(void *, int64_t capacity);
  void (*set_plan)(void *, const CompiledPlan *) noexcept;
  void (*destroy)(void *) noexcept;
};

class FrontendHandle {
public:
  // Creates an empty frontend handle.
  FrontendHandle() = default;
  // Creates a frontend handle from an implementation and vtable.
  FrontendHandle(void *self, const FrontendVTable *vt) : self_(self), vt_(vt) {}
  // Destroys the FrontendHandle.
  ~FrontendHandle() { destroy(); }

  // Disables copying frontend handles.
  FrontendHandle(const FrontendHandle &) = delete;
  // Disables copy assignment.
  FrontendHandle &operator=(const FrontendHandle &) = delete;

  // Moves a frontend handle.
  FrontendHandle(FrontendHandle &&other) noexcept
      : self_(other.self_), vt_(other.vt_) {
    other.self_ = nullptr;
    other.vt_ = nullptr;
  }

  // Replaces this handle with another frontend handle.
  FrontendHandle &operator=(FrontendHandle &&other) noexcept {
    if (this != &other) {
      destroy();
      self_ = other.self_;
      vt_ = other.vt_;
      other.self_ = nullptr;
      other.vt_ = nullptr;
    }
    return *this;
  }

  // Returns whether the object contains a value.
  explicit operator bool() const noexcept { return self_ && vt_; }

  // Rewinds the underlying frontend.
  void reset() noexcept {
    if (!self_ || !vt_)
      return;
    vt_->reset(self_);
  }

  // Passes the compiled materialization plan to the frontend.
  void set_plan(const CompiledPlan *plan) noexcept {
    if (!self_ || !vt_)
      return;
    vt_->set_plan(self_, plan);
  }

  // Returns the next batch.
  sanitize::Result<RowBatch> next_batch(int64_t capacity) {
    if (!self_ || !vt_)
      return sanitize::Status::Invalid("FrontendHandle: null frontend");
    return vt_->next_batch(self_, capacity);
  }

private:
  // Destroys the object state.
  void destroy() noexcept {
    if (self_ && vt_) {
      vt_->destroy(self_);
    }
    self_ = nullptr;
    vt_ = nullptr;
  }

  void *self_ = nullptr;
  const FrontendVTable *vt_ = nullptr;
};

} // namespace sanitize

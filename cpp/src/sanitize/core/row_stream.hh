// Defines frontend row streams and ownership handles.

#pragma once

#include "sanitize/core/diagnostics.hh"
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

namespace internal {
class OperationTaskArena;
}

// Optional frontend-owned hook that can release row-specific backing storage
// after materialization has copied the row into the analytical output.
class RowBatchReleaser {
public:
  virtual ~RowBatchReleaser() = default;
  virtual void ReleaseRows(std::size_t begin, std::size_t count) noexcept = 0;
};

struct CompiledPlan;

struct FieldRef {
  std::string_view key;
  uint64_t key_hash = 0; // optional (0 => compute)
  ValueView value;
};

enum class FrontendMaterializationMode : uint8_t {
  kDefault = 0,
  // Parse syntax in the frontend, then defer full row materialization.
  kValidatedRaw = 1,
  // Parse and arrange scalar fields in frozen plan order for column fan-out.
  kPlanOrdered = 2,
  // Emit raw rows without frontend validation. A bounded ordered worker stage
  // validates the complete frontend batch before materialization can commit.
  kDeferredValidationRaw = 3,
  // Emit framed raw rows whose only authoritative parse is worker-local.
  // Used by JSON document arrays after schema planning.
  kWorkerAuthoritativeRaw = 4,
};

enum class RowFlags : uint8_t {
  kNone = 0,
  // Row is provided as raw text only; field parsing is deferred to the
  // materializer.
  kRawOnly = 1,
  // Row fields occupy frozen plan order, followed by any retained extras.
  kPlanOrdered = 2,
  // Raw JSON row has an immutable validated top-level field-token index.
  kJsonValidatedTokens = 4,
  // Validated JSON field tokens match the frozen root plan exactly by name and
  // order. Materialization may consume values positionally without key lookup.
  kJsonPlanOrderedTokens = 8,
  // Deferred JSON row must be a top-level object (json_array semantics).
  kJsonObjectRequired = 16,
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
  // When present, rows may release heavyweight parser state independently of
  // the batch owner once their values have been materialized.
  std::shared_ptr<RowBatchReleaser> releaser;
  ReaderResourceDiagnostics reader_diagnostics;
};

struct FrontendVTable {
  void (*reset)(void *) noexcept;
  sanitize::Result<RowBatch> (*next_batch)(void *, int64_t capacity);
  void (*set_plan)(void *, const CompiledPlan *) noexcept;
  void (*destroy)(void *) noexcept;
  void (*set_memory_pool)(void *, std::shared_ptr<void>) noexcept = nullptr;
  void (*set_materialization_mode)(
      void *, FrontendMaterializationMode) noexcept = nullptr;
  void (*set_task_arena)(
      void *, std::shared_ptr<internal::OperationTaskArena>) noexcept = nullptr;
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

  // Selects the internal row representation used by capable frontends.
  void set_materialization_mode(FrontendMaterializationMode mode) noexcept {
    if (self_ && vt_ && vt_->set_materialization_mode) {
      vt_->set_materialization_mode(self_, mode);
    }
  }

  // Installs the operation-scoped tracked allocation pool when supported.
  void set_memory_pool(std::shared_ptr<void> pool) noexcept {
    if (self_ && vt_ && vt_->set_memory_pool) {
      vt_->set_memory_pool(self_, std::move(pool));
    }
  }

  // Installs the operation-scoped shared worker arena when supported.
  void set_task_arena(
      std::shared_ptr<internal::OperationTaskArena> task_arena) noexcept {
    if (self_ && vt_ && vt_->set_task_arena) {
      vt_->set_task_arena(self_, std::move(task_arena));
    }
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

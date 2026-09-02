/*
 * Implements Python ABI3 orchestration for direct Arrow C Stream ingestion.
 *
 * Schema parsing, scalar formatting, and Arrow value extraction live in
 * adjacent modules so this file stays focused on Python capsules, frontend
 * lifecycle, and ingest stream wiring.
 */

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_batch.hh"
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_validate.hh"
#include "api/python_abi3/arrow_direct/schema/logical.hh"
#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"

#include <algorithm>
#include <cstring>
#include <memory>
#include <new>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/planning/plan_compile.hh"
#include "internal/runtime/execution_policy.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "nanoarrow/nanoarrow.h"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/runtime/execution_context.hh"

namespace core_abi3_internal {
namespace {

// Owns one Arrow C stream capsule and exposes it as a row frontend.
class ArrowDirectFrontend final {
public:
  /// Retains the Python capsule/keepalive objects for stream lifetime.
  ArrowDirectFrontend(ArrowArrayStream *stream, PyObject *capsule,
                      PyObject *keepalive, std::vector<ArrowInputNode> fields,
                      std::int64_t memory_limit_bytes)
      : stream_(stream), capsule_(capsule), keepalive_(keepalive),
        fields_(std::move(fields)), memory_limit_bytes_(memory_limit_bytes) {
    Py_XINCREF(keepalive_);
  }

  /// Releases foreign batches before dropping the Python stream keepalive.
  ~ArrowDirectFrontend() {
    pending_.reset();
    pending_offset_ = 0;
    decref_with_gil(capsule_);
    capsule_ = nullptr;
    decref_with_gil(keepalive_);
    keepalive_ = nullptr;
    stream_ = nullptr;
  }

  /// Reads at most capacity rows while retaining foreign buffers zero-copy.
  sanitize::Result<sanitize::RowBatch> next_batch(int64_t capacity) {
    sanitize::RowBatch out;
    if (done_ || capacity <= 0) {
      return out;
    }

    while (!pending_) {
      std::shared_ptr<ArrowArrayStorage> storage;
      try {
        storage = std::make_shared<ArrowArrayStorage>();
      } catch (const std::bad_alloc &) {
        return sanitize::Status::OutOfMemory(
            "Arrow direct input batch owner allocation failed");
      }
      sanitize::internal::cdata_stream::clear_array(&storage->array);

      int code = 0;
      try {
        code = stream_->get_next(stream_, &storage->array);
      } catch (...) {
        return sanitize::internal::cdata_stream::status_from_current_exception(
            "Arrow direct stream get_next");
      }
      if (code != 0) {
        const char *last_error = nullptr;
        try {
          last_error = stream_->get_last_error
                           ? stream_->get_last_error(stream_)
                           : nullptr;
        } catch (...) {
          last_error = nullptr;
        }
        return sanitize::Status::IOError(
            last_error ? last_error : "Arrow direct stream get_next failed");
      }
      if (!storage->array.release) {
        done_ = true;
        return out;
      }
      SAN_RETURN_NOT_OK(validate_arrow_direct_batch(storage->array, fields_,
                                                    memory_limit_bytes_));
      if (storage->array.length == 0) {
        continue;
      }
      pending_ = std::move(storage);
      pending_offset_ = 0;
    }

    const int64_t remaining = pending_->array.length - pending_offset_;
    const int64_t row_count = std::min(capacity, remaining);
    auto batch = build_arrow_direct_row_batch(pending_, fields_,
                                              pending_offset_, row_count);
    if (!batch.ok()) {
      return batch.status();
    }
    pending_offset_ += row_count;
    if (pending_offset_ == pending_->array.length) {
      pending_.reset();
      pending_offset_ = 0;
    }
    return batch;
  }

  /// Marks the frontend exhausted and releases any pending foreign batch.
  void reset() noexcept {
    pending_.reset();
    pending_offset_ = 0;
    done_ = true;
  }

  /// Accepts a compiled plan as an intentional no-op because direct rows
  /// already expose stable field references.
  void set_plan(const sanitize::CompiledPlan *) noexcept {}

private:
  ArrowArrayStream *stream_ = nullptr;
  PyObject *capsule_ = nullptr;
  PyObject *keepalive_ = nullptr;
  std::vector<ArrowInputNode> fields_;
  std::int64_t memory_limit_bytes_ = -1;
  std::shared_ptr<ArrowArrayStorage> pending_;
  int64_t pending_offset_ = 0;
  bool done_ = false;
};

/// Rewinds the direct Arrow ingestion to its initial input position and clears
/// per-pass state.
void arrow_reset(void *self) noexcept {
  static_cast<ArrowDirectFrontend *>(self)->reset();
}

/// Reads and materializes the next bounded row batch from the direct Arrow
/// ingestion.
sanitize::Result<sanitize::RowBatch> arrow_next_batch(void *self,
                                                      int64_t capacity) {
  return static_cast<ArrowDirectFrontend *>(self)->next_batch(capacity);
}

/// Forwards the compiled plan through the direct Arrow frontend callback table.
void arrow_set_plan(void *self, const sanitize::CompiledPlan *plan) noexcept {
  static_cast<ArrowDirectFrontend *>(self)->set_plan(plan);
}

/// Destroys the heap-owned direct Arrow ingestion state after its final
/// callback completes.
void arrow_destroy(void *self) noexcept {
  delete static_cast<ArrowDirectFrontend *>(self);
}

const sanitize::FrontendVTable kArrowVTable{
    .reset = &arrow_reset,
    .next_batch = &arrow_next_batch,
    .set_plan = &arrow_set_plan,
    .destroy = &arrow_destroy,
};

} // namespace

bool arrow_direct_schema_is_supported(const ArrowSchema &schema) {
  std::vector<ArrowInputNode> fields;
  auto parsed = logical_schema_from_arrow_schema(
      &schema, &fields,
      ArrowDirectOptions{.timestamp_precision = "TIMESTAMP_MICROS"});
  return parsed.ok();
}

sanitize::Result<sanitize::FrontendHandle>
make_arrow_frontend(PyObject *stream_obj, sanitize::LogicalSchema *schema,
                    ArrowDirectOptions options) {
  PyObject *capsule = nullptr;
  ArrowArrayStream *stream = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &stream)) {
    return sanitize::Status::Invalid(
        "Arrow direct input must expose __arrow_c_stream__");
  }
  std::unique_ptr<PyObject, decltype(&decref_with_gil)> capsule_owner(
      capsule, decref_with_gil);

  sanitize::CSchemaGuard c_schema;
  int code = 0;
  try {
    code = stream->get_schema(stream, c_schema.get());
  } catch (...) {
    return sanitize::internal::cdata_stream::status_from_current_exception(
        "Arrow direct stream get_schema");
  }
  if (code != 0) {
    const char *last_error = nullptr;
    try {
      last_error =
          stream->get_last_error ? stream->get_last_error(stream) : nullptr;
    } catch (...) {
      last_error = nullptr;
    }
    return sanitize::Status::IOError(
        last_error ? last_error : "Arrow direct stream get_schema failed");
  }
  std::vector<ArrowInputNode> fields;
  auto logical =
      logical_schema_from_arrow_schema(c_schema.get(), &fields, options);
  if (!logical.ok()) {
    return logical.status();
  }
  *schema = std::move(logical).ValueOrDie();

  auto *frontend = new (std::nothrow)
      ArrowDirectFrontend(stream, capsule, stream_obj, std::move(fields),
                          options.memory_limit_bytes);
  if (!frontend) {
    return sanitize::Status::OutOfMemory("Arrow direct frontend OOM");
  }
  (void)capsule_owner.release();
  return sanitize::FrontendHandle(frontend, &kArrowVTable);
}

sanitize::Result<sanitize::IngestStream> ingest_direct_arrow_stream(
    sanitize::FrontendHandle frontend, sanitize::LogicalSchema final_schema,
    sanitize::PreparedOptionsPtr opts,
    std::shared_ptr<sanitize::ExecutionContext> owned_ctx) {
  auto compiled_r = sanitize::compile_plan(final_schema);
  if (!compiled_r.ok()) {
    return compiled_r.status();
  }
  auto plan = std::make_shared<sanitize::CompiledPlan>(
      std::move(compiled_r).ValueOrDie());
  frontend.set_plan(plan.get());

  auto diag = std::make_shared<sanitize::IngestDiagnostics>();
  diag->arrow_schema_depth = sanitize::arrow_schema_depth(final_schema);
  diag->parquet_schema_depth = sanitize::parquet_schema_depth(final_schema);
  diag->direct_arrow_input = 1;

  sanitize::PreparedIngest prepared;
  prepared.frontend_name = "arrow";
  prepared.frontend = std::move(frontend);
  prepared.owned_ctx = std::move(owned_ctx);
  prepared.ctx = prepared.owned_ctx.get();
  if (!prepared.ctx) {
    return sanitize::Status::Invalid(
        "prepared ingest has no execution context");
  }
  prepared.operation_memory_pool =
      prepared.ctx->make_operation_memory_pool_handle(
          opts->spec.memory_limit_bytes, opts->operation_memory_ledger);
  if (!prepared.operation_memory_pool) {
    return sanitize::Status::OutOfMemory(
        "operation memory pool allocation failed");
  }
  const auto policy = sanitize::internal::execution_policy_from(
      opts->spec.threading_mode, opts->spec.memory_limit_bytes);
  prepared.telemetry = prepared.ctx->begin_performance_telemetry(
      prepared.operation_memory_pool, opts->spec.memory_limit_bytes,
      policy.effective_workers,
      opts->spec.threading_mode == sanitize::ThreadingMode::kMulti);
  SAN_ASSIGN_OR_RAISE(prepared.task_arena,
                      sanitize::internal::OperationTaskArena::Make(
                          static_cast<std::size_t>(std::max<std::int64_t>(
                              1, policy.effective_workers)),
                          prepared.telemetry));
  const auto arena_budget = sanitize::internal::memory_budget_from_limit(
      opts->spec.memory_limit_bytes);
  prepared.task_arena->SetBackpressureTimeoutMillis(
      sanitize::internal::backpressure_timeout_millis_from(arena_budget));
  prepared.task_arena->SetBackpressureDeadlineMillis(
      sanitize::internal::backpressure_deadline_millis_from(arena_budget));
  prepared.plan = plan;
  prepared.opts = std::move(opts);
  prepared.diagnostics = diag;
  prepared.logical_schema = std::move(final_schema);
  prepared.inference_consumed = false;

  return sanitize::ingest_to_stream(std::move(prepared));
}

} // namespace core_abi3_internal

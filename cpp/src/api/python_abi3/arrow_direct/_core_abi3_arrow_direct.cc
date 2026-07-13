/*
 * Python ABI3 orchestration for direct Arrow C Stream ingestion.
 *
 * Schema parsing, scalar formatting, and Arrow value extraction live in
 * adjacent modules so this file stays focused on Python capsules, frontend
 * lifecycle, and ingest stream wiring.
 */
#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.hh"

#include "api/python_abi3/arrow_direct/_core_abi3_arrow_direct_batch.hh"
#include "api/python_abi3/arrow_direct/schema/logical.hh"
#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"

#include <cstring>
#include <memory>
#include <new>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/abi/schema_sanitizer_c_internal.hh"
#include "internal/planning/plan_compile.hh"
#include "nanoarrow/nanoarrow.h"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/ingest.hh"
#include "sanitize/ingest/ingest_types.hh"

namespace core_abi3_internal {
namespace {

// Owns one Arrow C stream capsule and exposes it as a row frontend.
class ArrowDirectFrontend final {
public:
  // Retains the Python capsule/keepalive objects for stream lifetime.
  ArrowDirectFrontend(ArrowArrayStream *stream, PyObject *capsule,
                      PyObject *keepalive, std::vector<ArrowInputNode> fields)
      : stream_(stream), capsule_(capsule), keepalive_(keepalive),
        fields_(std::move(fields)) {
    Py_XINCREF(keepalive_);
  }

  // Releases retained Python objects.
  ~ArrowDirectFrontend() {
    decref_with_gil(capsule_);
    decref_with_gil(keepalive_);
  }

  // Reads the next Arrow batch and exposes it as RowRef values.
  sanitize::Result<sanitize::RowBatch> next_batch(int64_t capacity) {
    sanitize::RowBatch out;
    if (done_ || capacity <= 0) {
      return out;
    }
    auto storage = std::make_shared<ArrowBatchStorage>();
    std::memset(&storage->array, 0, sizeof(storage->array));
    const int code = stream_->get_next(stream_, &storage->array);
    if (code != 0) {
      const char *last_error =
          stream_->get_last_error ? stream_->get_last_error(stream_) : nullptr;
      return sanitize::Status::IOError(
          last_error ? last_error : "Arrow direct stream get_next failed");
    }
    if (!storage->array.release) {
      done_ = true;
      return out;
    }
    if (storage->array.n_children != static_cast<int64_t>(fields_.size())) {
      return sanitize::Status::Invalid(
          "Arrow direct batch column count does not match schema");
    }
    return build_arrow_direct_row_batch(std::move(storage), fields_);
  }

  // Marks the frontend exhausted.
  void reset() noexcept { done_ = true; }

  // Direct Arrow rows already expose stable field references.
  void set_plan(const sanitize::CompiledPlan *) noexcept {}

private:
  ArrowArrayStream *stream_ = nullptr;
  PyObject *capsule_ = nullptr;
  PyObject *keepalive_ = nullptr;
  std::vector<ArrowInputNode> fields_;
  bool done_ = false;
};

void arrow_reset(void *self) noexcept {
  static_cast<ArrowDirectFrontend *>(self)->reset();
}

sanitize::Result<sanitize::RowBatch> arrow_next_batch(void *self,
                                                      int64_t capacity) {
  return static_cast<ArrowDirectFrontend *>(self)->next_batch(capacity);
}

void arrow_set_plan(void *self, const sanitize::CompiledPlan *plan) noexcept {
  static_cast<ArrowDirectFrontend *>(self)->set_plan(plan);
}

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

  sanitize::CSchemaGuard c_schema;
  const int code = stream->get_schema(stream, c_schema.get());
  if (code != 0) {
    const char *last_error =
        stream->get_last_error ? stream->get_last_error(stream) : nullptr;
    Py_DECREF(capsule);
    return sanitize::Status::IOError(
        last_error ? last_error : "Arrow direct stream get_schema failed");
  }
  std::vector<ArrowInputNode> fields;
  auto logical =
      logical_schema_from_arrow_schema(c_schema.get(), &fields, options);
  if (!logical.ok()) {
    Py_DECREF(capsule);
    return logical.status();
  }
  *schema = std::move(logical).ValueOrDie();

  auto *frontend = new (std::nothrow)
      ArrowDirectFrontend(stream, capsule, stream_obj, std::move(fields));
  if (!frontend) {
    Py_DECREF(capsule);
    return sanitize::Status::OutOfMemory("Arrow direct frontend OOM");
  }
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
  prepared.plan = plan;
  prepared.opts = std::move(opts);
  prepared.diagnostics = diag;
  prepared.logical_schema = std::move(final_schema);
  prepared.inference_consumed = false;

  return sanitize::ingest_to_stream(std::move(prepared));
}

} // namespace core_abi3_internal

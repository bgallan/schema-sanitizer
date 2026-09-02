// Implements Arrow C Stream export glue for the internal pipeline.
// The implementation preserves Arrow ownership and error contracts without
// depending on the Arrow C++ library.

#include "internal/arrow_c/cdata_export_internal.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/arrow_c/cdata_stream_runtime.hh"
#include "internal/runtime/process_identity.hh"

#include "nanoarrow/nanoarrow.h"

#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <utility>

namespace sanitize {

namespace {

struct ExportBatchStreamState {
  std::shared_ptr<sanitize::ExportBatchSource> source;
  std::string last_error;
  bool closed = false;
};

/// Returns the last error recorded by an exported stream.
static const char *stream_get_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid export stream";
  }
  auto *state = static_cast<ExportBatchStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

/// Closes the batch source once and preserves the first callback error.
static void close_source(ExportBatchStreamState *state) noexcept {
  if (!state || !state->source || state->closed) {
    return;
  }
  try {
    sanitize::Status st = state->source->Close();
    if (!st.ok() && state->last_error.empty()) {
      sanitize::internal::cdata_stream::set_last_error_nothrow(
          state->last_error, st);
    }
  } catch (...) {
    if (state->last_error.empty()) {
      sanitize::internal::cdata_stream::set_last_error_nothrow(
          state->last_error,
          sanitize::internal::cdata_stream::status_from_current_exception(
              "ArrowArrayStream::release"));
    }
  }
  state->closed = true;
}

/// Delegates Arrow C Stream schema export to the batch source.
static int stream_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<ExportBatchStreamState *>(stream->private_data);
  if (!state || !state->source) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, state->last_error, "ArrowArrayStream::get_schema",
      [&](ArrowSchema *schema) { return state->source->GetSchema(schema); });
}

/// Delegates Arrow C Stream batch export to the batch source.
static int stream_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<ExportBatchStreamState *>(stream->private_data);
  if (!state || !state->source) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_array_callback(
      out, state->last_error, "ArrowArrayStream::get_next",
      [&](ArrowArray *array) { return state->source->GetNext(array); });
}

/// Closes the source and releases exported stream state.
static void stream_release(ArrowArrayStream *stream) {
  if (!sanitize::internal::runtime_owner_process()) {
    return;
  }
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<ExportBatchStreamState *>(stream->private_data);
  close_source(state);
  sanitize::internal::detach_task_arena(stream);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

} // namespace

void ArrowArrayStreamDeleter::operator()(ArrowArrayStream *p) const noexcept {
  if (!p)
    return;
  sanitize::internal::cdata_stream::release_stream_nothrow(p);
  delete p;
}

CSchemaGuard::~CSchemaGuard() noexcept { reset(); }

void CSchemaGuard::reset() noexcept {
  sanitize::internal::cdata_stream::release_schema_nothrow(&schema_);
  sanitize::internal::cdata_stream::clear_schema(&schema_);
}

CArrayGuard::~CArrayGuard() noexcept { reset(); }

void CArrayGuard::reset() noexcept {
  sanitize::internal::cdata_stream::release_array_nothrow(&array_);
  sanitize::internal::cdata_stream::clear_array(&array_);
}

sanitize::Result<UniqueCStream>
export_stream_c(std::shared_ptr<ExportBatchSource> source) {
  if (!source) {
    return sanitize::Status::Invalid("export_stream_c: source is null");
  }

  auto *state = new (std::nothrow) ExportBatchStreamState();
  if (!state) {
    return sanitize::Status::OutOfMemory("export_stream_c: OOM state");
  }
  state->source = std::move(source);

  auto *stream = new (std::nothrow) ArrowArrayStream();
  if (!stream) {
    delete state;
    return sanitize::Status::OutOfMemory("export_stream_c: OOM stream");
  }
  std::memset(stream, 0, sizeof(*stream));
  stream->get_schema = &stream_get_schema;
  stream->get_next = &stream_get_next;
  stream->get_last_error = &stream_get_last_error;
  stream->release = &stream_release;
  stream->private_data = state;
  sanitize::internal::attach_task_arena(stream, state->source->TaskArena());
  return UniqueCStream(stream);
}

} // namespace sanitize

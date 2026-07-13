/* Arrow C stream batch coalescing wrapper and Python entry point. */
#include "api/python_abi3/streaming/coalesce_stream_internal.hh"

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"

#include <cerrno>
#include <cstring>
#include <memory>
#include <new>
#include <utility>

namespace core_abi3_internal::coalesce_detail {

void release_coalesce_stream(CoalesceStreamState *state) noexcept {
  if (!state || state->closed) {
    return;
  }
  close_arrow_stream_keepalive(&state->inner, &state->stream_obj,
                               &state->stream_capsule, &state->closed);
}

const char *coalesce_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid coalescing stream";
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

void coalesce_release(ArrowArrayStream *stream) {
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  release_coalesce_stream(state);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int coalesce_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  if (!state || !state->inner) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, state->last_error, "coalescing_stream.get_schema",
      [&](ArrowSchema *schema) -> sanitize::Status {
        const int rc = state->inner->get_schema(state->inner, schema);
        if (rc != 0) {
          return sanitize::Status::IOError(
              "coalescing stream inner get_schema failed");
        }
        return sanitize::Status::OK();
      });
}

int coalesce_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  if (!state || !state->inner) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_array_callback(
      out, state->last_error, "coalescing_stream.get_next",
      [&](ArrowArray *array) -> sanitize::Status {
        auto coalesced = std::make_unique<CoalescedArrayState>();
        std::int64_t rows = 0;
        bool has_batch = false;
        while (rows < state->target_rows) {
          sanitize::CArrayGuard batch;
          const int rc = state->inner->get_next(state->inner, batch.get());
          if (rc != 0) {
            return sanitize::Status::IOError(
                "coalescing stream inner get_next failed");
          }
          if (!batch.value().release) {
            break;
          }
          has_batch = true;
          rows += batch.value().length;
          SAN_RETURN_NOT_OK(
              append_node(state->root, &coalesced->root,
                          ArraySlice{&batch.value(), 0, batch.value().length}));
        }
        if (!has_batch) {
          sanitize::internal::cdata_stream::clear_array(array);
          return sanitize::Status::OK();
        }
        SAN_RETURN_NOT_OK(finish_node(&coalesced->root, state->root, true));
        return export_coalesced_array(std::move(coalesced), array);
      });
}

} // namespace core_abi3_internal::coalesce_detail

namespace core_abi3_internal {

PyObject *py_coalescing_stream_wrap(PyObject *, PyObject *args) {
  using namespace coalesce_detail;
  PyObject *stream_obj = nullptr;
  long long target_rows_arg = 65536;
  if (!PyArg_ParseTuple(args, "O|L:coalescing_stream_wrap", &stream_obj,
                        &target_rows_arg)) {
    return nullptr;
  }
  if (target_rows_arg <= 0) {
    PyErr_SetString(PyExc_ValueError, "target_rows must be positive");
    return nullptr;
  }

  PyObject *capsule = nullptr;
  ArrowArrayStream *inner = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &inner)) {
    return nullptr;
  }

  sanitize::CSchemaGuard schema;
  const int rc = inner->get_schema(inner, schema.get());
  if (rc != 0) {
    Py_DECREF(capsule);
    PyErr_SetString(PyExc_RuntimeError,
                    "coalescing stream inner get_schema failed");
    return nullptr;
  }

  CoalesceNodeSpec root;
  if (!schema_supported(schema.value(), &root)) {
    Py_DECREF(capsule);
    Py_RETURN_NONE;
  }

  auto state = std::make_unique<CoalesceStreamState>();
  state->inner = inner;
  state->stream_capsule = capsule;
  Py_INCREF(stream_obj);
  state->stream_obj = stream_obj;
  state->root = std::move(root);
  state->target_rows = static_cast<std::int64_t>(target_rows_arg);

  auto *wrapped = new (std::nothrow) ArrowArrayStream();
  if (!wrapped) {
    release_coalesce_stream(state.get());
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(wrapped, 0, sizeof(*wrapped));
  wrapped->get_schema = &coalesce_get_schema;
  wrapped->get_next = &coalesce_get_next;
  wrapped->get_last_error = &coalesce_last_error;
  wrapped->release = &coalesce_release;
  wrapped->private_data = state.release();
  return wrap_stream_capsule_with_keepalive(stream_obj, wrapped);
}

} // namespace core_abi3_internal

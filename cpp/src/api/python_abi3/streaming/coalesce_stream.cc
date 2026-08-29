/*
 * Implements the Arrow C stream batch-coalescing wrapper and Python entry
 * point.
 *
 * The phases validate schemas, append slices, and export one owned Arrow array
 * under budget.
 */

#include "api/python_abi3/streaming/coalesce_stream_internal.hh"

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/arrow_c/cdata_stream_runtime.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/runtime/process_identity.hh"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string_view>
#include <type_traits>
#include <utility>

namespace core_abi3_internal::coalesce_detail {

namespace {

/// Transfers an Arrow array structure and clears the source release callback.
void move_array(ArrowArray *source, ArrowArray *destination) noexcept {
  *destination = *source;
  sanitize::internal::cdata_stream::clear_array(source);
}

/// Releases pending array and clears transferred ownership to prevent reuse.
void release_pending_array(CoalesceStreamState *state) noexcept {
  if (!state) {
    return;
  }
  sanitize::internal::cdata_stream::release_array_nothrow(
      &state->pending_array);
  sanitize::internal::cdata_stream::clear_array(&state->pending_array);
  state->pending_offset = 0;
}

/// Loads and validates the next nonempty input batch when none is pending.
sanitize::Status ensure_pending_batch(CoalesceStreamState *state) {
  if (!state || !state->inner) {
    return sanitize::Status::Invalid("coalescing stream has no inner stream");
  }
  while (!state->pending_array.release && !state->inner_eof) {
    sanitize::CArrayGuard batch;
    const int rc = state->inner->get_next(state->inner, batch.get());
    if (rc != 0) {
      return sanitize::internal::cdata_stream::status_from_stream_error(
          rc, state->inner, "coalescing stream inner get_next failed");
    }
    if (!batch.value().release) {
      state->inner_eof = true;
      break;
    }
    if (batch.value().length < 0 || batch.value().offset < 0) {
      return sanitize::Status::Invalid("coalescing stream received an Arrow "
                                       "batch with negative length or offset");
    }
    if (batch.value().length == 0) {
      continue;
    }
    SAN_RETURN_NOT_OK(validate_arrow_node(
        state->root, batch.value(), 0, batch.value().length, 0,
        state->max_logical_slots, state->max_logical_buffer_bytes));
    move_array(batch.get(), &state->pending_array);
    state->pending_offset = 0;
  }
  return sanitize::Status::OK();
}

} // namespace

/// Releases coalesce stream and clears transferred ownership to prevent reuse.
void release_coalesce_stream(CoalesceStreamState *state) noexcept {
  if (!state || state->closed) {
    return;
  }
  release_pending_array(state);
  close_arrow_stream_keepalive(&state->inner, &state->stream_obj,
                               &state->stream_capsule, &state->closed);
}

/// Exposes the most recent Arrow batch coalescer failure through the Arrow C
/// Stream callback.
const char *coalesce_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid coalescing stream";
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

/// Releases the Arrow batch coalescer callback state and clears all transferred
/// Arrow ownership.
void coalesce_release(ArrowArrayStream *stream) {
  if (!sanitize::internal::runtime_owner_process()) {
    return;
  }
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<CoalesceStreamState *>(stream->private_data);
  release_coalesce_stream(state);
  sanitize::internal::detach_task_arena(stream);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

/// Exports the coalesced schema through the Arrow C Stream callback.
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

/// Produces the next Arrow C array through the Arrow batch coalescer stream
/// callback.
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
          SAN_RETURN_NOT_OK(ensure_pending_batch(state));
          if (!state->pending_array.release) {
            break;
          }

          const auto retained = retained_bytes(coalesced->root);
          const auto remaining_bytes = retained >= state->target_bytes
                                           ? std::size_t{0}
                                           : state->target_bytes - retained;
          const auto remaining_rows = state->target_rows - rows;
          auto slice_rows = fitting_slice_rows(
              state->root, state->pending_array, state->pending_offset,
              remaining_rows, remaining_bytes);
          if (slice_rows == 0) {
            if (has_batch) {
              break;
            }
            const auto hard_remaining = retained >= state->max_batch_bytes
                                            ? std::size_t{0}
                                            : state->max_batch_bytes - retained;
            if (fitting_slice_rows(state->root, state->pending_array,
                                   state->pending_offset, 1,
                                   hard_remaining) == 0) {
              return sanitize::Status::OutOfMemory(
                  "coalescing stream single row exceeds hard batch byte limit");
            }
            // A single Arrow row may exceed the preferred target, but never
            // the independent hard safety ceiling.
            slice_rows = 1;
          }

          SAN_RETURN_NOT_OK(
              append_node(state->root, &coalesced->root,
                          ArraySlice{&state->pending_array,
                                     state->pending_offset, slice_rows}));
          has_batch = true;
          rows += slice_rows;
          state->pending_offset += slice_rows;
          if (state->pending_offset >= state->pending_array.length) {
            release_pending_array(state);
          }
          const auto retained_after = retained_bytes(coalesced->root);
          if (retained_after > state->max_batch_bytes) {
            return sanitize::Status::OutOfMemory(
                "coalescing stream retained bytes exceed hard batch limit");
          }
          if (retained_after >= state->target_bytes) {
            break;
          }
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

/// Wraps an Arrow stream to coalesce adjacent batches within a memory budget.
PyObject *py_coalescing_stream_wrap(PyObject *, PyObject *args) {
  using namespace coalesce_detail;
  PyObject *stream_obj = nullptr;
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "O|L:coalescing_stream_wrap", &stream_obj,
                        &memory_limit_bytes)) {
    return nullptr;
  }
  if (memory_limit_bytes < -1 ||
      memory_limit_bytes > sanitize::internal::kHardMaxMemoryLimitBytes) {
    PyErr_SetString(
        PyExc_ValueError,
        "memory_limit_bytes must be -1 or within the 64 GiB safety ceiling");
    return nullptr;
  }
  const auto budget =
      sanitize::internal::memory_budget_from_limit(memory_limit_bytes);

  PyObject *capsule = nullptr;
  ArrowArrayStream *inner = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &inner)) {
    return nullptr;
  }
  std::unique_ptr<PyObject, decltype(&decref_with_gil)> capsule_owner(
      capsule, decref_with_gil);

  sanitize::CSchemaGuard schema;
  int rc = 0;
  try {
    rc = inner->get_schema(inner, schema.get());
  } catch (const std::bad_alloc &) {
    return PyErr_NoMemory();
  } catch (const std::exception &e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError,
                    "coalescing stream inner get_schema raised an exception");
    return nullptr;
  }
  if (rc != 0) {
    PyErr_SetString(PyExc_RuntimeError,
                    "coalescing stream inner get_schema failed");
    return nullptr;
  }

  CoalesceNodeSpec root;
  try {
    if (!schema_supported(schema.value(), &root)) {
      Py_RETURN_NONE;
    }
  } catch (const std::bad_alloc &) {
    return PyErr_NoMemory();
  } catch (const std::exception &e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return nullptr;
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError,
                    "coalescing stream schema parsing failed");
    return nullptr;
  }

  auto state = std::unique_ptr<CoalesceStreamState>(new (std::nothrow)
                                                        CoalesceStreamState());
  if (!state) {
    return PyErr_NoMemory();
  }
  state->inner = inner;
  state->stream_capsule = capsule_owner.release();
  Py_INCREF(stream_obj);
  state->stream_obj = stream_obj;
  state->root = std::move(root);
  state->target_rows = budget.parquet_row_group_rows;
  state->target_bytes =
      static_cast<std::size_t>(budget.parquet_row_group_bytes);
  state->max_batch_bytes = static_cast<std::size_t>(budget.coalesce_max_bytes);
  state->max_logical_slots = budget.arrow_logical_slots;
  state->max_logical_buffer_bytes = budget.arrow_logical_buffer_bytes;

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
  sanitize::internal::inherit_task_arena(wrapped, inner);
  return wrap_stream_capsule_with_keepalive(stream_obj, wrapped);
}

} // namespace core_abi3_internal

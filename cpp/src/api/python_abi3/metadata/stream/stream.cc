// Construction, lifecycle, and Arrow C callbacks for metadata streams.
#include "api/python_abi3/metadata/stream/stream.hh"

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/string_lookup.hh"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string_view>
#include <utility>

namespace core_abi3_internal {
namespace {

constexpr std::array<std::string_view, 4> kEtlColumnOrder{
    "schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"};

bool is_etl_column(std::string_view name) noexcept {
  return std::ranges::find(kEtlColumnOrder, name) != kEtlColumnOrder.end();
}

std::string_view base_child_name(const ArrowSchema &base,
                                 std::size_t index) noexcept {
  const ArrowSchema *child = base.children[index];
  return child && child->name ? std::string_view(child->name)
                              : std::string_view{};
}

void close_metadata_stream(MetadataStreamState *state) noexcept {
  if (!state || state->closed) {
    return;
  }
  if (state->inner && !state->stream_obj && !state->stream_capsule) {
    schema_sanitizer_stream_free(state->inner);
    state->inner = nullptr;
    state->closed = true;
    return;
  }
  close_arrow_stream_keepalive(&state->inner, &state->stream_obj,
                               &state->stream_capsule, &state->closed);
}

const char *metadata_stream_last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid metadata stream";
  }
  auto *state = static_cast<MetadataStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

void metadata_stream_release(ArrowArrayStream *stream) {
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<MetadataStreamState *>(stream->private_data);
  close_metadata_stream(state);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int metadata_stream_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  if (!stream || !stream->private_data) {
    return EINVAL;
  }
  auto *state = static_cast<MetadataStreamState *>(stream->private_data);
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, state->last_error, "metadata_stream.get_schema",
      [&](ArrowSchema *schema) {
        return build_metadata_schema(state, schema);
      });
}

int metadata_stream_get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream || !stream->private_data) {
    return EINVAL;
  }
  auto *state = static_cast<MetadataStreamState *>(stream->private_data);
  return sanitize::internal::cdata_stream::run_array_callback(
      out, state->last_error, "metadata_stream.get_next",
      [&](ArrowArray *array) { return build_metadata_array(state, array); });
}

bool append_metadata_columns(PyObject *first_row_columns,
                             PyObject *all_row_columns,
                             PyObject *row_span_columns,
                             PyObject *timestamp_columns,
                             std::vector<MetadataColumn> *columns) {
  if (!append_first_row_columns_from_dict(first_row_columns, columns)) {
    return false;
  }
  if (all_row_columns && all_row_columns != Py_None &&
      !append_all_row_columns_from_dict(all_row_columns, columns)) {
    return false;
  }
  if (row_span_columns && row_span_columns != Py_None &&
      !append_row_span_columns_from_dict(row_span_columns, columns)) {
    return false;
  }
  return !timestamp_columns || timestamp_columns == Py_None ||
         append_timestamp_columns_from_sequence(timestamp_columns, columns);
}

std::unique_ptr<MetadataStreamState> make_state(PyObject *first_row_columns,
                                                PyObject *all_row_columns,
                                                PyObject *row_span_columns,
                                                PyObject *timestamp_columns) {
  auto state = std::unique_ptr<MetadataStreamState>(new (std::nothrow)
                                                        MetadataStreamState());
  if (!state) {
    PyErr_NoMemory();
    return nullptr;
  }
  if (!append_metadata_columns(first_row_columns, all_row_columns,
                               row_span_columns, timestamp_columns,
                               &state->columns)) {
    return nullptr;
  }
  return state;
}

ArrowArrayStream *make_stream(std::unique_ptr<MetadataStreamState> state) {
  auto *wrapped = new (std::nothrow) ArrowArrayStream();
  if (!wrapped) {
    close_metadata_stream(state.get());
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(wrapped, 0, sizeof(*wrapped));
  wrapped->get_schema = &metadata_stream_get_schema;
  wrapped->get_next = &metadata_stream_get_next;
  wrapped->get_last_error = &metadata_stream_last_error;
  wrapped->release = &metadata_stream_release;
  wrapped->private_data = state.release();
  return wrapped;
}

} // namespace

sanitize::Status
prepare_metadata_child_layout(MetadataStreamState *stream_state,
                              const ArrowSchema &base_schema) {
  if (!stream_state || base_schema.n_children < 0) {
    return sanitize::Status::Invalid(
        "metadata stream base schema has invalid children");
  }
  const auto base_count = static_cast<std::size_t>(base_schema.n_children);
  sanitize::internal::BorrowedStringLookupSet names;
  names.reserve(base_count + stream_state->columns.size());
  for (std::size_t i = 0; i < base_count; ++i) {
    const auto name = base_child_name(base_schema, i);
    if (!name.empty() && !names.emplace(name).second) {
      return sanitize::Status::Invalid(
          "metadata stream base schema has duplicate column names");
    }
  }
  for (const auto &column : stream_state->columns) {
    if (!names.emplace(column.name).second) {
      return sanitize::Status::Invalid("generated metadata column '" +
                                       column.name +
                                       "' already exists in output schema");
    }
  }

  constexpr std::size_t kUnset = std::numeric_limits<std::size_t>::max();
  stream_state->base_child_output_indices.assign(base_count, kUnset);
  stream_state->metadata_child_output_indices.assign(
      stream_state->columns.size(), kUnset);
  std::size_t output_index = 0;
  for (std::size_t i = 0; i < base_count; ++i) {
    if (!is_etl_column(base_child_name(base_schema, i))) {
      stream_state->base_child_output_indices[i] = output_index++;
    }
  }
  for (std::size_t i = 0; i < stream_state->columns.size(); ++i) {
    if (!is_etl_column(stream_state->columns[i].name)) {
      stream_state->metadata_child_output_indices[i] = output_index++;
    }
  }
  for (const auto etl_name : kEtlColumnOrder) {
    for (std::size_t i = 0; i < base_count; ++i) {
      if (base_child_name(base_schema, i) == etl_name) {
        stream_state->base_child_output_indices[i] = output_index++;
      }
    }
    for (std::size_t i = 0; i < stream_state->columns.size(); ++i) {
      if (stream_state->columns[i].name == etl_name) {
        stream_state->metadata_child_output_indices[i] = output_index++;
      }
    }
  }
  stream_state->child_layout_ready = true;
  return sanitize::Status::OK();
}

ArrowArrayStream *make_metadata_stream_wrapper(PyObject *stream_obj,
                                               PyObject *first_row_columns,
                                               PyObject *all_row_columns,
                                               PyObject *row_span_columns,
                                               PyObject *timestamp_columns) {
  if (!stream_obj || !first_row_columns) {
    PyErr_SetString(PyExc_SystemError,
                    "metadata stream wrapper received null arguments");
    return nullptr;
  }
  auto state = make_state(first_row_columns, all_row_columns, row_span_columns,
                          timestamp_columns);
  if (!state) {
    return nullptr;
  }
  PyObject *capsule = nullptr;
  ArrowArrayStream *inner = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &inner)) {
    return nullptr;
  }
  state->inner = inner;
  state->stream_capsule = capsule;
  Py_INCREF(stream_obj);
  state->stream_obj = stream_obj;
  return make_stream(std::move(state));
}

ArrowArrayStream *make_metadata_stream_wrapper_from_stream(
    ArrowArrayStream *inner, PyObject *first_row_columns,
    PyObject *all_row_columns, PyObject *row_span_columns,
    PyObject *timestamp_columns) {
  if (!inner || !first_row_columns) {
    PyErr_SetString(PyExc_SystemError,
                    "metadata stream wrapper received null native stream");
    return nullptr;
  }
  auto state = make_state(first_row_columns, all_row_columns, row_span_columns,
                          timestamp_columns);
  if (!state) {
    return nullptr;
  }
  state->inner = inner;
  return make_stream(std::move(state));
}

PyObject *py_metadata_stream_wrap(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  PyObject *first_row_columns = nullptr;
  PyObject *all_row_columns = nullptr;
  PyObject *row_span_columns = nullptr;
  PyObject *timestamp_columns = nullptr;
  if (!PyArg_ParseTuple(args, "OO|OOO:metadata_stream_wrap", &stream_obj,
                        &first_row_columns, &all_row_columns, &row_span_columns,
                        &timestamp_columns)) {
    return nullptr;
  }
  ArrowArrayStream *wrapped = make_metadata_stream_wrapper(
      stream_obj, first_row_columns, all_row_columns, row_span_columns,
      timestamp_columns);
  return wrapped ? wrap_stream_capsule_with_keepalive(stream_obj, wrapped)
                 : nullptr;
}

} // namespace core_abi3_internal

// Construction, lifecycle, and Arrow C callbacks for metadata streams.
#include "api/python_abi3/metadata/stream/stream.hh"

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/arrow_c/cdata_stream_runtime.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/memory/size_math.hh"
#include "internal/runtime/process_identity.hh"
#include "internal/string_lookup.hh"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>

namespace core_abi3_internal {
namespace {

constexpr std::array<std::string_view, 4> kEtlColumnOrder{
    "schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"};

constexpr std::size_t kGeneratedColumnShellEstimateBytes = 256;

std::int64_t add_estimated_bytes(std::int64_t total,
                                 std::int64_t incoming) noexcept {
  return sanitize::internal::saturating_add_i64(total, incoming);
}

std::int64_t estimate_row_span_data_bytes(const MetadataColumn &column,
                                          std::int64_t length) noexcept {
  std::size_t span_index = column.span_index;
  std::int64_t span_offset = column.span_offset;
  std::int64_t row = 0;
  std::int64_t total = 0;
  while (row < length && span_index < column.spans.size()) {
    const MetadataSpan &span = column.spans[span_index];
    const std::int64_t remaining = span.row_count - span_offset;
    if (remaining <= 0) {
      ++span_index;
      span_offset = 0;
      continue;
    }
    const std::int64_t take = std::min(length - row, remaining);
    if (!span.is_null) {
      total = add_estimated_bytes(
          total, sanitize::internal::saturating_capacity_bytes(
                     static_cast<std::size_t>(take), span.value.size()));
    }
    row += take;
    span_offset += take;
    if (span_offset >= span.row_count) {
      ++span_index;
      span_offset = 0;
    }
  }
  return total;
}

std::int64_t
estimate_generated_metadata_bytes(const MetadataStreamState &stream_state,
                                  const ArrowArray &base,
                                  std::size_t timestamp_count) noexcept {
  const auto length = static_cast<std::size_t>(base.length);
  std::int64_t total = sanitize::internal::saturating_capacity_bytes(
      static_cast<std::size_t>(base.n_children) + stream_state.columns.size(),
      sizeof(ArrowArray *));
  total =
      add_estimated_bytes(total, sanitize::internal::saturating_capacity_bytes(
                                     stream_state.columns.size(),
                                     kGeneratedColumnShellEstimateBytes));
  for (const auto &column : stream_state.columns) {
    if (column.placement == MetadataColumnPlacement::AllRowsTimestampMicros) {
      total = add_estimated_bytes(total,
                                  sanitize::internal::saturating_capacity_bytes(
                                      length, sizeof(std::int64_t)));
      continue;
    }
    total = add_estimated_bytes(total,
                                sanitize::internal::saturating_capacity_bytes(
                                    length + 1U, sizeof(std::int32_t)));
    total = add_estimated_bytes(total,
                                sanitize::internal::saturating_capacity_bytes(
                                    (length + 7U) / 8U, sizeof(std::uint8_t)));
    std::int64_t data_bytes = 0;
    if (!column.is_null &&
        column.placement == MetadataColumnPlacement::AllRowsUtf8) {
      data_bytes = sanitize::internal::saturating_capacity_bytes(
          length, column.value.size());
    } else if (!column.is_null &&
               column.placement == MetadataColumnPlacement::RowSpanUtf8) {
      data_bytes = estimate_row_span_data_bytes(column, base.length);
    } else if (!column.is_null && stream_state.first_row_pending &&
               base.length > 0) {
      data_bytes =
          sanitize::internal::saturating_size_to_i64(column.value.size());
    }
    total = add_estimated_bytes(total, data_bytes);
  }
  (void)timestamp_count;
  return total;
}

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
  if (!sanitize::internal::runtime_owner_process()) {
    return;
  }
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<MetadataStreamState *>(stream->private_data);
  close_metadata_stream(state);
  sanitize::internal::detach_task_arena(stream);
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
         append_timestamp_columns(timestamp_columns, columns);
}

std::unique_ptr<MetadataStreamState>
make_state(PyObject *first_row_columns, PyObject *all_row_columns,
           PyObject *row_span_columns, PyObject *timestamp_columns,
           std::int64_t memory_limit_bytes) {
  auto state = std::unique_ptr<MetadataStreamState>(new (std::nothrow)
                                                        MetadataStreamState());
  if (!state) {
    PyErr_NoMemory();
    return nullptr;
  }
  configure_metadata_stream_budget(state.get(), memory_limit_bytes);
  if (!append_metadata_columns(first_row_columns, all_row_columns,
                               row_span_columns, timestamp_columns,
                               &state->columns)) {
    return nullptr;
  }
  if (state->columns.size() > kMaxMetadataStreamColumns) {
    PyErr_SetString(PyExc_ValueError,
                    "generated metadata column count exceeds safety limit");
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
  auto *inner = state->inner;
  wrapped->private_data = state.release();
  sanitize::internal::inherit_task_arena(wrapped, inner);
  return wrapped;
}

} // namespace

void configure_metadata_stream_budget(
    MetadataStreamState *stream_state,
    std::int64_t memory_limit_bytes) noexcept {
  if (!stream_state) {
    return;
  }
  const auto budget =
      sanitize::internal::memory_budget_from_limit(memory_limit_bytes);
  stream_state->configured_memory_limit_bytes = memory_limit_bytes;
  stream_state->max_generated_metadata_bytes = budget.metadata_bytes;
  stream_state->max_logical_slots = budget.arrow_logical_slots;
}

sanitize::Status
prepare_metadata_child_layout(MetadataStreamState *stream_state,
                              const ArrowSchema &base_schema) {
  if (!stream_state || base_schema.n_children < 0 ||
      base_schema.n_children >
          static_cast<std::int64_t>(kMaxMetadataStreamColumns) ||
      (base_schema.n_children > 0 && !base_schema.children)) {
    return sanitize::Status::Invalid(
        "metadata stream base schema has invalid children");
  }
  const auto base_count = static_cast<std::size_t>(base_schema.n_children);
  if (stream_state->columns.size() > kMaxMetadataStreamColumns - base_count) {
    return sanitize::Status::Invalid(
        "metadata stream output column count exceeds safety limit");
  }
  sanitize::internal::BorrowedStringLookupSet names;
  names.reserve(base_count + stream_state->columns.size());
  for (std::size_t i = 0; i < base_count; ++i) {
    if (!base_schema.children[i]) {
      return sanitize::Status::Invalid(
          "metadata stream base schema contains a null child");
    }
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

sanitize::Status
validate_generated_metadata_budget(const MetadataStreamState &stream_state,
                                   const ArrowArray &base,
                                   std::size_t timestamp_count) {
  const auto limit = stream_state.max_generated_metadata_bytes;
  const auto estimated =
      estimate_generated_metadata_bytes(stream_state, base, timestamp_count);
  if (estimated > limit) {
    return sanitize::Status::OutOfMemory(
        "generated metadata batch exceeds byte safety limit "
        "(estimated_bytes=" +
        std::to_string(estimated) + ", limit_bytes=" + std::to_string(limit) +
        ", rows=" + std::to_string(base.length) +
        ", base_columns=" + std::to_string(base.n_children) +
        ", generated_columns=" + std::to_string(stream_state.columns.size()) +
        ", configured_memory_limit_bytes=" +
        std::to_string(stream_state.configured_memory_limit_bytes) + ")");
  }
  return sanitize::Status::OK();
}

sanitize::Status
validate_metadata_base_array(const MetadataStreamState &stream_state,
                             const ArrowArray &base) {
  if (base.offset != 0) {
    return sanitize::Status::Invalid(
        "metadata stream base array root offset must be zero");
  }
  if (base.length < 0 || base.length > kMaxMetadataBatchRows ||
      base.null_count < -1 || base.null_count > base.length ||
      base.n_buffers < 0 || base.n_children < 0 ||
      base.n_children > static_cast<std::int64_t>(kMaxMetadataStreamColumns) ||
      (base.n_children > 0 && !base.children)) {
    return sanitize::Status::Invalid(
        "metadata stream base array has invalid logical metadata");
  }
  if (base.offset > std::numeric_limits<std::int64_t>::max() - base.length) {
    return sanitize::Status::Invalid(
        "metadata stream base array logical range overflows int64");
  }
  const auto max_slots = stream_state.max_logical_slots;
  if (base.offset + base.length > max_slots) {
    return sanitize::Status::OutOfMemory(
        "metadata stream base array exceeds logical slot limit");
  }
  const auto base_children = static_cast<std::size_t>(base.n_children);
  if (base_children != stream_state.base_child_output_indices.size() ||
      stream_state.columns.size() > kMaxMetadataStreamColumns - base_children) {
    return sanitize::Status::Invalid(
        "metadata stream base array does not match its schema layout");
  }
  for (std::size_t i = 0; i < base_children; ++i) {
    if (!base.children[i]) {
      return sanitize::Status::Invalid(
          "metadata stream base array contains a null child");
    }
  }
  return sanitize::Status::OK();
}

ArrowArrayStream *make_metadata_stream_wrapper(
    PyObject *stream_obj, PyObject *first_row_columns,
    PyObject *all_row_columns, PyObject *row_span_columns,
    PyObject *timestamp_columns, std::int64_t memory_limit_bytes) {
  if (!stream_obj || !first_row_columns) {
    PyErr_SetString(PyExc_SystemError,
                    "metadata stream wrapper received null arguments");
    return nullptr;
  }
  auto state = make_state(first_row_columns, all_row_columns, row_span_columns,
                          timestamp_columns, memory_limit_bytes);
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
    PyObject *timestamp_columns, std::int64_t memory_limit_bytes) {
  if (!inner || !first_row_columns) {
    PyErr_SetString(PyExc_SystemError,
                    "metadata stream wrapper received null native stream");
    return nullptr;
  }
  auto state = make_state(first_row_columns, all_row_columns, row_span_columns,
                          timestamp_columns, memory_limit_bytes);
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
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "OO|OOOL:metadata_stream_wrap", &stream_obj,
                        &first_row_columns, &all_row_columns, &row_span_columns,
                        &timestamp_columns, &memory_limit_bytes)) {
    return nullptr;
  }
  ArrowArrayStream *wrapped = make_metadata_stream_wrapper(
      stream_obj, first_row_columns, all_row_columns, row_span_columns,
      timestamp_columns, memory_limit_bytes);
  return wrapped ? wrap_stream_capsule_with_keepalive(stream_obj, wrapped)
                 : nullptr;
}

} // namespace core_abi3_internal

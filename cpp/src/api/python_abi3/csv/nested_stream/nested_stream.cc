/*
 * Owns CSV nested-column Arrow C Stream rewriting and its Python wrapper.
 */
#include "api/python_abi3/csv/nested_stream/state.hh"

#include "api/python_abi3/arrow_stream/_core_abi3_arrow_stream_lifecycle.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/arrow_c/cdata_stream_runtime.hh"
#include "internal/json_output/jsonl_value_writer.hh"
#include "internal/runtime/process_identity.hh"

#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>

namespace core_abi3_internal::csv_nested_stream {
namespace {

constexpr std::int64_t kMaxCsvNestedColumns = 65'536;
constexpr std::int64_t kMaxCsvNestedRows = std::int64_t{1} << 24;

bool validity_bit_is_set(const std::uint8_t *bitmap, std::int64_t index) {
  return (bitmap[index >> 3] & static_cast<std::uint8_t>(1u << (index & 7))) !=
         0;
}

bool array_is_null(const ArrowArray &array, std::int64_t row) {
  if (array.null_count == 0 || !array.buffers || !array.buffers[0]) {
    return false;
  }
  const auto *bitmap = static_cast<const std::uint8_t *>(array.buffers[0]);
  return !validity_bit_is_set(bitmap, array.offset + row);
}

void clear_validity_bit(std::vector<std::uint8_t> *validity,
                        std::int64_t index) {
  (*validity)[static_cast<std::size_t>(index >> 3)] &=
      static_cast<std::uint8_t>(~(1u << (index & 7)));
}

bool is_nested_kind(jsonl::JsonlKind kind) {
  return kind == jsonl::JsonlKind::kStruct || kind == jsonl::JsonlKind::kList ||
         kind == jsonl::JsonlKind::kLargeList || kind == jsonl::JsonlKind::kMap;
}

void clear_schema(ArrowSchema *schema) noexcept {
  sanitize::internal::cdata_stream::clear_schema(schema);
}

void clear_array(ArrowArray *array) noexcept {
  sanitize::internal::cdata_stream::clear_array(array);
}

void csv_nested_schema_child_release(ArrowSchema *schema) {
  if (!schema || !schema->release) {
    return;
  }
  clear_schema(schema);
}

void csv_nested_array_child_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  clear_array(array);
}

void csv_nested_schema_release(ArrowSchema *schema) {
  if (!schema || !schema->release) {
    return;
  }
  auto *state = static_cast<CsvNestedSchemaState *>(schema->private_data);
  delete state;
  clear_schema(schema);
}

void csv_nested_array_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  auto *state = static_cast<CsvNestedArrayState *>(array->private_data);
  delete state;
  clear_array(array);
}

sanitize::Status load_csv_nested_schema(CsvNestedStreamState *stream_state,
                                        ArrowSchema *base_schema) {
  stream_state->columns.clear();
  stream_state->nested_column_count = 0;
  if (!base_schema->format || std::strcmp(base_schema->format, "+s") != 0 ||
      base_schema->n_children < 0) {
    return sanitize::Status::Invalid("CSV nested stream: invalid root schema");
  }
  if (base_schema->n_children > kMaxCsvNestedColumns) {
    return sanitize::Status::OutOfMemory(
        "CSV nested stream: schema child count exceeds safety limit");
  }
  if (base_schema->n_children > 0 && !base_schema->children) {
    return sanitize::Status::Invalid(
        "CSV nested stream: schema children are missing");
  }
  stream_state->columns.reserve(
      static_cast<std::size_t>(base_schema->n_children));
  for (std::int64_t i = 0; i < base_schema->n_children; ++i) {
    if (!base_schema->children || !base_schema->children[i]) {
      return sanitize::Status::Invalid(
          "CSV nested stream: missing schema child");
    }
    SAN_ASSIGN_OR_RAISE(auto field,
                        jsonl::parse_schema_field(*base_schema->children[i]));
    CsvNestedColumnPlan plan;
    if (is_nested_kind(field.kind)) {
      plan.nested_slot = stream_state->nested_column_count++;
    }
    plan.field = std::move(field);
    stream_state->columns.push_back(std::move(plan));
  }
  stream_state->schema_loaded = true;
  return sanitize::Status::OK();
}

sanitize::Status append_schema_children(CsvNestedStreamState *stream_state,
                                        CsvNestedSchemaState *schema_state) {
  ArrowSchema &base = schema_state->base.value();
  const std::int64_t base_children = base.n_children;
  schema_state->children.reserve(static_cast<std::size_t>(base_children));
  schema_state->nested_fields.reserve(stream_state->nested_column_count);
  for (std::int64_t i = 0; i < base_children; ++i) {
    const auto &column = stream_state->columns[static_cast<std::size_t>(i)];
    if (!column.nested_slot.has_value()) {
      schema_state->children.push_back(base.children[i]);
      continue;
    }
    CsvNestedSchemaChild child;
    child.name = column.field.name;
    schema_state->nested_fields.push_back(std::move(child));
    auto &stored = schema_state->nested_fields.back();
    clear_schema(&stored.schema);
    stored.schema.format = "u";
    stored.schema.name = stored.name.c_str();
    stored.schema.metadata = nullptr;
    stored.schema.flags = base.children[i]->flags;
    stored.schema.n_children = 0;
    stored.schema.children = nullptr;
    stored.schema.dictionary = nullptr;
    stored.schema.private_data = nullptr;
    stored.schema.release = &csv_nested_schema_child_release;
    schema_state->children.push_back(&stored.schema);
  }
  return sanitize::Status::OK();
}

sanitize::Status build_nested_utf8_array(CsvNestedUtf8Array *out,
                                         const jsonl::JsonlField &field,
                                         const ArrowArray &array,
                                         std::int64_t length) {
  if (length < 0) {
    return sanitize::Status::Invalid(
        "CSV nested stream: negative array length");
  }
  if (length > kMaxCsvNestedRows) {
    return sanitize::Status::OutOfMemory(
        "CSV nested stream: row count exceeds safety limit");
  }
  if (static_cast<std::uint64_t>(length) >=
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::OutOfMemory(
        "CSV nested stream: offset count exceeds platform limits");
  }
  out->offsets.reserve(static_cast<std::size_t>(length) + 1);
  out->offsets.push_back(0);
  std::int64_t null_count = 0;
  const auto validity_bytes = static_cast<std::size_t>((length + 7) / 8);
  for (std::int64_t row = 0; row < length; ++row) {
    if (array_is_null(array, row)) {
      if (out->validity.empty()) {
        out->validity.assign(validity_bytes, 0xFF);
      }
      clear_validity_bit(&out->validity, row);
      ++null_count;
      out->offsets.push_back(out->offsets.back());
      continue;
    }
    const std::size_t before = out->data.size();
    SAN_RETURN_NOT_OK(jsonl::append_value(out->data, field, array, row));
    const auto added = out->data.size() - before;
    const auto previous = static_cast<std::size_t>(out->offsets.back());
    if (added > static_cast<std::size_t>(INT32_MAX) - previous) {
      return sanitize::Status::OutOfMemory(
          "CSV nested stream: UTF-8 data exceeds 32-bit offset limit");
    }
    out->offsets.push_back(static_cast<std::int32_t>(previous + added));
  }

  out->buffers[0] = out->validity.empty()
                        ? nullptr
                        : static_cast<const void *>(out->validity.data());
  out->buffers[1] = out->offsets.empty()
                        ? nullptr
                        : static_cast<const void *>(out->offsets.data());
  out->buffers[2] =
      out->data.empty() ? nullptr : static_cast<const void *>(out->data.data());

  clear_array(&out->array);
  out->array.length = length;
  out->array.null_count = null_count;
  out->array.offset = 0;
  out->array.n_buffers = 3;
  out->array.n_children = 0;
  out->array.buffers = out->buffers;
  out->array.children = nullptr;
  out->array.dictionary = nullptr;
  out->array.release = &csv_nested_array_child_release;
  out->array.private_data = nullptr;
  return sanitize::Status::OK();
}

} // namespace

void close_stream(CsvNestedStreamState *state) noexcept {
  if (!state) {
    return;
  }
  close_arrow_stream_keepalive(&state->inner, &state->stream_obj,
                               &state->stream_capsule, &state->closed);
}

const char *last_error(ArrowArrayStream *stream) {
  if (!stream) {
    return "invalid CSV nested stream";
  }
  auto *state = static_cast<CsvNestedStreamState *>(stream->private_data);
  return state ? sanitize::internal::cdata_stream::last_error_ptr(
                     state->last_error)
               : nullptr;
}

void release_stream(ArrowArrayStream *stream) {
  if (!sanitize::internal::runtime_owner_process()) {
    return;
  }
  if (!stream || !stream->release) {
    return;
  }
  auto *state = static_cast<CsvNestedStreamState *>(stream->private_data);
  close_stream(state);
  sanitize::internal::detach_task_arena(stream);
  delete state;
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *stream_state =
      static_cast<CsvNestedStreamState *>(stream->private_data);
  if (!stream_state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, stream_state->last_error, "csv_nested_stream.get_schema",
      [&](ArrowSchema *schema) {
        std::unique_ptr<CsvNestedSchemaState> schema_state(
            new (std::nothrow) CsvNestedSchemaState());
        if (!schema_state) {
          return sanitize::Status::OutOfMemory("CSV nested stream schema OOM");
        }
        const int rc = stream_state->inner->get_schema(
            stream_state->inner, schema_state->base.get());
        if (rc != 0) {
          return sanitize::Status::IOError(
              "CSV nested stream inner get_schema failed");
        }
        ArrowSchema &base_schema = schema_state->base.value();
        SAN_RETURN_NOT_OK(load_csv_nested_schema(stream_state, &base_schema));
        SAN_RETURN_NOT_OK(
            append_schema_children(stream_state, schema_state.get()));

        clear_schema(schema);
        schema->format = base_schema.format;
        schema->name = base_schema.name;
        schema->metadata = base_schema.metadata;
        schema->flags = base_schema.flags;
        schema->n_children =
            static_cast<std::int64_t>(schema_state->children.size());
        schema->children = schema_state->children.empty()
                               ? nullptr
                               : schema_state->children.data();
        schema->dictionary = base_schema.dictionary;
        schema->private_data = schema_state.release();
        schema->release = &csv_nested_schema_release;
        return sanitize::Status::OK();
      });
}

int get_next(ArrowArrayStream *stream, ArrowArray *out) {
  if (!stream) {
    return EINVAL;
  }
  auto *stream_state =
      static_cast<CsvNestedStreamState *>(stream->private_data);
  if (!stream_state) {
    return EINVAL;
  }
  return sanitize::internal::cdata_stream::run_array_callback(
      out, stream_state->last_error, "csv_nested_stream.get_next",
      [&](ArrowArray *array) {
        if (!stream_state->schema_loaded) {
          return sanitize::Status::Invalid(
              "CSV nested stream schema must be loaded before batches");
        }
        std::unique_ptr<CsvNestedArrayState> state(new (std::nothrow)
                                                       CsvNestedArrayState());
        if (!state) {
          return sanitize::Status::OutOfMemory("CSV nested stream array OOM");
        }
        const int rc = stream_state->inner->get_next(stream_state->inner,
                                                     state->base.get());
        if (rc != 0) {
          return sanitize::internal::cdata_stream::status_from_stream_error(
              rc, stream_state->inner,
              "CSV nested stream inner get_next failed");
        }
        ArrowArray &base_array = state->base.value();
        if (!base_array.release) {
          clear_array(array);
          return sanitize::Status::OK();
        }
        if (base_array.length < 0 || base_array.offset != 0 ||
            base_array.null_count < -1 ||
            base_array.null_count > base_array.length ||
            base_array.n_buffers != 1 || !base_array.buffers ||
            (base_array.null_count > 0 && !base_array.buffers[0]) ||
            base_array.n_children !=
                static_cast<std::int64_t>(stream_state->columns.size()) ||
            (base_array.n_children > 0 && !base_array.children)) {
          return sanitize::Status::Invalid(
              "CSV nested stream array/schema mismatch");
        }

        const std::int64_t length = base_array.length;
        if (length > kMaxCsvNestedRows) {
          return sanitize::Status::OutOfMemory(
              "CSV nested stream: row count exceeds safety limit");
        }
        for (std::size_t i = 0; i < stream_state->columns.size(); ++i) {
          if (!base_array.children[i]) {
            return sanitize::Status::Invalid(
                "CSV nested stream: array child is missing");
          }
          SAN_RETURN_NOT_OK(jsonl::validate_array_slice(
              stream_state->columns[i].field, *base_array.children[i], 0,
              length, stream_state->validation_limits));
        }

        state->nested_arrays.resize(stream_state->nested_column_count);
        state->children.reserve(stream_state->columns.size());
        for (std::size_t i = 0; i < stream_state->columns.size(); ++i) {
          const auto &column = stream_state->columns[i];
          if (!column.nested_slot.has_value()) {
            state->children.push_back(base_array.children[i]);
            continue;
          }
          auto &nested_array = state->nested_arrays[*column.nested_slot];
          SAN_RETURN_NOT_OK(build_nested_utf8_array(
              &nested_array, column.field, *base_array.children[i], length));
          state->children.push_back(&nested_array.array);
        }

        clear_array(array);
        array->length = length;
        array->null_count = base_array.null_count;
        array->offset = 0;
        array->n_buffers = base_array.n_buffers;
        array->buffers =
            base_array.buffers ? base_array.buffers : state->struct_buffers;
        array->n_children = static_cast<std::int64_t>(state->children.size());
        array->children =
            state->children.empty() ? nullptr : state->children.data();
        array->dictionary = base_array.dictionary;
        array->private_data = state.release();
        array->release = &csv_nested_array_release;
        return sanitize::Status::OK();
      });
}

} // namespace core_abi3_internal::csv_nested_stream

namespace core_abi3_internal {

PyObject *py_csv_nested_stream_wrap(PyObject *, PyObject *args) {
  PyObject *stream_obj = nullptr;
  long long memory_limit_bytes = -1;
  if (!PyArg_ParseTuple(args, "O|L:csv_nested_stream_wrap", &stream_obj,
                        &memory_limit_bytes)) {
    return nullptr;
  }

  std::unique_ptr<csv_nested_stream::CsvNestedStreamState> state(
      new (std::nothrow) csv_nested_stream::CsvNestedStreamState());
  if (!state) {
    PyErr_NoMemory();
    return nullptr;
  }
  state->validation_limits =
      csv_nested_stream::jsonl::array_validation_limits(memory_limit_bytes);

  PyObject *capsule = nullptr;
  ArrowArrayStream *inner = nullptr;
  if (!acquire_arrow_stream(stream_obj, &capsule, &inner)) {
    return nullptr;
  }
  state->inner = inner;
  state->stream_capsule = capsule;
  Py_INCREF(stream_obj);
  state->stream_obj = stream_obj;

  auto *wrapped = new (std::nothrow) ArrowArrayStream();
  if (!wrapped) {
    csv_nested_stream::close_stream(state.get());
    PyErr_NoMemory();
    return nullptr;
  }
  std::memset(wrapped, 0, sizeof(*wrapped));
  wrapped->get_schema = &csv_nested_stream::get_schema;
  wrapped->get_next = &csv_nested_stream::get_next;
  wrapped->get_last_error = &csv_nested_stream::last_error;
  wrapped->release = &csv_nested_stream::release_stream;
  wrapped->private_data = state.release();
  sanitize::internal::inherit_task_arena(wrapped, inner);

  return wrap_stream_capsule_with_keepalive(stream_obj, wrapped);
}

} // namespace core_abi3_internal

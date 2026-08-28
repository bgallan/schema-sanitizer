/* Python ABI3 context and diagnostics wrappers. */
#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"
#include "internal/abi/python_abi3/native_state.hh"

#include <algorithm>
#include <cstdint>
#include <exception>
#include <memory>
#include <new>
#include <string>

#include "internal/json_encoding/token_writer.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/runtime/performance_telemetry.hh"

namespace core_abi3_internal {
namespace {

PyObject *unicode_from_string(const std::string &value) {
  return PyUnicode_FromStringAndSize(value.data(),
                                     static_cast<Py_ssize_t>(value.size()));
}

void raise_native_exception(const char *where,
                            const std::exception &error) noexcept {
  PyErr_Format(PyExc_RuntimeError, "%s: %s", where, error.what());
}

std::string context_memory_stats_json(const NativeContext &context) {
  auto *pool = static_cast<sanitize::internal::MemoryPool *>(
      context.ctx->memory_pool_handle());
  std::string json;
  json.reserve(256);
  json.push_back('{');
  bool first = true;
  sanitize::internal::json_encoding::append_string_field(
      json, first, "backend_name", pool ? pool->backend_name() : "");
  sanitize::internal::json_encoding::append_int_field(
      json, first, "bytes_allocated",
      pool ? pool->bytes_allocated() : std::int64_t{0});
  sanitize::internal::json_encoding::append_int_field(
      json, first, "max_memory", pool ? pool->max_memory() : std::int64_t{0});
  sanitize::internal::json_encoding::append_int_field(
      json, first, "allocation_count",
      pool ? pool->allocation_count() : std::int64_t{0});
  sanitize::internal::json_encoding::append_int_field(
      json, first, "invalid_free_count",
      pool ? pool->invalid_free_count() : std::int64_t{0});
  sanitize::internal::json_encoding::append_int_field(
      json, first, "size_mismatch_count",
      pool ? pool->size_mismatch_count() : std::int64_t{0});
  sanitize::internal::json_encoding::append_int_field(
      json, first, "corruption_count",
      pool ? pool->corruption_count() : std::int64_t{0});
  sanitize::internal::json_encoding::append_int_field(
      json, first, "limit_bytes",
      pool ? pool->limit_bytes() : std::int64_t{-1});
  json.push_back('}');
  return json;
}

std::string diagnostics_json(const NativeDiagnostics &diagnostics) {
  sanitize::IngestDiagnostics merged = *diagnostics.diagnostics;
  if (diagnostics.has_inference_snapshot) {
    const auto &snapshot = diagnostics.inference_snapshot;
    merged.inferred_rows = snapshot.inferred_rows;
    merged.inferred_bytes = snapshot.inferred_bytes;
    merged.arrow_schema_depth = snapshot.arrow_schema_depth;
    merged.parquet_schema_depth = snapshot.parquet_schema_depth;
    merged.flattened_fields = snapshot.flattened_fields;
    merged.scalar_wrappings = snapshot.scalar_wrappings;
    merged.peak_charged_memory_bytes = std::max(
        merged.peak_charged_memory_bytes, snapshot.peak_charged_memory_bytes);
    merged.operation_memory_limit_bytes =
        std::max(merged.operation_memory_limit_bytes,
                 snapshot.operation_memory_limit_bytes);
    merged.reader.parser_max_depth = std::max(merged.reader.parser_max_depth,
                                              snapshot.reader.parser_max_depth);
    if (merged.reader.decoded_bytes == 0) {
      merged.reader.decoded_bytes = snapshot.reader.decoded_bytes;
    }
    if (merged.reader.records == 0) {
      merged.reader.records = snapshot.reader.records;
    }
    if (merged.reader.nodes == 0) {
      merged.reader.nodes = snapshot.reader.nodes;
    }
    if (merged.reader.compressed_bytes == 0) {
      merged.reader.compressed_bytes = snapshot.reader.compressed_bytes;
    }
    if (merged.reader.decompressed_bytes == 0) {
      merged.reader.decompressed_bytes = snapshot.reader.decompressed_bytes;
    }
  }
  return merged.to_json();
}

} // namespace

PyObject *py_context_new(PyObject *, PyObject *) {
  try {
    auto context = std::make_unique<NativeContext>();
    context->ctx = std::make_shared<sanitize::ExecutionContext>();
    install_python_interrupt_check(context.get());
    return wrap_context_capsule(context.release());
  } catch (const std::bad_alloc &) {
    PyErr_NoMemory();
  } catch (const std::exception &error) {
    raise_native_exception("context_new", error);
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError, "context_new: unknown error");
  }
  return nullptr;
}

PyObject *py_context_memory_stats_json(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:context_memory_stats_json", &ctx_obj)) {
    return nullptr;
  }
  auto *context = unwrap_context(ctx_obj);
  if (!context || !context->ctx) {
    return nullptr;
  }
  try {
    return unicode_from_string(context_memory_stats_json(*context));
  } catch (const std::bad_alloc &) {
    PyErr_NoMemory();
  } catch (const std::exception &error) {
    raise_native_exception("context_memory_stats_json", error);
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError,
                    "context_memory_stats_json: unknown error");
  }
  return nullptr;
}

PyObject *py_context_performance_stats_json(PyObject *, PyObject *args) {
  PyObject *ctx_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:context_performance_stats_json", &ctx_obj)) {
    return nullptr;
  }
  auto *context = unwrap_context(ctx_obj);
  if (!context || !context->ctx) {
    return nullptr;
  }
  try {
    const auto telemetry = context->ctx->performance_telemetry();
    return unicode_from_string(telemetry ? telemetry->ToJson() : "{}");
  } catch (const std::bad_alloc &) {
    PyErr_NoMemory();
  } catch (const std::exception &error) {
    raise_native_exception("context_performance_stats_json", error);
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError,
                    "context_performance_stats_json: unknown error");
  }
  return nullptr;
}

PyObject *py_diagnostics_json(PyObject *, PyObject *args) {
  PyObject *diagnostics_obj = nullptr;
  if (!PyArg_ParseTuple(args, "O:diagnostics_json", &diagnostics_obj)) {
    return nullptr;
  }
  auto *diagnostics = unwrap_diagnostics(diagnostics_obj);
  if (!diagnostics || !diagnostics->diagnostics) {
    return nullptr;
  }
  try {
    return unicode_from_string(diagnostics_json(*diagnostics));
  } catch (const std::bad_alloc &) {
    PyErr_NoMemory();
  } catch (const std::exception &error) {
    raise_native_exception("diagnostics_json", error);
  } catch (...) {
    PyErr_SetString(PyExc_RuntimeError, "diagnostics_json: unknown error");
  }
  return nullptr;
}

} // namespace core_abi3_internal

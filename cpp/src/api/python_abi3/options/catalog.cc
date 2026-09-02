// Exposes catalog-backed option metadata from the C++ source of truth. The
// boundary converts Python inputs into validated native policy and
// memory-budget state.

#include "internal/abi/python_abi3/methods.hh"

#include "internal/memory/memory_budget.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/runtime/execution_policy.hh"
#include "sanitize/options/options.hh"

#include <concepts>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace core_abi3_internal {
namespace {

using LogicalSchemaOption = std::optional<sanitize::LogicalSchema>;
using StringListOption = std::vector<std::string>;

/// Classifies an option descriptor into its Python catalog value category.
template <class T> constexpr std::string_view option_kind() {
  if constexpr (std::same_as<T, bool>) {
    return "bool";
  } else if constexpr (std::same_as<T, std::int32_t>) {
    return "i32";
  } else if constexpr (std::same_as<T, std::int64_t>) {
    return "i64";
  } else if constexpr (std::same_as<T, std::string>) {
    return "string";
  } else if constexpr (std::same_as<T, StringListOption>) {
    return "string_list";
  } else if constexpr (std::same_as<T, LogicalSchemaOption>) {
    return "logical_schema";
  } else if constexpr (std::same_as<T, sanitize::SchemaEvolutionMode>) {
    return "schema_evolution";
  } else if constexpr (std::same_as<T, sanitize::FieldOrderPolicy>) {
    return "field_order";
  } else if constexpr (std::same_as<T, sanitize::OnErrorPolicy>) {
    return "on_error";
  } else if constexpr (std::same_as<T, sanitize::ThreadingMode>) {
    return "threading_mode";
  } else {
    static_assert(!sizeof(T), "unsupported option catalog type");
  }
}

/// Creates a Python Unicode value from a borrowed UTF-8 view.
PyObject *unicode_from_view(std::string_view value) {
  return PyUnicode_FromStringAndSize(value.data(),
                                     static_cast<Py_ssize_t>(value.size()));
}

/// Converts an option descriptor default into the matching Python object.
template <class T> PyObject *default_value(const T &value) {
  if constexpr (std::same_as<T, bool>) {
    return PyBool_FromLong(value ? 1 : 0);
  } else if constexpr (std::integral<T>) {
    return PyLong_FromLongLong(static_cast<long long>(value));
  } else if constexpr (std::same_as<T, std::string>) {
    return unicode_from_view(value);
  } else if constexpr (std::same_as<T, StringListOption>) {
    PyObject *items = PyTuple_New(static_cast<Py_ssize_t>(value.size()));
    if (!items) {
      return nullptr;
    }
    for (std::size_t index = 0; index < value.size(); ++index) {
      if (!tuple_set_item_steal(items, static_cast<Py_ssize_t>(index),
                                unicode_from_view(value[index]))) {
        Py_DECREF(items);
        return nullptr;
      }
    }
    return items;
  } else if constexpr (std::same_as<T, LogicalSchemaOption>) {
    Py_INCREF(Py_None);
    return Py_None;
  } else if constexpr (std::is_enum_v<T>) {
    return PyLong_FromLongLong(
        static_cast<long long>(std::to_underlying(value)));
  } else {
    static_assert(!sizeof(T), "unsupported option default type");
  }
}

/// Appends one `(name, kind, default, group)` descriptor to the Python catalog.
template <class T>
bool append_descriptor(PyObject *catalog, Py_ssize_t index,
                       std::string_view name, std::string_view group,
                       const T &value) {
  PyObject *descriptor = PyTuple_New(4);
  if (!descriptor) {
    return false;
  }
  if (!tuple_set_item_steal(descriptor, 0, unicode_from_view(name)) ||
      !tuple_set_item_steal(descriptor, 1,
                            unicode_from_view(option_kind<T>())) ||
      !tuple_set_item_steal(descriptor, 2, default_value(value)) ||
      !tuple_set_item_steal(descriptor, 3, unicode_from_view(group))) {
    Py_DECREF(descriptor);
    return false;
  }
  return tuple_set_item_steal(catalog, index, descriptor) != 0;
}

constexpr Py_ssize_t kOptionCount = 0
#define SCHEMA_SANITIZER_OPTION(type, name, default_expr, group, doc) +1
#define SCHEMA_SANITIZER_OPTION_DEFAULT(type, name, group, doc) +1
#include "sanitize/options/options_catalog.def"
#undef SCHEMA_SANITIZER_OPTION_DEFAULT
#undef SCHEMA_SANITIZER_OPTION
    ;

} // namespace

/// Returns the native memory-budget fields derived from a requested byte limit.
PyObject *py_memory_budget(PyObject *, PyObject *args) {
  long long requested = -1;
  if (!PyArg_ParseTuple(args, "L", &requested)) {
    return nullptr;
  }
  const auto budget = sanitize::internal::memory_budget_from_limit(
      static_cast<std::int64_t>(requested));
  PyObject *out = PyTuple_New(19);
  if (!out) {
    return nullptr;
  }
  const auto set_i64 = [out](Py_ssize_t index, std::int64_t value) {
    return tuple_set_item_steal(out, index, PyLong_FromLongLong(value)) != 0;
  };
  if (!set_i64(0, budget.total_bytes) || !set_i64(1, budget.io_chunk_bytes) ||
      !set_i64(2, budget.batch_target_bytes) ||
      !set_i64(3, budget.coalesce_max_bytes) ||
      !set_i64(4, budget.metadata_bytes) ||
      !set_i64(5, budget.materialized_input_bytes) ||
      !set_i64(6, budget.replay_spool_bytes) ||
      !set_i64(7, budget.parquet_reader_buffer_bytes) ||
      !set_i64(8, budget.parquet_reader_rows) ||
      !set_i64(9, budget.parquet_row_group_bytes) ||
      !set_i64(10, budget.parquet_row_group_rows) ||
      !set_i64(11, budget.parquet_page_bytes) ||
      !set_i64(12, budget.parquet_footer_bytes) ||
      !set_i64(13, budget.async_concurrency) ||
      !set_i64(14, budget.async_prefetch_files) ||
      !set_i64(15, budget.async_retries) ||
      !tuple_set_item_steal(out, 16,
                            PyFloat_FromDouble(budget.async_timeout_seconds)) ||
      !set_i64(17, budget.remote_chunk_prefetch) ||
      !set_i64(18, budget.source_discovery_concurrency)) {
    Py_DECREF(out);
    return nullptr;
  }
  return out;
}

/// Returns process memory-governor capacity, leased bytes, and waiter count.
PyObject *py_process_memory_governor_stats(PyObject *, PyObject *) {
  const auto stats = sanitize::internal::process_memory_governor_stats();
  PyObject *out = PyTuple_New(3);
  if (!out ||
      !tuple_set_item_steal(out, 0,
                            PyLong_FromLongLong(stats.capacity_bytes)) ||
      !tuple_set_item_steal(out, 1, PyLong_FromLongLong(stats.leased_bytes)) ||
      !tuple_set_item_steal(out, 2,
                            PyLong_FromLongLong(stats.waiting_operations))) {
    Py_XDECREF(out);
    return nullptr;
  }
  return out;
}

/// Returns process resident-memory capacity, reservation, and peak usage.
PyObject *py_process_resident_memory_stats(PyObject *, PyObject *) {
  const auto stats = sanitize::internal::process_resident_memory_stats();
  PyObject *out = PyTuple_New(3);
  if (!out ||
      !tuple_set_item_steal(out, 0,
                            PyLong_FromLongLong(stats.capacity_bytes)) ||
      !tuple_set_item_steal(out, 1,
                            PyLong_FromLongLong(stats.reserved_bytes)) ||
      !tuple_set_item_steal(out, 2,
                            PyLong_FromLongLong(stats.peak_reserved_bytes))) {
    Py_XDECREF(out);
    return nullptr;
  }
  return out;
}

/// Returns allocation-registry capacity, occupancy, and collision diagnostics.
PyObject *py_allocation_registry_stats(PyObject *, PyObject *) {
  const auto stats = sanitize::internal::allocation_registry_stats();
  PyObject *out = PyTuple_New(8);
  if (!out ||
      !tuple_set_item_steal(out, 0,
                            PyLong_FromLongLong(stats.metadata_bytes)) ||
      !tuple_set_item_steal(out, 1,
                            PyLong_FromLongLong(stats.peak_metadata_bytes)) ||
      !tuple_set_item_steal(out, 2,
                            PyLong_FromLongLong(stats.capacity_records)) ||
      !tuple_set_item_steal(out, 3, PyLong_FromLongLong(stats.live_entries)) ||
      !tuple_set_item_steal(
          out, 4, PyLong_FromUnsignedLongLong(stats.rejected_registrations)) ||
      !tuple_set_item_steal(
          out, 5, PyLong_FromUnsignedLongLong(stats.secondary_probes)) ||
      !tuple_set_item_steal(
          out, 6, PyLong_FromUnsignedLongLong(stats.collision_rejections)) ||
      !tuple_set_item_steal(out, 7,
                            PyLong_FromLongLong(stats.max_shard_occupancy))) {
    Py_XDECREF(out);
    return nullptr;
  }
  return out;
}

/// Resolves threading and memory inputs into the effective native execution
/// policy.
PyObject *py_execution_policy(PyObject *, PyObject *args) {
  int mode_value = 0;
  long long requested = -1;
  long long available_cpus = -1;
  if (!PyArg_ParseTuple(args, "iL|L", &mode_value, &requested,
                        &available_cpus)) {
    return nullptr;
  }
  if (mode_value < 0 || mode_value > 1) {
    PyErr_SetString(PyExc_ValueError,
                    "threading mode must be 0 (single) or 1 (multi)");
    return nullptr;
  }
  const auto mode = static_cast<sanitize::ThreadingMode>(mode_value);
  const auto policy = available_cpus > 0
                          ? sanitize::internal::execution_policy_from(
                                mode, static_cast<std::int64_t>(requested),
                                static_cast<std::int64_t>(available_cpus))
                          : sanitize::internal::execution_policy_from(
                                mode, static_cast<std::int64_t>(requested));
  PyObject *out = PyTuple_New(14);
  if (!out) {
    return nullptr;
  }
  const auto set_i64 = [out](Py_ssize_t index, std::int64_t value) {
    return tuple_set_item_steal(out, index, PyLong_FromLongLong(value)) != 0;
  };
  if (!set_i64(0, std::to_underlying(policy.requested_mode)) ||
      !set_i64(1, policy.available_cpus) ||
      !set_i64(2, policy.effective_workers) ||
      !set_i64(3, policy.task_queue_capacity) ||
      !set_i64(4, policy.reorder_capacity) ||
      !set_i64(5, policy.worker_arena_bytes) ||
      !set_i64(6, policy.materialization_packet_target_bytes) ||
      !set_i64(7, policy.materialization_packet_max_rows) ||
      !set_i64(8, policy.async_concurrency) ||
      !set_i64(9, policy.async_prefetch_files) ||
      !set_i64(10, policy.remote_chunk_prefetch) ||
      !set_i64(11, policy.source_discovery_concurrency) ||
      !tuple_set_item_steal(
          out, 12, PyBool_FromLong(policy.pyarrow_use_threads ? 1 : 0)) ||
      !set_i64(13, std::to_underlying(policy.fallback_reason))) {
    Py_DECREF(out);
    return nullptr;
  }
  return out;
}

/// Returns every native option descriptor together with its group and default
/// value.
PyObject *py_options_catalog(PyObject *, PyObject *) {
  sanitize::Options defaults;
  PyObject *catalog = PyTuple_New(kOptionCount);
  if (!catalog) {
    return nullptr;
  }
  Py_ssize_t index = 0;
#define SCHEMA_SANITIZER_OPTION(type, name, default_expr, group, doc)          \
  if (!append_descriptor(catalog, index++, #name, group, defaults.name)) {     \
    Py_DECREF(catalog);                                                        \
    return nullptr;                                                            \
  }
#define SCHEMA_SANITIZER_OPTION_DEFAULT(type, name, group, doc)                \
  if (!append_descriptor(catalog, index++, #name, group, defaults.name)) {     \
    Py_DECREF(catalog);                                                        \
    return nullptr;                                                            \
  }
#include "sanitize/options/options_catalog.def"
#undef SCHEMA_SANITIZER_OPTION_DEFAULT
#undef SCHEMA_SANITIZER_OPTION
  return catalog;
}

} // namespace core_abi3_internal

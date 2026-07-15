// Exposes catalog-backed option metadata from the C++ source of truth.

#include "internal/abi/python_abi3/methods.hh"

#include "internal/memory/memory_budget.hh"
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
  } else {
    static_assert(!sizeof(T), "unsupported option catalog type");
  }
}

PyObject *unicode_from_view(std::string_view value) {
  return PyUnicode_FromStringAndSize(value.data(),
                                     static_cast<Py_ssize_t>(value.size()));
}

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

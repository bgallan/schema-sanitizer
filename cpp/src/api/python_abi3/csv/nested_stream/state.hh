/*
 * Shared CSV nested-column stream helpers.
 *
 * These declarations are used by the ABI3 CSV stream wrapper translation units
 * that rewrite nested top-level Arrow columns to JSON UTF-8 columns.
 */
#pragma once

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"
#include "internal/json_output/schema/model.hh"

#include "nanoarrow/nanoarrow.h"
#include "sanitize/abi/cdata_types.hh"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace core_abi3_internal {

namespace csv_nested_stream {

namespace jsonl = sanitize::internal::jsonl_stream_writer;

struct CsvNestedSchemaChild {
  ArrowSchema schema{};
  std::string name;
};

struct CsvNestedColumnPlan {
  std::optional<std::size_t> nested_slot;
  jsonl::JsonlField field;
};

struct CsvNestedSchemaState {
  sanitize::CSchemaGuard base;
  std::vector<CsvNestedSchemaChild> nested_fields;
  std::vector<ArrowSchema *> children;
};

struct CsvNestedUtf8Array {
  std::vector<std::uint8_t> validity;
  std::vector<std::int32_t> offsets;
  std::string data;
  const void *buffers[3]{nullptr, nullptr, nullptr};
  ArrowArray array{};
};

struct CsvNestedArrayState {
  sanitize::CArrayGuard base;
  std::vector<CsvNestedUtf8Array> nested_arrays;
  std::vector<ArrowArray *> children;
  const void *struct_buffers[1]{nullptr};
};

struct CsvNestedStreamState {
  ArrowArrayStream *inner = nullptr;
  PyObject *stream_obj = nullptr;
  PyObject *stream_capsule = nullptr;
  std::vector<CsvNestedColumnPlan> columns;
  std::size_t nested_column_count = 0;
  jsonl::ArrayValidationLimits validation_limits{};
  std::string last_error;
  bool schema_loaded = false;
  bool closed = false;
};

void close_stream(CsvNestedStreamState *state) noexcept;

const char *last_error(ArrowArrayStream *stream);

void release_stream(ArrowArrayStream *stream);

int get_schema(ArrowArrayStream *stream, ArrowSchema *out);

int get_next(ArrowArrayStream *stream, ArrowArray *out);

} // namespace csv_nested_stream

} // namespace core_abi3_internal

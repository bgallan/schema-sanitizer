/*
 * Shared CSV nested-column stream helpers.
 *
 * These declarations are used by the ABI3 CSV stream wrapper translation units
 * that rewrite nested top-level Arrow columns to JSON UTF-8 columns.
 */
#pragma once

#include "internal/abi/core_abi3_internal.hh"
#include "internal/json/jsonl_stream_writer_schema.hh"

#include "nanoarrow/nanoarrow.h"
#include "sanitize/abi/cdata_types.hh"

#include <cstdint>
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
  bool nested = false;
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
  std::string last_error;
  bool schema_loaded = false;
  bool closed = false;
};

void clear_schema(ArrowSchema *schema) noexcept;

void clear_array(ArrowArray *array) noexcept;

void csv_nested_schema_child_release(ArrowSchema *schema);

void csv_nested_array_child_release(ArrowArray *array);

void csv_nested_schema_release(ArrowSchema *schema);

void csv_nested_array_release(ArrowArray *array);

sanitize::Status load_csv_nested_schema(CsvNestedStreamState *stream_state,
                                        ArrowSchema *base_schema);

sanitize::Status append_schema_children(CsvNestedStreamState *stream_state,
                                        CsvNestedSchemaState *schema_state);

sanitize::Status build_nested_utf8_array(CsvNestedUtf8Array *out,
                                         const jsonl::JsonlField &field,
                                         const ArrowArray &array,
                                         std::int64_t length);

} // namespace csv_nested_stream

} // namespace core_abi3_internal

/*
 * Common CSV nested-column stream lifecycle helpers.
 *
 * Owns Arrow C Data release callbacks for wrapper schema and array states.
 */
#include "api/python_abi3/_core_abi3_csv_nested_stream_parts.hh"

#include "internal/pipeline/cdata_stream_utils.hh"

namespace core_abi3_internal::csv_nested_stream {

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

} // namespace core_abi3_internal::csv_nested_stream

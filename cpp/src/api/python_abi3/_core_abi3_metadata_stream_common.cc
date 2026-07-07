/*
 * Arrow metadata stream shared release helpers.
 *
 * This file owns Arrow C Data cleanup helpers used by the metadata stream
 * schema and array builder translation units.
 */
#include "api/python_abi3/_core_abi3_metadata_stream_builder_parts.hh"

#include "internal/pipeline/cdata_stream_utils.hh"

namespace core_abi3_internal {

void clear_schema(ArrowSchema *schema) noexcept {
  sanitize::internal::cdata_stream::clear_schema(schema);
}

void clear_array(ArrowArray *array) noexcept {
  sanitize::internal::cdata_stream::clear_array(array);
}

void metadata_schema_child_release(ArrowSchema *schema) {
  if (!schema || !schema->release) {
    return;
  }
  clear_schema(schema);
}

void metadata_array_child_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  clear_array(array);
}

void metadata_schema_release(ArrowSchema *schema) {
  if (!schema || !schema->release) {
    return;
  }
  auto *state = static_cast<MetadataSchemaState *>(schema->private_data);
  delete state;
  clear_schema(schema);
}

void metadata_array_release(ArrowArray *array) {
  if (!array || !array->release) {
    return;
  }
  auto *state = static_cast<MetadataArrayState *>(array->private_data);
  delete state;
  clear_array(array);
}

} // namespace core_abi3_internal

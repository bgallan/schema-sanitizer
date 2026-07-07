// Declares shared state and helpers for metadata stream builder files.

#pragma once

#include "api/python_abi3/_core_abi3_metadata_columns.hh"

#include "nanoarrow/nanoarrow.h"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/status.hh"

#include <cstdint>
#include <string>
#include <vector>

namespace core_abi3_internal {

struct MetadataSchemaChild {
  ArrowSchema schema{};
  std::string name;
  std::string format;
};

struct MetadataSchemaState {
  sanitize::CSchemaGuard base;
  std::vector<MetadataSchemaChild> metadata;
  std::vector<ArrowSchema *> children;
};

struct Utf8ColumnData {
  std::vector<std::uint8_t> validity;
  std::vector<std::int32_t> offsets;
  std::vector<char> data;
  std::int64_t null_count = 0;
  const void *buffers[3]{nullptr, nullptr, nullptr};
  ArrowArray array{};
};

struct TimestampMicrosColumnData {
  std::vector<std::int64_t> values;
  const void *buffers[2]{nullptr, nullptr};
  ArrowArray array{};
};

struct MetadataColumnData {
  Utf8ColumnData utf8;
  TimestampMicrosColumnData timestamp;
  ArrowArray *array = nullptr;
};

struct MetadataArrayState {
  sanitize::CArrayGuard base;
  std::vector<MetadataColumnData> metadata;
  std::vector<ArrowArray *> children;
  const void *struct_buffers[1]{nullptr};
};

// Clears an Arrow C schema without releasing child ownership.
void clear_schema(ArrowSchema *schema) noexcept;
// Clears an Arrow C array without releasing child ownership.
void clear_array(ArrowArray *array) noexcept;
// Releases a metadata schema child.
void metadata_schema_child_release(ArrowSchema *schema);
// Releases a metadata array child.
void metadata_array_child_release(ArrowArray *array);
// Releases a full metadata wrapper schema.
void metadata_schema_release(ArrowSchema *schema);
// Releases a full metadata wrapper array.
void metadata_array_release(ArrowArray *array);
// Appends metadata children to a schema state after the base schema is loaded.
sanitize::Status append_metadata_schema_children(MetadataSchemaState *state);
// Builds one metadata column array for a batch.
sanitize::Status build_metadata_column_array(MetadataColumnData *out,
                                             MetadataColumn *column,
                                             std::int64_t length,
                                             bool first_row_pending);
} // namespace core_abi3_internal

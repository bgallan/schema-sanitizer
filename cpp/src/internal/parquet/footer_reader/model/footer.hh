// Row-group and footer metadata exposed by the bounded Parquet reader.

#pragma once

#include "internal/parquet/footer_reader/model/column.hh"
#include "internal/parquet/footer_reader/model/schema.hh"

#include <cstdint>
#include <string>
#include <vector>

namespace sanitize::internal::parquet_footer_reader {

struct RowGroupInfo {
  bool has_total_byte_size = false;
  std::int64_t total_byte_size = 0;
  bool has_num_rows = false;
  std::int64_t num_rows = 0;
  std::vector<ColumnChunkInfo> columns;
};

struct FooterInfo {
  std::int32_t version = 0;
  std::int64_t num_rows = 0;
  std::int32_t schema_element_count = 0;
  std::int32_t row_group_count = 0;
  std::string created_by;
  std::vector<SchemaElementInfo> schema_elements;
  std::vector<RowGroupInfo> row_groups;
  std::vector<std::string> projected_columns;
};

} // namespace sanitize::internal::parquet_footer_reader

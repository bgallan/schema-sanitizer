// Prepares independent small Parquet row groups while the stream produces more
// Arrow batches. The coordinator remains the sole owner of physical file order.

struct ParquetRowGroupTask {
  std::shared_ptr<sanitize::CArrayGuard> owner;
  std::int64_t row_offset = 0;
  std::int64_t row_count = 0;
  std::size_t estimated_retained_bytes = 0;
};

struct PreparedParquetRowGroup {
  std::string bytes;
  RowGroupInfo info;
  std::size_t estimated_retained_bytes = 0;
};

sanitize::Result<PreparedParquetRowGroup> prepare_serial_row_group(
    const ParquetNode &root, const std::vector<LeafColumn> &columns,
    ParquetRowGroupTask task, CompressionCodec compression_codec,
    int gzip_level, std::int64_t target_page_bytes,
    const WriterOptions &options,
    const sanitize::internal::MemoryBudget &budget) {
  if (!task.owner || !task.owner->value().release || task.row_count <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid asynchronous row-group task");
  }
  PreparedParquetRowGroup prepared;
  prepared.info.num_rows = task.row_count;
  prepared.info.columns.reserve(columns.size());
  prepared.estimated_retained_bytes = task.estimated_retained_bytes;
  StringParquetOutput memory_output(&prepared.bytes);
  CountingOutput counted(memory_output);
  auto serial_options = options;
  serial_options.threading_mode = sanitize::ThreadingMode::kSingle;
  SAN_RETURN_NOT_OK(write_row_group_columns(
      counted, root, columns, task.owner->value(), task.row_offset,
      task.row_count, compression_codec, gzip_level, target_page_bytes,
      serial_options, budget, nullptr, &prepared.info));
  // Page-index payloads contain physical page offsets. Defer their encoding
  // until ordered commit, after the row group's absolute file base is known.
  return prepared;
}

void add_row_group_base_offset(RowGroupInfo *row_group,
                               std::int64_t base) noexcept {
  if (!row_group) {
    return;
  }
  for (auto &column : row_group->columns) {
    add_page_base_offset(&column.aggregate, base);
    for (auto &page : column.pages) {
      add_page_base_offset(&page, base);
    }
    if (column.column_index_offset >= 0) {
      column.column_index_offset += base;
    }
    if (column.offset_index_offset >= 0) {
      column.offset_index_offset += base;
    }
  }
}

sanitize::Status
commit_prepared_row_group(CountingOutput &out,
                          const std::vector<LeafColumn> &columns,
                          PreparedParquetRowGroup *prepared) {
  if (!prepared) {
    return sanitize::Status::Invalid(
        "native Parquet writer: null prepared row group");
  }
  add_row_group_base_offset(&prepared->info, out.offset());
  SAN_RETURN_NOT_OK(out.Write(prepared->bytes));
  std::string().swap(prepared->bytes);
  // Encode indexes directly into the final stream so every page location is
  // absolute and the footer/index consistency oracle remains exact.
  return write_row_group_page_indexes(out, columns, &prepared->info);
}

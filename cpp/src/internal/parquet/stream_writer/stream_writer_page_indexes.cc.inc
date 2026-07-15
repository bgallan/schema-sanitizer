std::string encode_column_index(const std::vector<PageInfo> &pages) {
  std::string out;
  CompactWriter writer(out);
  writer.FieldListBool(1, pages.size(), [&](std::size_t index) {
    const auto &page = pages[index];
    return page.num_values > 0 && page.null_count == page.num_values;
  });
  writer.FieldListString(
      2, pages.size(), [&](std::size_t index) -> std::string_view {
        const auto &value = pages[index].min_value;
        return value ? std::string_view(*value) : std::string_view{};
      });
  writer.FieldListString(
      3, pages.size(), [&](std::size_t index) -> std::string_view {
        const auto &value = pages[index].max_value;
        return value ? std::string_view(*value) : std::string_view{};
      });
  writer.FieldI32(4, 0); // UNORDERED
  writer.FieldListI64(5, pages.size(), [&](std::size_t index) {
    return pages[index].null_count;
  });
  writer.Stop();
  return out;
}

bool page_is_null_page(const PageInfo &page) {
  return page.num_values > 0 && page.null_count == page.num_values;
}

bool can_encode_column_index(const std::vector<PageInfo> &pages) {
  for (const auto &page : pages) {
    if (page_is_null_page(page)) {
      continue;
    }
    if (!page.min_value || !page.max_value) {
      return false;
    }
  }
  return true;
}

std::string encode_offset_index(const std::vector<PageInfo> &pages) {
  std::string out;
  CompactWriter writer(out);
  writer.FieldListStruct(1, pages.size(), [&](CompactWriter &) {
    std::int64_t first_row_index = 0;
    for (const auto &page : pages) {
      CompactWriter location(out);
      location.FieldI64(1, page.offset);
      location.FieldI32(2,
                        static_cast<std::int32_t>(
                            page.header_size + page.compressed_payload_size));
      location.FieldI64(3, first_row_index);
      location.Stop();
      first_row_index += page.num_values;
    }
  });
  writer.Stop();
  return out;
}

sanitize::Status
write_row_group_page_indexes(CountingOutput &out,
                             const std::vector<LeafColumn> &columns,
                             RowGroupInfo *row_group) {
  if (!row_group) {
    return sanitize::Status::Invalid("native Parquet writer: null row group");
  }
  for (std::size_t i = 0; i < row_group->columns.size(); ++i) {
    auto &chunk = row_group->columns[i];
    if (i >= columns.size() || columns[i].max_repetition_level != 0 ||
        chunk.pages.empty()) {
      // Repeated columns do not currently emit page indexes. Their per-page
      // staging metadata is no longer needed after the chunk is complete.
      std::vector<PageInfo>().swap(chunk.pages);
      continue;
    }
    if (can_encode_column_index(chunk.pages)) {
      std::string column_index = encode_column_index(chunk.pages);
      if (column_index.size() >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
        return sanitize::Status::Invalid(
            "native Parquet writer: column index is too large");
      }
      chunk.column_index_offset = out.offset();
      chunk.column_index_length =
          static_cast<std::int32_t>(column_index.size());
      SAN_RETURN_NOT_OK(out.Write(column_index));
    }

    std::string offset_index = encode_offset_index(chunk.pages);
    if (offset_index.size() >
        static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
      return sanitize::Status::Invalid(
          "native Parquet writer: offset index is too large");
    }
    chunk.offset_index_offset = out.offset();
    chunk.offset_index_length = static_cast<std::int32_t>(offset_index.size());
    SAN_RETURN_NOT_OK(out.Write(offset_index));

    // Footer metadata only needs aggregate chunk statistics and index offsets.
    // Drop page-level min/max strings and locations immediately so writer
    // memory is bounded by the active row group rather than the full file.
    std::vector<PageInfo>().swap(chunk.pages);
  }
  return sanitize::Status::OK();
}

std::string encode_column_index(const std::vector<PageInfo> &pages) {
  std::vector<bool> null_pages;
  std::vector<std::string> min_values;
  std::vector<std::string> max_values;
  std::vector<std::int64_t> null_counts;
  null_pages.reserve(pages.size());
  min_values.reserve(pages.size());
  max_values.reserve(pages.size());
  null_counts.reserve(pages.size());
  for (const auto &page : pages) {
    const bool null_page =
        page.num_values > 0 && page.null_count == page.num_values;
    null_pages.push_back(null_page);
    min_values.push_back(page.min_value.value_or(std::string{}));
    max_values.push_back(page.max_value.value_or(std::string{}));
    null_counts.push_back(page.null_count);
  }

  std::string out;
  CompactWriter writer(out);
  writer.FieldListBool(1, null_pages);
  writer.FieldListString(2, min_values);
  writer.FieldListString(3, max_values);
  writer.FieldI32(4, 0); // UNORDERED
  writer.FieldListI64(5, null_counts);
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

sanitize::Status write_page_indexes(CountingOutput &out,
                                    const std::vector<LeafColumn> &columns,
                                    std::vector<RowGroupInfo> *row_groups) {
  if (!row_groups) {
    return sanitize::Status::Invalid("native Parquet writer: null row groups");
  }
  for (auto &row_group : *row_groups) {
    for (std::size_t i = 0; i < row_group.columns.size(); ++i) {
      auto &chunk = row_group.columns[i];
      if (i >= columns.size() || columns[i].max_repetition_level != 0 ||
          chunk.pages.empty()) {
        continue;
      }
      if (can_encode_column_index(chunk.pages)) {
        std::string column_index = encode_column_index(chunk.pages);
        if (column_index.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::int32_t>::max())) {
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
      chunk.offset_index_length =
          static_cast<std::int32_t>(offset_index.size());
      SAN_RETURN_NOT_OK(out.Write(offset_index));
    }
  }
  return sanitize::Status::OK();
}

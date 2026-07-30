// Selects, writes, estimates, and slices native Parquet pages.

sanitize::Result<PreparedPagePayload>
choose_page_payload(const LeafColumn &column, const ColumnPageData &page_data,
                    CompressionCodec codec, int gzip_level,
                    bool allow_dictionary) {
  std::optional<PreparedPagePayload> best;
  std::size_t best_size = std::numeric_limits<std::size_t>::max();

  const auto consider =
      [&](std::int32_t encoding, std::string_view encoded_values,
          const DictionaryEncodingData *dictionary) -> sanitize::Status {
    // Candidates are built, compressed, compared, and released one at a time.
    // Keeping only the winner avoids retaining PLAIN plus every alternate
    // encoding and their compressed forms simultaneously.
    std::string payload =
        encode_page_levels_and_values(column, page_data, encoded_values);
    SAN_ASSIGN_OR_RAISE(auto compressed_payload,
                        compress_payload(payload, codec, gzip_level));
    std::string compressed_dictionary;
    std::size_t candidate_size = compressed_payload.size();
    std::optional<PreparedDictionaryInfo> dictionary_info;
    if (dictionary) {
      SAN_ASSIGN_OR_RAISE(
          compressed_dictionary,
          compress_payload(dictionary->dictionary_values, codec, gzip_level));
      candidate_size =
          saturating_size_add(candidate_size, compressed_dictionary.size());
      dictionary_info = PreparedDictionaryInfo{
          .value_count = dictionary->dictionary_size,
          .uncompressed_payload_size = dictionary->dictionary_values.size(),
      };
    }
    const bool encoding_tie_break =
        candidate_size == best_size &&
        ((dictionary && prefers_dictionary_tie_break(column)) ||
         encoding == kEncodingByteStreamSplit);
    if (!best || candidate_size < best_size || encoding_tie_break) {
      best_size = candidate_size;
      best = PreparedPagePayload{
          .encoding = encoding,
          .dictionary = dictionary_info,
          .uncompressed_payload_size = payload.size(),
          .compressed_payload = std::move(compressed_payload),
          .compressed_dictionary = std::move(compressed_dictionary),
      };
    }
    return {};
  };

  // PLAIN is already present in page_data, so evaluate it through a view
  // instead of making the previous unconditional full-size copy.
  SAN_RETURN_NOT_OK(consider(kEncodingPlain, page_data.values, nullptr));

  if (allow_dictionary) {
    if (auto dictionary =
            try_dictionary_encode_column(column, page_data.values)) {
      const std::string_view indices(dictionary->encoded_indices);
      SAN_RETURN_NOT_OK(
          consider(kEncodingRleDictionary, indices, &*dictionary));
    }
  }
  if (is_float_column(column)) {
    if (auto encoded = encode_byte_stream_split(page_data.values,
                                                float_value_width(column))) {
      SAN_RETURN_NOT_OK(consider(kEncodingByteStreamSplit, *encoded, nullptr));
    }
  }
  if (auto encoded =
          encode_delta_binary_packed_column(column, page_data.values)) {
    SAN_RETURN_NOT_OK(consider(kEncodingDeltaBinaryPacked, *encoded, nullptr));
  }
  if (is_byte_array_kind(leaf_value_kind(column))) {
    if (auto encoded = encode_delta_length_byte_array(page_data.values)) {
      SAN_RETURN_NOT_OK(
          consider(kEncodingDeltaLengthByteArray, *encoded, nullptr));
    }
  }
  if (!best) {
    return sanitize::Status::Invalid(
        "native Parquet writer: no encodable Parquet page candidate");
  }
  return std::move(*best);
}

// Writes one column data page and returns its file offsets and sizes.
sanitize::Result<PageInfo>
write_column_page(CountingOutput &out, const LeafColumn &column,
                  const ColumnPageData &page_data, CompressionCodec codec,
                  int gzip_level, bool allow_dictionary) {
  SAN_ASSIGN_OR_RAISE(auto prepared,
                      choose_page_payload(column, page_data, codec, gzip_level,
                                          allow_dictionary));
  if (prepared.uncompressed_payload_size >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()) ||
      page_data.definition_levels.size() >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: page is too large");
  }
  if (prepared.compressed_payload.size() >
      static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: compressed page is too large");
  }
  std::string header = encode_data_page_header(
      static_cast<std::int32_t>(page_data.definition_levels.size()),
      static_cast<std::int32_t>(prepared.uncompressed_payload_size),
      static_cast<std::int32_t>(prepared.compressed_payload.size()),
      prepared.encoding);
  PageInfo info;
  info.value_encoding = prepared.encoding;
  info.has_plain_encoding = prepared.encoding == kEncodingPlain;
  info.has_delta_binary_packed_encoding =
      prepared.encoding == kEncodingDeltaBinaryPacked;
  info.has_delta_length_byte_array_encoding =
      prepared.encoding == kEncodingDeltaLengthByteArray;
  info.has_byte_stream_split_encoding =
      prepared.encoding == kEncodingByteStreamSplit;
  if (prepared.dictionary) {
    if (prepared.dictionary->uncompressed_payload_size >
        static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
      return sanitize::Status::Invalid(
          "native Parquet writer: dictionary page is too large");
    }
    if (prepared.compressed_dictionary.size() >
        static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
      return sanitize::Status::Invalid(
          "native Parquet writer: compressed dictionary page is too large");
    }
    std::string dictionary_header = encode_dictionary_page_header(
        prepared.dictionary->value_count,
        static_cast<std::int32_t>(
            prepared.dictionary->uncompressed_payload_size),
        static_cast<std::int32_t>(prepared.compressed_dictionary.size()));
    info.dictionary_encoded = true;
    info.dictionary_page_offset = out.offset();
    info.first_page_offset = info.dictionary_page_offset;
    info.dictionary_header_size =
        static_cast<std::int64_t>(dictionary_header.size());
    info.dictionary_uncompressed_payload_size = static_cast<std::int64_t>(
        prepared.dictionary->uncompressed_payload_size);
    info.dictionary_compressed_payload_size =
        static_cast<std::int64_t>(prepared.compressed_dictionary.size());
    SAN_RETURN_NOT_OK(out.Write(dictionary_header));
    SAN_RETURN_NOT_OK(out.Write(prepared.compressed_dictionary));
  }

  info.offset = out.offset();
  if (!prepared.dictionary) {
    info.first_page_offset = info.offset;
  }
  info.header_size = static_cast<std::int64_t>(header.size());
  info.uncompressed_payload_size =
      static_cast<std::int64_t>(prepared.uncompressed_payload_size);
  info.compressed_payload_size =
      static_cast<std::int64_t>(prepared.compressed_payload.size());
  info.num_values =
      static_cast<std::int64_t>(page_data.definition_levels.size());
  info.null_count = static_cast<std::int64_t>(std::count_if(
      page_data.definition_levels.begin(), page_data.definition_levels.end(),
      [&](std::int16_t level) { return level < column.max_definition_level; }));
  if (auto bounds = encoded_min_max_for_column(column, page_data)) {
    info.min_value = std::move(bounds->first);
    info.max_value = std::move(bounds->second);
  }
  SAN_RETURN_NOT_OK(out.Write(header));
  SAN_RETURN_NOT_OK(out.Write(prepared.compressed_payload));
  return info;
}

std::size_t fixed_value_width_for_leaf(const LeafColumn &column) {
  if (leaf_value_kind(column) == jsonl::JsonlKind::kBool) {
    return 0;
  }
  return fixed_dictionary_value_width(*column.node, leaf_value_kind(column))
      .value_or(0);
}

std::size_t estimate_column_page_data_bytes(const ColumnPageData &page_data) {
  auto total = saturating_size_add(page_data.values.size(),
                                   page_data.bool_values.size());
  total = saturating_size_add(
      total, saturating_size_multiply(page_data.definition_levels.size(),
                                      sizeof(std::int16_t)));
  return saturating_size_add(
      total, saturating_size_multiply(page_data.repetition_levels.size(),
                                      sizeof(std::int16_t)));
}

std::size_t byte_array_item_end(std::string_view values,
                                std::size_t offset) noexcept {
  if (offset > values.size() ||
      values.size() - offset < sizeof(std::uint32_t)) {
    return offset;
  }
  const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
  const auto payload_offset = offset + sizeof(std::uint32_t);
  if (values.size() - payload_offset < size) {
    return offset;
  }
  return payload_offset + size;
}

struct PageSliceIndex {
  jsonl::JsonlKind kind = jsonl::JsonlKind::kNull;
  std::size_t fixed_width = 0;
};

PageSliceIndex build_page_slice_index(const LeafColumn &column) {
  return PageSliceIndex{.kind = leaf_value_kind(column),
                        .fixed_width = fixed_value_width_for_leaf(column)};
}

ColumnPageData slice_column_page_data(
    const LeafColumn &column, const ColumnPageData &page_data,
    const PageSliceIndex &index, std::size_t begin, std::size_t end,
    std::size_t value_begin, std::size_t value_end, std::size_t byte_begin,
    std::size_t byte_end) {
  ColumnPageData out;
  out.definition_levels.insert(out.definition_levels.end(),
                               page_data.definition_levels.begin() + begin,
                               page_data.definition_levels.begin() + end);
  if (column.max_repetition_level > 0) {
    out.repetition_levels.insert(out.repetition_levels.end(),
                                 page_data.repetition_levels.begin() + begin,
                                 page_data.repetition_levels.begin() + end);
  }

  if (index.kind == jsonl::JsonlKind::kBool) {
    const auto available_end =
        std::min(value_end, page_data.bool_values.size());
    if (value_begin < available_end) {
      out.bool_values.insert(out.bool_values.end(),
                             page_data.bool_values.begin() + value_begin,
                             page_data.bool_values.begin() + available_end);
    }
    encode_bool_values_plain(&out.values, out.bool_values);
    return out;
  }

  if (is_byte_array_kind(index.kind)) {
    const auto available_begin = std::min(byte_begin, page_data.values.size());
    const auto available_end = std::min(byte_end, page_data.values.size());
    if (available_begin < available_end) {
      out.values.append(page_data.values, available_begin,
                        available_end - available_begin);
    }
    return out;
  }

  if (index.fixed_width > 0) {
    const auto available_values = page_data.values.size() / index.fixed_width;
    const auto available_end = std::min(value_end, available_values);
    if (value_begin < available_end) {
      const auto start = value_begin * index.fixed_width;
      const auto size = (available_end - value_begin) * index.fixed_width;
      out.values.append(page_data.values, start, size);
    }
  }
  return out;
}

std::size_t page_row_incremental_bytes(const LeafColumn &column,
                                       const ColumnPageData &page_data,
                                       const PageSliceIndex &index,
                                       std::size_t row, std::size_t value_index,
                                       std::size_t value_byte_offset,
                                       std::size_t page_bool_values,
                                       std::size_t *next_value_byte_offset) {
  if (next_value_byte_offset) {
    *next_value_byte_offset = value_byte_offset;
  }
  std::size_t bytes = sizeof(std::int16_t);
  if (column.max_repetition_level > 0) {
    bytes += sizeof(std::int16_t);
  }
  if (page_data.definition_levels[row] != column.max_definition_level) {
    return bytes;
  }

  if (index.kind == jsonl::JsonlKind::kBool) {
    if (value_index >= page_data.bool_values.size()) {
      return bytes;
    }
    return bytes + 1 + (page_bool_values % 8 == 0 ? 1 : 0);
  }
  if (is_byte_array_kind(index.kind)) {
    const auto item_end =
        byte_array_item_end(page_data.values, value_byte_offset);
    if (item_end > value_byte_offset) {
      bytes = saturating_size_add(bytes, item_end - value_byte_offset);
      if (next_value_byte_offset) {
        *next_value_byte_offset = item_end;
      }
    }
    return bytes;
  }
  if (index.fixed_width > 0 &&
      value_index < page_data.values.size() / index.fixed_width) {
    bytes = saturating_size_add(bytes, index.fixed_width);
  }
  return bytes;
}

struct ColumnPageRange {
  std::size_t begin = 0;
  std::size_t end = 0;
  std::size_t value_begin = 0;
  std::size_t value_end = 0;
  std::size_t byte_begin = 0;
  std::size_t byte_end = 0;
};

std::vector<ColumnPageRange> split_column_page_ranges(
    const LeafColumn &column, const ColumnPageData &page_data,
    const PageSliceIndex &index, std::int64_t target_page_bytes) {
  const auto count = page_data.definition_levels.size();
  if (target_page_bytes <= 0 ||
      static_cast<std::int64_t>(estimate_column_page_data_bytes(page_data)) <=
          target_page_bytes ||
      count <= 1) {
    return {{.begin = 0, .end = count, .byte_end = page_data.values.size()}};
  }

  std::vector<ColumnPageRange> ranges;
  std::size_t begin = 0;
  std::size_t page_bytes = 0;
  std::size_t page_bool_values = 0;
  std::size_t value_index = 0;
  std::size_t begin_value_index = 0;
  std::size_t value_byte_offset = 0;
  std::size_t begin_value_byte_offset = 0;
  for (std::size_t row = 0; row < count; ++row) {
    const bool has_value =
        page_data.definition_levels[row] == column.max_definition_level;
    std::size_t next_value_byte_offset = value_byte_offset;
    auto increment = page_row_incremental_bytes(
        column, page_data, index, row, value_index, value_byte_offset,
        page_bool_values, &next_value_byte_offset);
    if (row > begin && saturating_size_add(page_bytes, increment) >
                           static_cast<std::size_t>(target_page_bytes)) {
      ranges.emplace_back(ColumnPageRange{.begin = begin,
                                          .end = row,
                                          .value_begin = begin_value_index,
                                          .value_end = value_index,
                                          .byte_begin = begin_value_byte_offset,
                                          .byte_end = value_byte_offset});
      begin = row;
      begin_value_index = value_index;
      begin_value_byte_offset = value_byte_offset;
      page_bytes = 0;
      page_bool_values = 0;
      increment = page_row_incremental_bytes(
          column, page_data, index, row, value_index, value_byte_offset,
          page_bool_values, &next_value_byte_offset);
    }
    page_bytes = saturating_size_add(page_bytes, increment);
    if (has_value) {
      if (index.kind == jsonl::JsonlKind::kBool &&
          value_index < page_data.bool_values.size()) {
        ++page_bool_values;
      }
      value_byte_offset = next_value_byte_offset;
      ++value_index;
    }
  }
  ranges.emplace_back(ColumnPageRange{.begin = begin,
                                      .end = count,
                                      .value_begin = begin_value_index,
                                      .value_end = value_index,
                                      .byte_begin = begin_value_byte_offset,
                                      .byte_end = value_byte_offset});
  return ranges;
}

void merge_page_info(PageInfo &target, const PageInfo &page) {
  if (target.num_values == 0) {
    target = page;
    return;
  }
  if (target.dictionary_page_offset < 0 && page.dictionary_page_offset >= 0) {
    target.dictionary_page_offset = page.dictionary_page_offset;
  }
  target.dictionary_header_size += page.dictionary_header_size;
  target.dictionary_uncompressed_payload_size +=
      page.dictionary_uncompressed_payload_size;
  target.dictionary_compressed_payload_size +=
      page.dictionary_compressed_payload_size;
  target.header_size += page.header_size;
  target.uncompressed_payload_size += page.uncompressed_payload_size;
  target.compressed_payload_size += page.compressed_payload_size;
  target.num_values += page.num_values;
  target.null_count += page.null_count;
  target.dictionary_encoded =
      target.dictionary_encoded || page.dictionary_encoded;
  target.has_plain_encoding =
      target.has_plain_encoding || page.has_plain_encoding;
  target.has_delta_binary_packed_encoding =
      target.has_delta_binary_packed_encoding ||
      page.has_delta_binary_packed_encoding;
  target.has_delta_length_byte_array_encoding =
      target.has_delta_length_byte_array_encoding ||
      page.has_delta_length_byte_array_encoding;
  target.has_byte_stream_split_encoding =
      target.has_byte_stream_split_encoding ||
      page.has_byte_stream_split_encoding;
  target.min_value.reset();
  target.max_value.reset();
}

// Writes all pages for one column chunk.
sanitize::Result<ColumnChunkInfo>
write_column_pages(CountingOutput &out, const LeafColumn &column,
                   const ColumnPageData &page_data, CompressionCodec codec,
                   int gzip_level, std::int64_t target_page_bytes,
                   bool allow_single_page_dictionary = true) {
  const auto index = build_page_slice_index(column);
  const auto ranges =
      split_column_page_ranges(column, page_data, index, target_page_bytes);
  ColumnChunkInfo chunk;
  chunk.pages.reserve(ranges.size());
  const bool allow_dictionary =
      allow_single_page_dictionary && ranges.size() == 1;
  for (const auto &range : ranges) {
    // The common one-page case writes the original buffers directly. Split
    // columns materialize only the current slice, so memory is bounded by one
    // page instead of all copied page fragments at once.
    const bool whole_page =
        range.begin == 0 && range.end == page_data.definition_levels.size();
    if (whole_page) {
      SAN_ASSIGN_OR_RAISE(auto page,
                          write_column_page(out, column, page_data, codec,
                                            gzip_level, allow_dictionary));
      merge_page_info(chunk.aggregate, page);
      chunk.pages.push_back(std::move(page));
      continue;
    }
    auto page_data_part = slice_column_page_data(
        column, page_data, index, range.begin, range.end, range.value_begin,
        range.value_end, range.byte_begin, range.byte_end);
    SAN_ASSIGN_OR_RAISE(auto page,
                        write_column_page(out, column, page_data_part, codec,
                                          gzip_level, false));
    merge_page_info(chunk.aggregate, page);
    chunk.pages.push_back(std::move(page));
  }
  if (auto bounds = encoded_min_max_for_column(column, page_data)) {
    chunk.aggregate.min_value = std::move(bounds->first);
    chunk.aggregate.max_value = std::move(bounds->second);
  }
  return chunk;
}

// Validates that a record batch has a root struct array with expected children.
sanitize::Status validate_batch(const ParquetNode &root,
                                const ArrowArray &array) {
  if (array.length < 0) {
    return sanitize::Status::Invalid(
        "native Parquet writer: negative batch length");
  }
  if (root.node_kind != NodeKind::kRoot) {
    return sanitize::Status::Invalid(
        "native Parquet writer: root is not struct");
  }
  if (array.n_children != static_cast<std::int64_t>(root.children.size()) ||
      (!root.children.empty() && !array.children)) {
    return sanitize::Status::Invalid(
        "native Parquet writer: root array/schema mismatch");
  }
  return sanitize::Status::OK();
}

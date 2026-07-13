// Selects, writes, estimates, and slices native Parquet pages.

struct EncodedValueCandidate {
  std::int32_t encoding = kEncodingPlain;
  std::string values;
  std::optional<DictionaryEncodingData> dictionary;
};

std::vector<EncodedValueCandidate>
encoded_value_candidates(const LeafColumn &column,
                         const ColumnPageData &page_data,
                         bool allow_dictionary) {
  std::vector<EncodedValueCandidate> candidates;
  candidates.push_back(EncodedValueCandidate{
      .encoding = kEncodingPlain,
      .values = std::string(page_data.values),
      .dictionary = std::nullopt,
  });

  if (allow_dictionary) {
    if (auto dictionary =
            try_dictionary_encode_column(column, page_data.values)) {
      candidates.push_back(EncodedValueCandidate{
          .encoding = kEncodingRleDictionary,
          .values = dictionary->encoded_indices,
          .dictionary = std::move(dictionary),
      });
    }
  }
  if (is_float_column(column)) {
    if (auto encoded = encode_byte_stream_split(page_data.values,
                                                float_value_width(column))) {
      candidates.push_back(EncodedValueCandidate{
          .encoding = kEncodingByteStreamSplit,
          .values = std::move(*encoded),
          .dictionary = std::nullopt,
      });
    }
  }
  if (auto encoded =
          encode_delta_binary_packed_column(column, page_data.values)) {
    candidates.push_back(EncodedValueCandidate{
        .encoding = kEncodingDeltaBinaryPacked,
        .values = std::move(*encoded),
        .dictionary = std::nullopt,
    });
  }
  if (is_byte_array_kind(leaf_value_kind(column))) {
    if (auto encoded = encode_delta_length_byte_array(page_data.values)) {
      candidates.push_back(EncodedValueCandidate{
          .encoding = kEncodingDeltaLengthByteArray,
          .values = std::move(*encoded),
          .dictionary = std::nullopt,
      });
    }
  }
  return candidates;
}

std::string encode_page_levels_and_values(const LeafColumn &column,
                                          const ColumnPageData &page_data,
                                          std::string_view encoded_values) {
  std::string payload;
  if (column.max_repetition_level > 0) {
    payload.append(encode_levels(page_data.repetition_levels,
                                 column.max_repetition_level));
  }
  if (column.max_definition_level > 0) {
    payload.append(encode_levels(page_data.definition_levels,
                                 column.max_definition_level));
  }
  payload.append(encoded_values);
  return payload;
}

struct PreparedPagePayload {
  EncodedValueCandidate candidate;
  std::string payload;
  std::string compressed_payload;
  std::string compressed_dictionary;
};

bool prefers_dictionary_tie_break(const LeafColumn &column) {
  if (column.path.empty()) {
    return false;
  }
  const auto &name = column.path.back();
  return name == "source_file" || name == "schema_registry" ||
         name == "schema_drifts" || name == "year" || name == "month" ||
         name == "date" || name == "hour";
}

sanitize::Result<PreparedPagePayload>
choose_page_payload(const LeafColumn &column, const ColumnPageData &page_data,
                    CompressionCodec codec, bool allow_dictionary) {
  auto candidates =
      encoded_value_candidates(column, page_data, allow_dictionary);
  std::optional<PreparedPagePayload> best;
  std::size_t best_size = std::numeric_limits<std::size_t>::max();
  for (auto &candidate : candidates) {
    std::string payload =
        encode_page_levels_and_values(column, page_data, candidate.values);
    SAN_ASSIGN_OR_RAISE(auto compressed_payload,
                        compress_payload(payload, codec));
    std::string compressed_dictionary;
    std::size_t candidate_size = compressed_payload.size();
    if (candidate.dictionary) {
      SAN_ASSIGN_OR_RAISE(
          compressed_dictionary,
          compress_payload(candidate.dictionary->dictionary_values, codec));
      candidate_size += compressed_dictionary.size();
    }
    const bool encoding_tie_break =
        candidate_size == best_size &&
        ((candidate.dictionary && prefers_dictionary_tie_break(column)) ||
         candidate.encoding == kEncodingByteStreamSplit);
    if (!best || candidate_size < best_size || encoding_tie_break) {
      best_size = candidate_size;
      best = PreparedPagePayload{
          .candidate = std::move(candidate),
          .payload = std::move(payload),
          .compressed_payload = std::move(compressed_payload),
          .compressed_dictionary = std::move(compressed_dictionary),
      };
    }
  }
  if (!best) {
    return sanitize::Status::Invalid(
        "native Parquet writer: no encodable Parquet page candidate");
  }
  return std::move(*best);
}

// Writes one column data page and returns its file offsets and sizes.
sanitize::Result<PageInfo> write_column_page(CountingOutput &out,
                                             const LeafColumn &column,
                                             const ColumnPageData &page_data,
                                             CompressionCodec codec,
                                             bool allow_dictionary) {
  SAN_ASSIGN_OR_RAISE(
      auto prepared,
      choose_page_payload(column, page_data, codec, allow_dictionary));
  if (prepared.payload.size() >
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
      static_cast<std::int32_t>(prepared.payload.size()),
      static_cast<std::int32_t>(prepared.compressed_payload.size()),
      prepared.candidate.encoding);
  PageInfo info;
  info.value_encoding = prepared.candidate.encoding;
  info.has_plain_encoding = prepared.candidate.encoding == kEncodingPlain;
  info.has_delta_binary_packed_encoding =
      prepared.candidate.encoding == kEncodingDeltaBinaryPacked;
  info.has_delta_length_byte_array_encoding =
      prepared.candidate.encoding == kEncodingDeltaLengthByteArray;
  info.has_byte_stream_split_encoding =
      prepared.candidate.encoding == kEncodingByteStreamSplit;
  if (prepared.candidate.dictionary) {
    if (prepared.candidate.dictionary->dictionary_values.size() >
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
        prepared.candidate.dictionary->dictionary_size,
        static_cast<std::int32_t>(
            prepared.candidate.dictionary->dictionary_values.size()),
        static_cast<std::int32_t>(prepared.compressed_dictionary.size()));
    info.dictionary_encoded = true;
    info.dictionary_page_offset = out.offset();
    info.first_page_offset = info.dictionary_page_offset;
    info.dictionary_header_size =
        static_cast<std::int64_t>(dictionary_header.size());
    info.dictionary_uncompressed_payload_size = static_cast<std::int64_t>(
        prepared.candidate.dictionary->dictionary_values.size());
    info.dictionary_compressed_payload_size =
        static_cast<std::int64_t>(prepared.compressed_dictionary.size());
    SAN_RETURN_NOT_OK(out.Write(dictionary_header));
    SAN_RETURN_NOT_OK(out.Write(prepared.compressed_dictionary));
  }

  info.offset = out.offset();
  if (!prepared.candidate.dictionary) {
    info.first_page_offset = info.offset;
  }
  info.header_size = static_cast<std::int64_t>(header.size());
  info.uncompressed_payload_size =
      static_cast<std::int64_t>(prepared.payload.size());
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
  return page_data.values.size() + page_data.bool_values.size() +
         page_data.definition_levels.size() * sizeof(std::int16_t) +
         page_data.repetition_levels.size() * sizeof(std::int16_t);
}

std::size_t
estimate_row_group_page_data_bytes(const std::vector<ColumnPageData> &pages) {
  std::size_t total = 0;
  for (const auto &page : pages) {
    total += estimate_column_page_data_bytes(page);
  }
  return total;
}

std::vector<std::pair<std::size_t, std::size_t>>
byte_array_value_offsets(std::string_view values) {
  std::vector<std::pair<std::size_t, std::size_t>> offsets;
  std::size_t offset = 0;
  while (offset < values.size()) {
    const auto start = offset;
    if (values.size() - offset < sizeof(std::uint32_t)) {
      return {};
    }
    const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
    offset += sizeof(std::uint32_t);
    if (values.size() - offset < size) {
      return {};
    }
    offset += size;
    offsets.emplace_back(start, offset);
  }
  return offsets;
}

struct PageSliceIndex {
  jsonl::JsonlKind kind = jsonl::JsonlKind::kNull;
  std::size_t fixed_width = 0;
  std::vector<std::size_t> present_value_prefix;
  std::vector<std::pair<std::size_t, std::size_t>> byte_offsets;
};

PageSliceIndex build_page_slice_index(const LeafColumn &column,
                                      const ColumnPageData &page_data) {
  PageSliceIndex index;
  index.kind = leaf_value_kind(column);
  index.fixed_width = fixed_value_width_for_leaf(column);
  if (is_byte_array_kind(index.kind)) {
    index.byte_offsets = byte_array_value_offsets(page_data.values);
  }

  index.present_value_prefix.reserve(page_data.definition_levels.size() + 1);
  index.present_value_prefix.push_back(0);
  std::size_t present_values = 0;
  for (const auto level : page_data.definition_levels) {
    present_values += level == column.max_definition_level ? 1U : 0U;
    index.present_value_prefix.push_back(present_values);
  }
  return index;
}

ColumnPageData slice_column_page_data(const LeafColumn &column,
                                      const ColumnPageData &page_data,
                                      const PageSliceIndex &index,
                                      std::size_t begin, std::size_t end) {
  ColumnPageData out;
  out.definition_levels.insert(out.definition_levels.end(),
                               page_data.definition_levels.begin() + begin,
                               page_data.definition_levels.begin() + end);
  if (column.max_repetition_level > 0) {
    out.repetition_levels.insert(out.repetition_levels.end(),
                                 page_data.repetition_levels.begin() + begin,
                                 page_data.repetition_levels.begin() + end);
  }

  const auto value_begin = index.present_value_prefix[begin];
  const auto value_end = index.present_value_prefix[end];
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
    const auto available_end = std::min(value_end, index.byte_offsets.size());
    if (value_begin < available_end) {
      const auto start = index.byte_offsets[value_begin].first;
      const auto stop = index.byte_offsets[available_end - 1].second;
      out.values.append(page_data.values, start, stop - start);
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
                                       std::size_t page_bool_values) {
  std::size_t bytes = sizeof(std::int16_t);
  if (column.max_repetition_level > 0) {
    bytes += sizeof(std::int16_t);
  }
  if (page_data.definition_levels[row] != column.max_definition_level) {
    return bytes;
  }

  if (index.kind == jsonl::JsonlKind::kBool) {
    // The estimator counts only staged bool values and their PLAIN bytes.
    if (value_index >= page_data.bool_values.size()) {
      return bytes;
    }
    return bytes + 1 + (page_bool_values % 8 == 0 ? 1 : 0);
  }
  if (is_byte_array_kind(index.kind)) {
    if (value_index < index.byte_offsets.size()) {
      const auto [start, stop] = index.byte_offsets[value_index];
      bytes += stop - start;
    }
    return bytes;
  }
  if (index.fixed_width > 0 &&
      value_index < page_data.values.size() / index.fixed_width) {
    bytes += index.fixed_width;
  }
  return bytes;
}

std::vector<ColumnPageData>
split_column_page_data(const LeafColumn &column,
                       const ColumnPageData &page_data,
                       std::int64_t target_page_bytes) {
  if (target_page_bytes <= 0 ||
      static_cast<std::int64_t>(estimate_column_page_data_bytes(page_data)) <=
          target_page_bytes ||
      page_data.definition_levels.size() <= 1) {
    return {page_data};
  }

  const auto index = build_page_slice_index(column, page_data);
  std::vector<std::pair<std::size_t, std::size_t>> ranges;
  std::size_t begin = 0;
  std::size_t page_bytes = 0;
  std::size_t page_bool_values = 0;
  std::size_t value_index = 0;
  const auto count = page_data.definition_levels.size();
  for (std::size_t row = 0; row < count; ++row) {
    const bool has_value =
        page_data.definition_levels[row] == column.max_definition_level;
    auto increment = page_row_incremental_bytes(column, page_data, index, row,
                                                value_index, page_bool_values);
    if (row > begin &&
        page_bytes + increment > static_cast<std::size_t>(target_page_bytes)) {
      ranges.emplace_back(begin, row);
      begin = row;
      page_bytes = 0;
      page_bool_values = 0;
      increment = page_row_incremental_bytes(column, page_data, index, row,
                                             value_index, page_bool_values);
    }
    page_bytes += increment;
    if (has_value) {
      if (index.kind == jsonl::JsonlKind::kBool &&
          value_index < page_data.bool_values.size()) {
        ++page_bool_values;
      }
      ++value_index;
    }
  }
  ranges.emplace_back(begin, count);

  std::vector<ColumnPageData> out;
  out.reserve(ranges.size());
  for (const auto &[slice_begin, slice_end] : ranges) {
    out.push_back(slice_column_page_data(column, page_data, index, slice_begin,
                                         slice_end));
  }
  return out;
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
                   std::int64_t target_page_bytes) {
  auto pages = split_column_page_data(column, page_data, target_page_bytes);
  ColumnChunkInfo chunk;
  const bool allow_dictionary = pages.size() == 1;
  for (const auto &page_data_part : pages) {
    SAN_ASSIGN_OR_RAISE(auto page,
                        write_column_page(out, column, page_data_part, codec,
                                          allow_dictionary));
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

// Selects and applies optional Parquet value encodings.

struct DictionaryEncodingData {
  std::string dictionary_values;
  std::string encoded_indices;
  std::int32_t dictionary_size = 0;
};

std::uint8_t dictionary_bit_width(std::size_t dictionary_size) {
  return static_cast<std::uint8_t>(
      std::max<std::size_t>(1U, std::bit_width(dictionary_size - 1U)));
}

void append_rle_run_u32(std::string &out, std::int64_t run_length,
                        std::uint32_t value, std::uint8_t bit_width) {
  append_varint(out, static_cast<std::uint64_t>(run_length) << 1U);
  const auto byte_width = static_cast<std::uint8_t>((bit_width + 7U) / 8U);
  for (std::uint8_t i = 0; i < byte_width; ++i) {
    out.push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
  }
}

template <class Fn>
bool for_each_dictionary_item(const LeafColumn &column,
                              jsonl::JsonlKind kind,
                              std::string_view values, Fn &&fn) {
  if (is_byte_array_kind(kind)) {
    std::size_t offset = 0;
    while (offset < values.size()) {
      const auto item_start = offset;
      if (values.size() - offset < sizeof(std::uint32_t)) {
        return false;
      }
      const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
      offset += sizeof(std::uint32_t);
      if (values.size() - offset < size) {
        return false;
      }
      offset += size;
      if (!fn(values.substr(item_start, offset - item_start))) {
        return false;
      }
    }
    return true;
  }

  const auto width = fixed_dictionary_value_width(*column.node, kind);
  if (!width || *width == 0 || values.size() % *width != 0) {
    return false;
  }
  for (std::size_t offset = 0; offset < values.size(); offset += *width) {
    if (!fn(values.substr(offset, *width))) {
      return false;
    }
  }
  return true;
}

std::size_t estimated_dictionary_scratch_bytes(std::size_t entry_count,
                                               std::size_t value_bytes) {
  constexpr std::size_t kEstimatedEntryOverhead =
      sizeof(std::string_view) +
      sizeof(std::pair<const std::string_view, std::uint32_t>) +
      4U * sizeof(void *);
  return saturating_size_add(
      value_bytes,
      saturating_size_multiply(entry_count, kEstimatedEntryOverhead));
}

std::optional<DictionaryEncodingData>
try_dictionary_encode_column(const LeafColumn &column,
                             std::string_view values) {
  const auto kind = column.node->arrow_kind == jsonl::JsonlKind::kDictionary
                        ? column.node->dictionary_value_kind
                        : column.node->arrow_kind;
  if (kind == jsonl::JsonlKind::kBool || values.empty()) {
    return std::nullopt;
  }

  std::vector<std::string_view> dictionary;
  internal::BorrowedStringLookupMap<std::uint32_t> index_by_value;
  std::size_t dictionary_bytes = 0;
  std::size_t value_count = 0;

  constexpr std::size_t kInitialUniqueReserve = 4096;
  const auto estimated_count = is_byte_array_kind(kind)
                                   ? std::max<std::size_t>(
                                         1U, values.size() /
                                                 (sizeof(std::uint32_t) + 4U))
                                   : values.size();
  const auto initial_unique_capacity =
      std::min(estimated_count, kInitialUniqueReserve);
  dictionary.reserve(initial_unique_capacity);
  index_by_value.reserve(initial_unique_capacity);

  const auto collect_item = [&](std::string_view current) -> bool {
    value_count = saturating_size_add(value_count, 1U);
    if (value_count == std::numeric_limits<std::size_t>::max()) {
      return false;
    }
    if (index_by_value.contains(current)) {
      return true;
    }
    if (dictionary.size() >= kMaxDictionaryCandidateEntries ||
        dictionary.size() >=
            static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
      return false;
    }
    const auto next_dictionary_bytes =
        saturating_size_add(dictionary_bytes, current.size());
    const auto next_entry_count = dictionary.size() + 1U;
    if (next_dictionary_bytes == std::numeric_limits<std::size_t>::max() ||
        estimated_dictionary_scratch_bytes(next_entry_count,
                                           next_dictionary_bytes) >
            kMaxDictionaryCandidateScratchBytes) {
      return false;
    }
    const auto next_index = static_cast<std::uint32_t>(dictionary.size());
    index_by_value.emplace(current, next_index);
    dictionary.push_back(current);
    dictionary_bytes = next_dictionary_bytes;
    return true;
  };

  if (!for_each_dictionary_item(column, kind, values, collect_item) ||
      dictionary.empty() || dictionary.size() >= value_count) {
    return std::nullopt;
  }

  DictionaryEncodingData out;
  out.dictionary_size = static_cast<std::int32_t>(dictionary.size());
  out.dictionary_values.reserve(dictionary_bytes);
  for (const auto value : dictionary) {
    out.dictionary_values.append(value);
  }

  const auto bit_width = dictionary_bit_width(dictionary.size());
  out.encoded_indices.push_back(static_cast<char>(bit_width));
  std::uint32_t current_index = 0;
  std::int64_t run_length = 0;
  const auto encode_item = [&](std::string_view current) -> bool {
    const auto found = index_by_value.find(current);
    if (found == index_by_value.end()) {
      return false;
    }
    if (run_length == 0) {
      current_index = found->second;
      run_length = 1;
      return true;
    }
    if (found->second == current_index) {
      ++run_length;
      return true;
    }
    append_rle_run_u32(out.encoded_indices, run_length, current_index,
                       bit_width);
    current_index = found->second;
    run_length = 1;
    return true;
  };
  if (!for_each_dictionary_item(column, kind, values, encode_item)) {
    return std::nullopt;
  }
  if (run_length > 0) {
    append_rle_run_u32(out.encoded_indices, run_length, current_index,
                       bit_width);
  }
  return out;
}

void append_zigzag_varint(std::string &out, std::int64_t value) {
  const auto encoded = (static_cast<std::uint64_t>(value) << 1U) ^
                       static_cast<std::uint64_t>(value >> 63U);
  append_varint(out, encoded);
}

std::uint8_t bit_width_u64(std::uint64_t value) {
  return static_cast<std::uint8_t>(std::bit_width(value));
}

void append_bit_packed_values(std::string &out,
                              std::span<const std::uint64_t> values,
                              std::uint8_t bit_width) {
  if (bit_width == 0 || values.empty()) {
    return;
  }
  std::uint64_t buffer = 0;
  std::uint8_t bits_in_buffer = 0;
  for (const auto value : values) {
    std::uint8_t written = 0;
    while (written < bit_width) {
      const auto room = static_cast<std::uint8_t>(64U - bits_in_buffer);
      const auto take =
          static_cast<std::uint8_t>(std::min<int>(bit_width - written, room));
      const std::uint64_t mask = take == 64
                                     ? std::numeric_limits<std::uint64_t>::max()
                                     : ((std::uint64_t{1} << take) - 1U);
      buffer |= ((value >> written) & mask) << bits_in_buffer;
      bits_in_buffer = static_cast<std::uint8_t>(bits_in_buffer + take);
      written = static_cast<std::uint8_t>(written + take);
      while (bits_in_buffer >= 8) {
        out.push_back(static_cast<char>(buffer & 0xFFU));
        buffer >>= 8U;
        bits_in_buffer = static_cast<std::uint8_t>(bits_in_buffer - 8U);
      }
    }
  }
  if (bits_in_buffer > 0) {
    out.push_back(static_cast<char>(buffer & 0xFFU));
  }
}

std::optional<std::int64_t> checked_i64_subtract(std::int64_t left,
                                                 std::int64_t right) {
  if ((right > 0 && left < std::numeric_limits<std::int64_t>::min() + right) ||
      (right < 0 && left > std::numeric_limits<std::int64_t>::max() + right)) {
    return std::nullopt;
  }
  return left - right;
}

std::uint64_t nonnegative_i64_difference(std::int64_t high, std::int64_t low) {
  if (low >= 0) {
    return static_cast<std::uint64_t>(high - low);
  }
  if (high < 0) {
    return static_cast<std::uint64_t>(high - low);
  }
  return static_cast<std::uint64_t>(high) +
         static_cast<std::uint64_t>(-(low + 1)) + 1U;
}

template <class NextValue>
std::optional<std::string>
encode_delta_binary_packed_sequence(std::size_t value_count,
                                    NextValue &&next_value) {
  constexpr std::size_t kBlockSize = 128;
  constexpr std::size_t kMiniBlockCount = 4;
  constexpr std::size_t kMiniBlockSize = kBlockSize / kMiniBlockCount;
  std::string out;
  if (value_count == 0) {
    return out;
  }
  append_varint(out, kBlockSize);
  append_varint(out, kMiniBlockCount);
  append_varint(out, static_cast<std::uint64_t>(value_count));
  auto previous_value = next_value();
  append_zigzag_varint(out, previous_value);
  if (value_count == 1) {
    return out;
  }

  std::array<std::int64_t, kBlockSize> block_deltas{};
  std::array<std::array<std::uint64_t, kMiniBlockSize>, kMiniBlockCount>
      adjusted{};
  const auto delta_count = value_count - 1U;
  for (std::size_t block_start = 0; block_start < delta_count;
       block_start += kBlockSize) {
    const auto block_count =
        std::min<std::size_t>(kBlockSize, delta_count - block_start);
    auto min_delta = std::numeric_limits<std::int64_t>::max();
    for (std::size_t i = 0; i < block_count; ++i) {
      const auto current_value = next_value();
      auto delta = checked_i64_subtract(current_value, previous_value);
      if (!delta) {
        return std::nullopt;
      }
      previous_value = current_value;
      block_deltas[i] = *delta;
      min_delta = std::min(min_delta, *delta);
    }
    append_zigzag_varint(out, min_delta);

    std::array<std::uint8_t, kMiniBlockCount> bit_widths{};
    for (std::size_t mini = 0; mini < kMiniBlockCount; ++mini) {
      auto &mini_values = adjusted[mini];
      mini_values.fill(0);
      std::uint64_t max_value = 0;
      const auto mini_start = mini * kMiniBlockSize;
      const auto mini_count =
          mini_start < block_count
              ? std::min<std::size_t>(kMiniBlockSize, block_count - mini_start)
              : 0U;
      for (std::size_t i = 0; i < mini_count; ++i) {
        const auto value =
            nonnegative_i64_difference(block_deltas[mini_start + i], min_delta);
        mini_values[i] = value;
        max_value = std::max(max_value, value);
      }
      bit_widths[mini] = bit_width_u64(max_value);
      out.push_back(static_cast<char>(bit_widths[mini]));
    }
    for (std::size_t mini = 0; mini < kMiniBlockCount; ++mini) {
      append_bit_packed_values(out, std::span(adjusted[mini]),
                               bit_widths[mini]);
    }
  }
  return out;
}

bool is_float_column(const LeafColumn &column) {
  if (!column.node) {
    return false;
  }
  const auto kind = column.node->arrow_kind == jsonl::JsonlKind::kDictionary
                        ? column.node->dictionary_value_kind
                        : column.node->arrow_kind;
  return kind == jsonl::JsonlKind::kFloat32 ||
         kind == jsonl::JsonlKind::kFloat64;
}

std::size_t float_value_width(const LeafColumn &column) {
  const auto kind = column.node->arrow_kind == jsonl::JsonlKind::kDictionary
                        ? column.node->dictionary_value_kind
                        : column.node->arrow_kind;
  return kind == jsonl::JsonlKind::kFloat32 ? sizeof(float) : sizeof(double);
}

jsonl::JsonlKind leaf_value_kind(const LeafColumn &column) {
  return column.node->arrow_kind == jsonl::JsonlKind::kDictionary
             ? column.node->dictionary_value_kind
             : column.node->arrow_kind;
}


// Encodes float values with Parquet BYTE_STREAM_SPLIT.
std::optional<std::string> encode_byte_stream_split(std::string_view values,
                                                    std::size_t width) {
  if (width == 0 || values.empty() || values.size() % width != 0) {
    return std::nullopt;
  }
  const auto count = values.size() / width;
  std::string out;
  out.resize_and_overwrite(values.size(), [&](char *target, std::size_t size) {
    for (std::size_t byte_index = 0; byte_index < width; ++byte_index) {
      for (std::size_t value_index = 0; value_index < count; ++value_index) {
        target[byte_index * count + value_index] =
            values[value_index * width + byte_index];
      }
    }
    return size;
  });
  return out;
}

template <class T>
std::int64_t load_signed_delta_value(std::string_view values,
                                     std::size_t index) {
  T value{};
  std::memcpy(&value, values.data() + index * sizeof(T), sizeof(T));
  return static_cast<std::int64_t>(value);
}

std::optional<std::string>
encode_delta_binary_packed_column(const LeafColumn &column,
                                  std::string_view values) {
  const auto kind = leaf_value_kind(column);
  switch (kind) {
  case jsonl::JsonlKind::kInt8:
  case jsonl::JsonlKind::kInt16:
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kDate32:
  case jsonl::JsonlKind::kTime32s: {
    if (values.empty() || values.size() % sizeof(std::int32_t) != 0) {
      return std::nullopt;
    }
    const auto count = values.size() / sizeof(std::int32_t);
    if (count < 2) {
      return std::nullopt;
    }
    std::size_t index = 0;
    return encode_delta_binary_packed_sequence(count, [&] {
      return load_signed_delta_value<std::int32_t>(values, index++);
    });
  }
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos: {
    if (values.empty() || values.size() % sizeof(std::int64_t) != 0) {
      return std::nullopt;
    }
    const auto count = values.size() / sizeof(std::int64_t);
    if (count < 2) {
      return std::nullopt;
    }
    std::size_t index = 0;
    return encode_delta_binary_packed_sequence(count, [&] {
      return load_signed_delta_value<std::int64_t>(values, index++);
    });
  }
  default:
    return std::nullopt;
  }
}

std::optional<std::string>
encode_delta_length_byte_array(std::string_view values) {
  if (values.empty()) {
    return std::nullopt;
  }
  std::size_t value_count = 0;
  std::size_t offset = 0;
  while (offset < values.size()) {
    if (values.size() - offset < sizeof(std::uint32_t)) {
      return std::nullopt;
    }
    const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
    offset += sizeof(std::uint32_t);
    if (values.size() - offset < size) {
      return std::nullopt;
    }
    offset += size;
    value_count = saturating_size_add(value_count, 1U);
    if (value_count == std::numeric_limits<std::size_t>::max()) {
      return std::nullopt;
    }
  }
  if (value_count < 2) {
    return std::nullopt;
  }

  offset = 0;
  auto encoded_lengths = encode_delta_binary_packed_sequence(value_count, [&] {
    const auto size = static_cast<std::int64_t>(read_u32_le(values, offset));
    offset += sizeof(std::uint32_t) + static_cast<std::size_t>(size);
    return size;
  });
  if (!encoded_lengths) {
    return std::nullopt;
  }
  std::string out = std::move(*encoded_lengths);
  const auto prefix_bytes =
      saturating_size_multiply(value_count, sizeof(std::uint32_t));
  const auto raw_value_bytes = values.size() - prefix_bytes;
  const auto reserve_size = saturating_size_add(out.size(), raw_value_bytes);
  if (reserve_size == std::numeric_limits<std::size_t>::max()) {
    return std::nullopt;
  }
  out.reserve(reserve_size);
  offset = 0;
  for (std::size_t index = 0; index < value_count; ++index) {
    const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
    offset += sizeof(std::uint32_t);
    out.append(values.substr(offset, size));
    offset += size;
  }
  return out;
}

bool prefers_dictionary_tie_break(const LeafColumn &column) {
  if (column.path.empty()) {
    return false;
  }
  const auto &name = column.path.back();
  return name == "source_file" || name == "schema_registry" ||
         name == "schema_drifts" || name == "year" || name == "month" ||
         name == "date" || name == "hour";
}

std::string encode_page_levels_and_values(const LeafColumn &column,
                                          const ColumnPageData &page_data,
                                          std::string_view encoded_values) {
  std::string payload;
  if (column.max_repetition_level > 0) {
    auto levels = encode_levels(page_data.repetition_levels,
                                column.max_repetition_level);
    payload.reserve(levels.size() + encoded_values.size());
    payload.append(levels);
  }
  if (column.max_definition_level > 0) {
    auto levels = encode_levels(page_data.definition_levels,
                                column.max_definition_level);
    payload.reserve(payload.size() + levels.size() + encoded_values.size());
    payload.append(levels);
  }
  payload.append(encoded_values);
  return payload;
}

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

std::string encode_dictionary_indices(const std::vector<std::uint32_t> &indices,
                                      std::size_t dictionary_size) {
  const auto bit_width = dictionary_bit_width(dictionary_size);
  std::string out;
  out.push_back(static_cast<char>(bit_width));
  if (indices.empty()) {
    return out;
  }
  auto current = indices.front();
  std::int64_t run_length = 1;
  for (std::size_t i = 1; i < indices.size(); ++i) {
    if (indices[i] == current) {
      ++run_length;
      continue;
    }
    append_rle_run_u32(out, run_length, current, bit_width);
    current = indices[i];
    run_length = 1;
  }
  append_rle_run_u32(out, run_length, current, bit_width);
  return out;
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

  std::vector<std::uint32_t> indices;
  std::vector<std::string_view> dictionary;
  internal::BorrowedStringLookupMap<std::uint32_t> index_by_value;
  std::size_t dictionary_bytes = 0;

  auto append_item = [&](std::string_view current) -> bool {
    const auto found = index_by_value.find(current);
    if (found != index_by_value.end()) {
      indices.push_back(found->second);
      return true;
    }
    if (dictionary.size() >=
        static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
      return false;
    }
    const auto next_index = static_cast<std::uint32_t>(dictionary.size());
    index_by_value.emplace(current, next_index);
    dictionary.push_back(current);
    dictionary_bytes += current.size();
    indices.push_back(next_index);
    return true;
  };

  constexpr std::size_t kInitialUniqueReserve = 4096;
  if (is_byte_array_kind(kind)) {
    const auto estimated_count =
        std::max<std::size_t>(1U, values.size() / (sizeof(std::uint32_t) + 4U));
    const auto initial_unique_capacity =
        std::min(estimated_count, kInitialUniqueReserve);
    indices.reserve(estimated_count);
    dictionary.reserve(initial_unique_capacity);
    index_by_value.reserve(initial_unique_capacity);
    std::size_t offset = 0;
    while (offset < values.size()) {
      const auto item_start = offset;
      if (values.size() - offset < sizeof(std::uint32_t)) {
        return std::nullopt;
      }
      const auto size = static_cast<std::size_t>(read_u32_le(values, offset));
      offset += sizeof(std::uint32_t);
      if (values.size() - offset < size) {
        return std::nullopt;
      }
      offset += size;
      if (!append_item(values.substr(item_start, offset - item_start))) {
        return std::nullopt;
      }
    }
  } else {
    const auto width = fixed_dictionary_value_width(*column.node, kind);
    if (!width || *width == 0 || values.size() % *width != 0) {
      return std::nullopt;
    }
    const auto value_count = values.size() / *width;
    const auto initial_unique_capacity =
        std::min(value_count, kInitialUniqueReserve);
    indices.reserve(value_count);
    dictionary.reserve(initial_unique_capacity);
    index_by_value.reserve(initial_unique_capacity);
    for (std::size_t offset = 0; offset < values.size(); offset += *width) {
      if (!append_item(values.substr(offset, *width))) {
        return std::nullopt;
      }
    }
  }

  if (dictionary.empty() || dictionary.size() >= indices.size()) {
    return std::nullopt;
  }

  DictionaryEncodingData out;
  out.dictionary_size = static_cast<std::int32_t>(dictionary.size());
  out.dictionary_values.reserve(dictionary_bytes);
  for (const auto value : dictionary) {
    out.dictionary_values.append(value);
  }
  out.encoded_indices = encode_dictionary_indices(indices, dictionary.size());
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

std::optional<std::string>
encode_delta_binary_packed_values(const std::vector<std::int64_t> &values) {
  constexpr std::size_t kBlockSize = 128;
  constexpr std::size_t kMiniBlockCount = 4;
  constexpr std::size_t kMiniBlockSize = kBlockSize / kMiniBlockCount;
  std::string out;
  if (values.empty()) {
    return out;
  }
  append_varint(out, kBlockSize);
  append_varint(out, kMiniBlockCount);
  append_varint(out, static_cast<std::uint64_t>(values.size()));
  append_zigzag_varint(out, values.front());
  if (values.size() == 1) {
    return out;
  }

  std::array<std::int64_t, kBlockSize> block_deltas{};
  std::array<std::array<std::uint64_t, kMiniBlockSize>, kMiniBlockCount>
      adjusted{};
  const auto delta_count = values.size() - 1U;
  for (std::size_t block_start = 0; block_start < delta_count;
       block_start += kBlockSize) {
    const auto block_count =
        std::min<std::size_t>(kBlockSize, delta_count - block_start);
    auto min_delta = std::numeric_limits<std::int64_t>::max();
    for (std::size_t i = 0; i < block_count; ++i) {
      const auto value_index = block_start + i + 1U;
      auto delta =
          checked_i64_subtract(values[value_index], values[value_index - 1U]);
      if (!delta) {
        return std::nullopt;
      }
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

std::optional<std::vector<std::int64_t>>
signed_delta_values_for_column(const LeafColumn &column,
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
    std::vector<std::int64_t> out;
    out.reserve(values.size() / sizeof(std::int32_t));
    for (std::size_t offset = 0; offset < values.size();
         offset += sizeof(std::int32_t)) {
      std::int32_t value{};
      std::memcpy(&value, values.data() + offset, sizeof(value));
      out.push_back(value);
    }
    return out;
  }
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos: {
    if (values.empty() || values.size() % sizeof(std::int64_t) != 0) {
      return std::nullopt;
    }
    std::vector<std::int64_t> out;
    out.reserve(values.size() / sizeof(std::int64_t));
    for (std::size_t offset = 0; offset < values.size();
         offset += sizeof(std::int64_t)) {
      std::int64_t value{};
      std::memcpy(&value, values.data() + offset, sizeof(value));
      out.push_back(value);
    }
    return out;
  }
  default:
    return std::nullopt;
  }
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

std::optional<std::string>
encode_delta_binary_packed_column(const LeafColumn &column,
                                  std::string_view values) {
  auto extracted = signed_delta_values_for_column(column, values);
  if (!extracted || extracted->size() < 2) {
    return std::nullopt;
  }
  return encode_delta_binary_packed_values(*extracted);
}

std::optional<std::string>
encode_delta_length_byte_array(std::string_view values) {
  if (values.empty()) {
    return std::nullopt;
  }
  std::vector<std::int64_t> lengths;
  lengths.reserve(values.size() / (sizeof(std::uint32_t) + 4U));
  std::size_t offset = 0;
  while (offset < values.size()) {
    if (values.size() - offset < sizeof(std::uint32_t)) {
      return std::nullopt;
    }
    const auto size = read_u32_le(values, offset);
    offset += sizeof(std::uint32_t);
    if (values.size() - offset < size) {
      return std::nullopt;
    }
    lengths.push_back(static_cast<std::int64_t>(size));
    offset += size;
  }
  if (lengths.size() < 2) {
    return std::nullopt;
  }
  auto encoded_lengths = encode_delta_binary_packed_values(lengths);
  if (!encoded_lengths) {
    return std::nullopt;
  }
  std::string out = std::move(*encoded_lengths);
  out.reserve(out.size() + values.size() -
              lengths.size() * sizeof(std::uint32_t));
  offset = 0;
  for (const auto length : lengths) {
    offset += sizeof(std::uint32_t);
    out.append(values.substr(offset, static_cast<std::size_t>(length)));
    offset += static_cast<std::size_t>(length);
  }
  return out;
}

namespace jsonl = sanitize::internal::jsonl_stream_writer;

constexpr std::string_view kUnsupportedPrefix =
    "native Parquet writer: unsupported";
constexpr std::string_view kMagic = "PAR1";
constexpr std::int64_t kDefaultMaxRowsPerRowGroup = 64 * 1024;
constexpr std::int64_t kHardMaxRowsPerRowGroup = 1024 * 1024;
constexpr std::int64_t kDefaultMaxBytesPerRowGroup = 64LL * 1024LL * 1024LL;
constexpr std::int64_t kHardMaxBytesPerRowGroup = 512LL * 1024LL * 1024LL;
constexpr std::int64_t kDefaultTargetPageBytes = 1024LL * 1024LL;
constexpr std::int64_t kHardMaxTargetPageBytes = 64LL * 1024LL * 1024LL;
constexpr std::int64_t kDefaultMaxRowGroups = 65'536;
constexpr std::int64_t kHardMaxRowGroups = kDefaultMaxRowGroups;
constexpr std::int64_t kDefaultMaxFooterMetadataBytes =
    256LL * 1024LL * 1024LL;
constexpr std::int64_t kHardMaxFooterMetadataBytes =
    kDefaultMaxFooterMetadataBytes;
constexpr std::size_t kMaxDictionaryCandidateEntries = 262'144;
constexpr std::size_t kMaxDictionaryCandidateScratchBytes =
    64U * 1024U * 1024U;

std::size_t saturating_size_add(std::size_t left,
                                std::size_t right) noexcept {
  return right > std::numeric_limits<std::size_t>::max() - left
             ? std::numeric_limits<std::size_t>::max()
             : left + right;
}

std::size_t saturating_size_multiply(std::size_t left,
                                     std::size_t right) noexcept {
  if (left == 0 || right == 0) {
    return 0;
  }
  return left > std::numeric_limits<std::size_t>::max() / right
             ? std::numeric_limits<std::size_t>::max()
             : left * right;
}

constexpr std::int32_t kEncodingPlain = 0;
constexpr std::int32_t kEncodingDeltaBinaryPacked = 5;
constexpr std::int32_t kEncodingDeltaLengthByteArray = 6;
constexpr std::int32_t kEncodingRleDictionary = 8;
constexpr std::int32_t kEncodingByteStreamSplit = 9;

enum class PhysicalType : int32_t {
  kBoolean = 0,
  kInt32 = 1,
  kInt64 = 2,
  kFloat = 4,
  kDouble = 5,
  kByteArray = 6,
  kFixedLenByteArray = 7,
};

enum class CompressionCodec : int32_t {
  kUncompressed = 0,
  kSnappy = 1,
  kGzip = 2,
};

enum class ConvertedType : int32_t {
  kUtf8 = 0,
  kMap = 1,
  kList = 3,
  kDecimal = 5,
  kDate = 6,
  kTimeMillis = 7,
  kUInt8 = 11,
  kUInt16 = 12,
  kUInt32 = 13,
  kUInt64 = 14,
  kInt8 = 15,
  kInt16 = 16,
  kInt32 = 17,
  kInt64 = 18,
};

enum class NodeKind {
  kRoot,
  kStruct,
  kList,
  kMap,
  kPrimitive,
};

struct ParquetNode {
  NodeKind node_kind = NodeKind::kPrimitive;
  jsonl::JsonlKind arrow_kind = jsonl::JsonlKind::kNull;
  jsonl::JsonlKind dictionary_index_kind = jsonl::JsonlKind::kNull;
  jsonl::JsonlKind dictionary_value_kind = jsonl::JsonlKind::kNull;
  std::string name;
  bool required = false;
  PhysicalType physical_type = PhysicalType::kInt64;
  bool has_converted_type = false;
  ConvertedType converted_type = ConvertedType::kUtf8;
  bool has_null_logical_type = false;
  bool has_timestamp_logical_type = false;
  bool has_time_millis_logical_type = false;
  bool has_int_logical_type = false;
  std::int8_t int_bit_width = 0;
  bool int_is_signed = true;
  bool has_decimal_metadata = false;
  std::int32_t decimal_precision = 0;
  std::int32_t decimal_scale = 0;
  std::int32_t decimal_byte_width = 0;
  std::int32_t fixed_binary_byte_width = 0;
  std::vector<ParquetNode> children;
  std::unique_ptr<ParquetNode> element;
  std::size_t leaf_index = 0;
  std::size_t first_leaf_index = 0;
  std::size_t leaf_count = 0;
  std::size_t subtree_schema_element_count = 1;
  std::int16_t repeated_repetition_level = 0;
  std::int32_t fixed_size_list_size = 0;
};

struct LeafColumn {
  const ParquetNode *node = nullptr;
  std::vector<std::string> path;
  std::int16_t max_definition_level = 0;
  std::int16_t max_repetition_level = 0;
};

struct ColumnPageData {
  std::vector<std::int16_t> definition_levels;
  std::vector<std::int16_t> repetition_levels;
  std::string values;
  std::vector<bool> bool_values;
};

struct PageInfo {
  std::int64_t first_page_offset = 0;
  std::int64_t offset = 0;
  std::int64_t dictionary_page_offset = -1;
  std::int64_t dictionary_header_size = 0;
  std::int64_t dictionary_uncompressed_payload_size = 0;
  std::int64_t dictionary_compressed_payload_size = 0;
  std::int64_t header_size = 0;
  std::int64_t uncompressed_payload_size = 0;
  std::int64_t compressed_payload_size = 0;
  std::int64_t num_values = 0;
  std::int64_t null_count = 0;
  std::optional<std::string> min_value;
  std::optional<std::string> max_value;
  bool dictionary_encoded = false;
  bool has_plain_encoding = false;
  bool has_delta_binary_packed_encoding = false;
  bool has_delta_length_byte_array_encoding = false;
  bool has_byte_stream_split_encoding = false;
  std::int32_t value_encoding = kEncodingPlain;
};

struct PreparedDictionaryInfo {
  std::int32_t value_count = 0;
  std::size_t uncompressed_payload_size = 0;
};

struct PreparedPagePayload {
  std::int32_t encoding = kEncodingPlain;
  std::optional<PreparedDictionaryInfo> dictionary;
  std::size_t uncompressed_payload_size = 0;
  std::string compressed_payload;
  std::string compressed_dictionary;
};

struct ColumnChunkInfo {
  PageInfo aggregate;
  std::vector<PageInfo> pages;
  std::int64_t column_index_offset = -1;
  std::int32_t column_index_length = 0;
  std::int64_t offset_index_offset = -1;
  std::int32_t offset_index_length = 0;
};

struct RowGroupInfo {
  std::int64_t num_rows = 0;
  std::vector<ColumnChunkInfo> columns;
};

class CountingOutput {
public:
  // Wraps an output target and tracks bytes written through it.
  explicit CountingOutput(Output &out) : out_(out) {}

  // Writes bytes and advances the logical file offset.
  sanitize::Status Write(std::string_view data) {
    if (data.size() >
            static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()) ||
        offset_ > std::numeric_limits<std::int64_t>::max() -
                      static_cast<std::int64_t>(data.size())) {
      return sanitize::Status::Invalid(
          "native Parquet writer: output offset exceeds int64 range");
    }
    SAN_RETURN_NOT_OK(out_.Write(data));
    offset_ += static_cast<std::int64_t>(data.size());
    return sanitize::Status::OK();
  }

  // Flushes the wrapped target.
  sanitize::Status Flush() { return out_.Flush(); }

  // Returns the current logical file offset.
  [[nodiscard]] std::int64_t offset() const noexcept { return offset_; }

private:
  Output &out_;
  std::int64_t offset_ = 0;
};

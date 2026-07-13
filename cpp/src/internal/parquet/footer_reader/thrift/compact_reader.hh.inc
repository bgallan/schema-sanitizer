// Declares the bounded Thrift compact reader used by Parquet footer parsing.

class CompactReader {
public:
  explicit CompactReader(std::string_view data) : data_(data) {}

  sanitize::Result<std::uint8_t> read_byte();
  sanitize::Result<std::uint64_t> read_varint();
  sanitize::Result<std::int16_t> read_i16();
  sanitize::Result<std::int32_t> read_i32();
  sanitize::Result<std::int64_t> read_i64();
  sanitize::Result<std::string> read_binary();
  sanitize::Status skip_type(std::uint8_t type, int depth);
  sanitize::Status skip_container_value(std::uint8_t type, int depth);
  sanitize::Status skip_struct(int depth);
  sanitize::Status skip_list(int depth);
  sanitize::Status skip_map(int depth);
  sanitize::Status read_list_header(std::uint8_t *element_type,
                                    std::uint64_t *size);

  [[nodiscard]] std::size_t position() const noexcept { return pos_; }

private:
  std::string_view data_;
  std::size_t pos_ = 0;
};

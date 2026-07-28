// Composes scalar Arrow C Data builders by physical representation.

#include "internal/materialization/builders/detail.hh"
#include "internal/memory/size_math.hh"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory_resource>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace sanitize::internal {
namespace {

using sanitize::LogicalKind;
using sanitize::Status;

template <typename T> class FixedWidthBuilder final : public BaseBuilder {
public:
  explicit FixedWidthBuilder(std::shared_ptr<PoolResource> pool)
      : BaseBuilder(std::move(pool)), values_(pool_.get()) {}

  Status reserve(int64_t rows, int64_t) override {
    if (rows < 0)
      return Status::Invalid("negative fixed-width reserve");
    try {
      values_.reserve(static_cast<std::size_t>(rows));
    } catch (const std::bad_alloc &) {
      return Status::OutOfMemory(
          "FixedWidthBuilder::reserve: allocation failed");
    }
    return Status::OK();
  }

  // Appends the object state.
  Status append(const Cell &cell) override {
    if (cell.is_null)
      return append_null();
    if constexpr (std::is_same_v<T, double>) {
      values_.push_back(cell.f64);
    } else if constexpr (std::is_same_v<T, int64_t>) {
      values_.push_back(static_cast<int64_t>(cell.i64));
    } else {
      values_.push_back(static_cast<T>(cell.i64));
    }
    push_validity(true);
    return Status::OK();
  }

  Status append_direct(const DirectScalarValue &value) override {
    if (value.is_null)
      return append_null();
    if constexpr (std::is_same_v<T, double>) {
      values_.push_back(value.f64);
    } else if constexpr (std::is_same_v<T, int64_t>) {
      values_.push_back(value.i64);
    } else {
      values_.push_back(static_cast<T>(value.i64));
    }
    push_validity(true);
    return Status::OK();
  }

  // Appends null.
  Status append_null() override {
    values_.push_back(T{});
    push_validity(false);
    return Status::OK();
  }

  Status append_array(const ArrowArray &array) override {
    if (array.length < 0 || array.offset < 0 || array.n_buffers < 2 ||
        !array.buffers) {
      return Status::Invalid("invalid fixed-width Arrow array");
    }
    if (array.length > 0 && !array.buffers[1]) {
      return Status::Invalid("fixed-width Arrow array has no values buffer");
    }
    const auto *values = static_cast<const T *>(array.buffers[1]);
    const auto begin = static_cast<std::size_t>(array.offset);
    const auto count = static_cast<std::size_t>(array.length);
    try {
      if (count > 0) {
        values_.insert(values_.end(), values + begin, values + begin + count);
      }
    } catch (const std::bad_alloc &) {
      return Status::OutOfMemory(
          "FixedWidthBuilder::append_array: allocation failed");
    }
    return append_array_validity(array);
  }

  // Finishes the current output.
  Status finish(ArrowArray *out) override {
    auto payload = make_array_payload(pool_);
    if (!payload)
      return Status::OutOfMemory("FixedWidthBuilder::finish: OOM payload");
    payload->validity = std::move(validity_);
    if constexpr (std::is_same_v<T, double>) {
      payload->f64 = std::move(values_);
    } else if constexpr (std::is_same_v<T, int64_t>) {
      payload->i64 = std::move(values_);
    } else {
      payload->i32 = std::move(values_);
    }
    payload->buffers.resize(2);
    payload->buffers[0] = validity_buffer(payload.get(), null_count_);
    if constexpr (std::is_same_v<T, double>) {
      payload->buffers[1] =
          payload->f64.empty() ? nullptr : payload->f64.data();
    } else if constexpr (std::is_same_v<T, int64_t>) {
      payload->buffers[1] =
          payload->i64.empty() ? nullptr : payload->i64.data();
    } else {
      payload->buffers[1] =
          payload->i32.empty() ? nullptr : payload->i32.data();
    }
    init_common(out, payload.get(), 2);
    out->private_data = payload.release();
    return Status::OK();
  }

  // Returns the current byte usage.
  [[nodiscard]] int64_t bytes() const noexcept override {
    return saturating_add_i64(
        saturating_size_to_i64(validity_.capacity()),
        saturating_capacity_bytes(values_.capacity(), sizeof(T)));
  }

protected:
  // Resets values.
  Status reset_values() override {
    values_.clear();
    return Status::OK();
  }

private:
  std::pmr::vector<T> values_;
};
class NullBuilder final : public BaseBuilder {
public:
  explicit NullBuilder(std::shared_ptr<PoolResource> pool)
      : BaseBuilder(std::move(pool)) {}

  // Appends the object state.
  Status append(const Cell &) override { return append_null(); }

  Status append_direct(const DirectScalarValue &) override {
    return append_null();
  }

  // Appends null.
  Status append_null() override {
    ++length_;
    ++null_count_;
    return Status::OK();
  }

  Status append_array(const ArrowArray &array) override {
    if (array.length < 0) {
      return Status::Invalid("invalid null Arrow array length");
    }
    length_ += array.length;
    null_count_ += array.length;
    return Status::OK();
  }

  // Finishes the current output.
  Status finish(ArrowArray *out) override {
    auto payload = make_array_payload(pool_);
    if (!payload)
      return Status::OutOfMemory("NullBuilder::finish: OOM payload");
    payload->buffers.assign(1, nullptr);
    std::memset(out, 0, sizeof(*out));
    out->length = length_;
    out->null_count = length_;
    out->offset = 0;
    out->n_buffers = 1;
    out->buffers = payload->buffers.data();
    out->private_data = payload.release();
    out->release = &array_release;
    return Status::OK();
  }

  // Returns the current byte usage.
  [[nodiscard]] int64_t bytes() const noexcept override { return 0; }

protected:
  // Resets values.
  Status reset_values() override { return Status::OK(); }
};

class BoolBuilder final : public BaseBuilder {
public:
  explicit BoolBuilder(std::shared_ptr<PoolResource> pool)
      : BaseBuilder(std::move(pool)), bits_(pool_.get()) {}

  Status reserve(int64_t rows, int64_t) override {
    if (rows < 0)
      return Status::Invalid("negative boolean reserve");
    try {
      const auto byte_count = static_cast<std::size_t>((rows + 7) / 8);
      bits_.reserve(byte_count);
    } catch (const std::bad_alloc &) {
      return Status::OutOfMemory("BoolBuilder::reserve: allocation failed");
    }
    return Status::OK();
  }

  // Appends the object state.
  Status append(const Cell &cell) override {
    if (cell.is_null)
      return append_null();
    push_bit(cell.b);
    push_validity(true);
    return Status::OK();
  }

  Status append_direct(const DirectScalarValue &value) override {
    if (value.is_null)
      return append_null();
    push_bit(value.b);
    push_validity(true);
    return Status::OK();
  }

  // Appends null.
  Status append_null() override {
    push_bit(false);
    push_validity(false);
    return Status::OK();
  }

  Status append_array(const ArrowArray &array) override {
    if (array.length < 0 || array.offset < 0 || array.n_buffers < 2 ||
        !array.buffers) {
      return Status::Invalid("invalid boolean Arrow array");
    }
    const auto *source_bits = static_cast<const uint8_t *>(array.buffers[1]);
    if (array.length > 0 && !source_bits) {
      return Status::Invalid("boolean Arrow array has no values buffer");
    }
    const auto *validity = static_cast<const uint8_t *>(array.buffers[0]);
    try {
      for (int64_t index = 0; index < array.length; ++index) {
        const int64_t source_index = array.offset + index;
        const bool value =
            ((source_bits[static_cast<std::size_t>(source_index >> 3)] >>
              (source_index & 7)) &
             1u) != 0;
        const bool valid =
            !validity ||
            ((validity[static_cast<std::size_t>(source_index >> 3)] >>
              (source_index & 7)) &
             1u) != 0;
        push_bit(value);
        push_validity(valid);
      }
    } catch (const std::bad_alloc &) {
      return Status::OutOfMemory(
          "BoolBuilder::append_array: allocation failed");
    }
    return Status::OK();
  }

  // Finishes the current output.
  Status finish(ArrowArray *out) override {
    auto payload = make_array_payload(pool_);
    if (!payload)
      return Status::OutOfMemory("BoolBuilder::finish: OOM payload");
    payload->validity = std::move(validity_);
    payload->bit_data = std::move(bits_);
    payload->buffers.resize(2);
    payload->buffers[0] = validity_buffer(payload.get(), null_count_);
    payload->buffers[1] =
        payload->bit_data.empty() ? nullptr : payload->bit_data.data();
    init_common(out, payload.get(), 2);
    out->private_data = payload.release();
    return Status::OK();
  }

  // Returns the current byte usage.
  [[nodiscard]] int64_t bytes() const noexcept override {
    return saturating_add_i64(saturating_size_to_i64(validity_.capacity()),
                              saturating_size_to_i64(bits_.capacity()));
  }

protected:
  // Resets values.
  Status reset_values() override {
    bits_.clear();
    return Status::OK();
  }

private:
  // Performs the push bit operation.
  void push_bit(bool value) {
    if ((length_ & 7) == 0)
      bits_.push_back(0);
    if (value)
      bits_[static_cast<std::size_t>(length_ >> 3)] |=
          static_cast<uint8_t>(1u << (length_ & 7));
  }

  std::pmr::vector<uint8_t> bits_;
};
class Utf8Builder final : public BaseBuilder {
public:
  // Creates a Utf8Builder.
  explicit Utf8Builder(std::shared_ptr<PoolResource> pool)
      : BaseBuilder(std::move(pool)), offsets_(pool_.get()),
        data_(pool_.get()) {
    offsets_.push_back(0);
  }

  Status reserve(int64_t rows, int64_t variable_bytes) override {
    if (rows < 0 || variable_bytes < 0)
      return Status::Invalid("negative UTF-8 reserve");
    try {
      const auto row_capacity = static_cast<std::size_t>(rows);
      if (row_capacity < std::numeric_limits<std::size_t>::max()) {
        offsets_.reserve(row_capacity + 1);
      }
      const auto byte_capacity = static_cast<std::uint64_t>(variable_bytes);
      const auto max_utf8 =
          static_cast<std::uint64_t>(std::numeric_limits<int32_t>::max());
      data_.reserve(
          static_cast<std::size_t>(std::min(byte_capacity, max_utf8)));
    } catch (const std::bad_alloc &) {
      return Status::OutOfMemory("Utf8Builder::reserve: allocation failed");
    }
    return Status::OK();
  }

  // Resets the object state.
  Status reset() override {
    SAN_RETURN_NOT_OK(BaseBuilder::reset());
    offsets_.clear();
    offsets_.push_back(0);
    data_.clear();
    return Status::OK();
  }

  // Appends the object state.
  Status append(const Cell &cell) override {
    if (cell.is_null)
      return append_null();
    if (data_.size() + cell.str.size() >
        static_cast<std::size_t>(std::numeric_limits<int32_t>::max())) {
      return Status::Invalid("utf8 array payload exceeds 2GB");
    }
    data_.insert(data_.end(), cell.str.begin(), cell.str.end());
    offsets_.push_back(static_cast<int32_t>(data_.size()));
    push_validity(true);
    return Status::OK();
  }

  Status append_direct(const DirectScalarValue &value) override {
    if (value.is_null)
      return append_null();
    const std::string_view bytes = value.utf8();
    if (data_.size() + bytes.size() >
        static_cast<std::size_t>(std::numeric_limits<int32_t>::max())) {
      return Status::Invalid("utf8 array payload exceeds 2GB");
    }
    data_.insert(data_.end(), bytes.begin(), bytes.end());
    offsets_.push_back(static_cast<int32_t>(data_.size()));
    push_validity(true);
    return Status::OK();
  }

  // Appends null.
  Status append_null() override {
    offsets_.push_back(offsets_.empty() ? 0 : offsets_.back());
    push_validity(false);
    return Status::OK();
  }

  Status append_array(const ArrowArray &array) override {
    if (array.length < 0 || array.offset < 0 || array.n_buffers < 3 ||
        !array.buffers || !array.buffers[1]) {
      return Status::Invalid("invalid UTF-8 Arrow array");
    }
    const auto *source_offsets = static_cast<const int32_t *>(array.buffers[1]);
    const auto source_begin_index = static_cast<std::size_t>(array.offset);
    const auto source_end_index =
        source_begin_index + static_cast<std::size_t>(array.length);
    const int32_t source_begin = source_offsets[source_begin_index];
    const int32_t source_end = source_offsets[source_end_index];
    if (source_begin < 0 || source_end < source_begin) {
      return Status::Invalid("invalid UTF-8 Arrow offsets");
    }
    const auto source_bytes =
        static_cast<std::size_t>(source_end - source_begin);
    if (source_bytes > 0 && !array.buffers[2]) {
      return Status::Invalid("UTF-8 Arrow array has no data buffer");
    }
    if (data_.size() + source_bytes >
        static_cast<std::size_t>(std::numeric_limits<int32_t>::max())) {
      return Status::Invalid("utf8 array payload exceeds 2GB");
    }

    const auto destination_base = static_cast<int32_t>(data_.size());
    const auto *source_data = static_cast<const char *>(array.buffers[2]);
    try {
      if (source_bytes > 0) {
        data_.insert(data_.end(), source_data + source_begin,
                     source_data + source_end);
      }
      offsets_.reserve(offsets_.size() +
                       static_cast<std::size_t>(array.length));
      for (std::size_t index = source_begin_index + 1;
           index <= source_end_index; ++index) {
        const int32_t relative = source_offsets[index] - source_begin;
        if (relative < 0) {
          return Status::Invalid("invalid UTF-8 Arrow offsets");
        }
        offsets_.push_back(destination_base + relative);
      }
    } catch (const std::bad_alloc &) {
      return Status::OutOfMemory(
          "Utf8Builder::append_array: allocation failed");
    }
    return append_array_validity(array);
  }

  // Finishes the current output.
  Status finish(ArrowArray *out) override {
    auto payload = make_array_payload(pool_);
    if (!payload)
      return Status::OutOfMemory("Utf8Builder::finish: OOM payload");
    payload->validity = std::move(validity_);
    payload->offsets = std::move(offsets_);
    payload->bytes = std::move(data_);
    payload->buffers.resize(3);
    payload->buffers[0] = validity_buffer(payload.get(), null_count_);
    payload->buffers[1] =
        payload->offsets.empty() ? nullptr : payload->offsets.data();
    payload->buffers[2] =
        payload->bytes.empty() ? nullptr : payload->bytes.data();
    init_common(out, payload.get(), 3);
    out->private_data = payload.release();
    return Status::OK();
  }

  // Returns the current byte usage.
  [[nodiscard]] int64_t bytes() const noexcept override {
    auto total = saturating_size_to_i64(validity_.capacity());
    total = saturating_add_i64(
        total, saturating_capacity_bytes(offsets_.capacity(), sizeof(int32_t)));
    return saturating_add_i64(total, saturating_size_to_i64(data_.capacity()));
  }

protected:
  // Resets values.
  Status reset_values() override { return Status::OK(); }

private:
  std::pmr::vector<int32_t> offsets_;
  std::pmr::vector<char> data_;
};

} // namespace

sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_scalar_builder(LogicalKind kind,
                    const std::shared_ptr<PoolResource> &pool) {
  switch (kind) {
  case LogicalKind::kNull:
    return make_column_builder<NullBuilder>(pool);
  case LogicalKind::kBool:
    return make_column_builder<BoolBuilder>(pool);
  case LogicalKind::kInt64:
  case LogicalKind::kTimestampNs:
    return make_column_builder<FixedWidthBuilder<int64_t>>(pool);
  case LogicalKind::kFloat64:
    return make_column_builder<FixedWidthBuilder<double>>(pool);
  case LogicalKind::kDate32:
  case LogicalKind::kTime32s:
    return make_column_builder<FixedWidthBuilder<int32_t>>(pool);
  case LogicalKind::kUtf8:
    return make_column_builder<Utf8Builder>(pool);
  case LogicalKind::kStruct:
  case LogicalKind::kList:
    return Status::Invalid("make_scalar_builder: nested logical kind");
  }
  return Status::Invalid("make_scalar_builder: unsupported logical kind");
}

} // namespace sanitize::internal

// Composes scalar Arrow C Data builders by physical representation.

#include "internal/materialization/builders/detail.hh"
#include "internal/memory/size_math.hh"

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory_resource>
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

  // Appends null.
  Status append_null() override {
    values_.push_back(T{});
    push_validity(false);
    return Status::OK();
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

  // Appends null.
  Status append_null() override {
    ++length_;
    ++null_count_;
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

  // Appends the object state.
  Status append(const Cell &cell) override {
    if (cell.is_null)
      return append_null();
    push_bit(cell.b);
    push_validity(true);
    return Status::OK();
  }

  // Appends null.
  Status append_null() override {
    push_bit(false);
    push_validity(false);
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
    return saturating_add_i64(
        saturating_size_to_i64(validity_.capacity()),
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
      : BaseBuilder(std::move(pool)), offsets_(pool_.get()), data_(pool_.get()) {
    offsets_.push_back(0);
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

  // Appends null.
  Status append_null() override {
    offsets_.push_back(offsets_.empty() ? 0 : offsets_.back());
    push_validity(false);
    return Status::OK();
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

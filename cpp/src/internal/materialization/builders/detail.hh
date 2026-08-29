// Declares shared private helpers for Arrow C Data array builders. The code
// converts validated rows into memory-accounted Arrow C Data batches for
// ordered ingestion.

#pragma once

#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/materialization/batch_appender_internal.hh"
#include "internal/memory/pool_resource.hh"

#include <cstdint>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <new>
#include <utility>
#include <vector>

#include "sanitize/core/logical_schema.hh"

namespace sanitize::internal {

struct ArrayPayload {
  /// Initializes Arrow array payload ownership around the supplied private
  /// allocation state.
  explicit ArrayPayload(std::shared_ptr<PoolResource> pool)
      : pool_keepalive(std::move(pool)), buffers(pool_keepalive.get()),
        validity(pool_keepalive.get()), bit_data(pool_keepalive.get()),
        i32(pool_keepalive.get()), i64(pool_keepalive.get()),
        f64(pool_keepalive.get()), offsets(pool_keepalive.get()),
        bytes(pool_keepalive.get()), children(pool_keepalive.get()) {}

  std::shared_ptr<PoolResource> pool_keepalive;
  std::pmr::vector<const void *> buffers;
  std::pmr::vector<uint8_t> validity;
  std::pmr::vector<uint8_t> bit_data;
  std::pmr::vector<int32_t> i32;
  std::pmr::vector<int64_t> i64;
  std::pmr::vector<double> f64;
  std::pmr::vector<int32_t> offsets;
  std::pmr::vector<char> bytes;
  std::pmr::vector<ArrowArray *> children;
};

/// Releases child Arrow arrays before deleting their owning payload.
inline void destroy_array_payload(ArrayPayload *payload) noexcept {
  if (!payload)
    return;
  for (ArrowArray *child : payload->children) {
    if (!child)
      continue;
    cdata_stream::release_array_nothrow(child);
    delete child;
  }
  delete payload;
}

struct ArrayPayloadDeleter {
  /// Releases child Arrow arrays and deletes an unfinished array payload.
  void operator()(ArrayPayload *payload) const noexcept {
    destroy_array_payload(payload);
  }
};

using ArrayPayloadPtr = std::unique_ptr<ArrayPayload, ArrayPayloadDeleter>;

/// Allocates a memory-accounted Arrow payload while retaining its pool.
inline ArrayPayloadPtr
make_array_payload(const std::shared_ptr<PoolResource> &pool) noexcept {
  if (!pool) {
    return {};
  }
  return ArrayPayloadPtr(new (std::nothrow) ArrayPayload(pool));
}

/// Releases an exported array payload and clears every Arrow C Data field.
inline void array_release(ArrowArray *array) {
  if (!array || !array->release)
    return;
  destroy_array_payload(static_cast<ArrayPayload *>(array->private_data));
  array->length = 0;
  array->null_count = 0;
  array->offset = 0;
  array->n_buffers = 0;
  array->n_children = 0;
  array->buffers = nullptr;
  array->children = nullptr;
  array->dictionary = nullptr;
  array->private_data = nullptr;
  array->release = nullptr;
}

/// Finalizes a child builder into a newly allocated Arrow array slot.
inline sanitize::Status finish_child_array(ColumnBuilder *builder,
                                           ArrowArray **slot,
                                           const char *oom_message) {
  if (!builder)
    return sanitize::Status::Invalid("finish_child_array: builder is null");
  if (!slot)
    return sanitize::Status::Invalid("finish_child_array: slot is null");
  std::unique_ptr<ArrowArray> child(new (std::nothrow) ArrowArray());
  if (!child)
    return sanitize::Status::OutOfMemory(oom_message);
  sanitize::Status st = builder->finish(child.get());
  if (!st.ok()) {
    cdata_stream::release_array_nothrow(child.get());
    return st;
  }
  *slot = child.release();
  return sanitize::Status::OK();
}

class BaseBuilder : public ColumnBuilder {
public:
  /// Initializes shared builder state for one compiled field and its
  /// memory-accounted Arrow buffers.
  explicit BaseBuilder(std::shared_ptr<PoolResource> pool)
      : pool_(std::move(pool)), validity_(pool_.get()) {}

  /// Resets row counts, validity bits, and builder-specific values.
  sanitize::Status reset() override {
    length_ = 0;
    null_count_ = 0;
    validity_.clear();
    return reset_values();
  }

  /// Returns the number of appended rows.
  [[nodiscard]] int64_t length() const noexcept override { return length_; }

protected:
  /// Resets values owned by a concrete builder.
  virtual sanitize::Status reset_values() = 0;

  /// Appends one validity bit and advances the row count, eliding all-valid
  /// bitmaps.
  void push_validity(bool valid) {
    const auto byte_index = static_cast<std::size_t>(length_ >> 3);
    const auto bit_mask = static_cast<uint8_t>(1u << (length_ & 7));
    if (validity_.empty()) {
      if (valid) {
        ++length_;
        return;
      }
      const auto byte_count = static_cast<std::size_t>((length_ >> 3) + 1);
      validity_.assign(byte_count, uint8_t{0xff});
    } else if ((length_ & 7) == 0) {
      validity_.push_back(0);
    }
    if (valid) {
      validity_[byte_index] |= bit_mask;
    } else {
      validity_[byte_index] &= static_cast<uint8_t>(~bit_mask);
      ++null_count_;
    }
    ++length_;
  }

  /// Appends validity bits from an Arrow array slice without copying physical
  /// values.
  sanitize::Status append_array_validity(const ArrowArray &array) {
    if (array.length < 0 || array.offset < 0) {
      return sanitize::Status::Invalid(
          "bulk Arrow append received negative length or offset");
    }
    const auto *validity = array.n_buffers > 0 && array.buffers
                               ? static_cast<const uint8_t *>(array.buffers[0])
                               : nullptr;
    for (int64_t index = 0; index < array.length; ++index) {
      const int64_t source_index = array.offset + index;
      const bool valid =
          !validity ||
          ((validity[static_cast<std::size_t>(source_index >> 3)] >>
            (source_index & 7)) &
           1u) != 0;
      push_validity(valid);
    }
    return sanitize::Status::OK();
  }

  /// Returns the validity bitmap when the array contains nulls.
  static const void *validity_buffer(const ArrayPayload *payload,
                                     int64_t null_count) noexcept {
    return (payload && null_count > 0 && !payload->validity.empty())
               ? static_cast<const void *>(payload->validity.data())
               : nullptr;
  }

  /// Initializes ArrowArray fields shared by all concrete builders.
  void init_common(ArrowArray *out, ArrayPayload *payload,
                   int64_t n_buffers) const {
    std::memset(out, 0, sizeof(*out));
    out->length = length_;
    out->null_count = null_count_;
    out->offset = 0;
    out->n_buffers = n_buffers;
    out->n_children = static_cast<int64_t>(payload->children.size());
    out->buffers = payload->buffers.data();
    out->children =
        payload->children.empty() ? nullptr : payload->children.data();
    out->dictionary = nullptr;
    out->private_data = payload;
    out->release = &array_release;
  }

  std::shared_ptr<PoolResource> pool_;
  int64_t length_ = 0;
  int64_t null_count_ = 0;
  std::pmr::vector<uint8_t> validity_;
};

template <typename BuilderT, typename... Args>
/// Allocates a concrete column builder without throwing on allocation failure.
std::unique_ptr<ColumnBuilder> make_column_builder(Args &&...args) {
  return std::unique_ptr<ColumnBuilder>(
      new (std::nothrow) BuilderT(std::forward<Args>(args)...));
}

/// Selects a physical scalar builder for the requested logical kind.
sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_scalar_builder(sanitize::LogicalKind kind,
                    const std::shared_ptr<PoolResource> &pool);

/// Constructs a struct builder that owns the supplied child builders.
sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_struct_builder(std::vector<std::unique_ptr<ColumnBuilder>> children,
                    const std::shared_ptr<PoolResource> &pool);

/// Constructs a list builder around its element builder.
sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_list_builder(std::unique_ptr<ColumnBuilder> child,
                  const std::shared_ptr<PoolResource> &pool);

} // namespace sanitize::internal

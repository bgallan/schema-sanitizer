// Shared private helpers for Arrow C Data array builders.

#pragma once

#include "internal/materialization/batch_appender_internal.hh"

#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <utility>
#include <vector>

#include "sanitize/core/logical_schema.hh"

namespace sanitize::internal {

struct ArrayPayload {
  std::vector<const void *> buffers;
  std::vector<uint8_t> validity;
  std::vector<uint8_t> bit_data;
  std::vector<int32_t> i32;
  std::vector<int64_t> i64;
  std::vector<double> f64;
  std::vector<int32_t> offsets;
  std::vector<char> bytes;
  std::vector<ArrowArray *> children;
};

// Destroys array payload.
inline void destroy_array_payload(ArrayPayload *payload) noexcept {
  if (!payload)
    return;
  for (ArrowArray *child : payload->children) {
    if (!child)
      continue;
    if (child->release)
      child->release(child);
    delete child;
  }
  delete payload;
}

struct ArrayPayloadDeleter {
  // Invokes the callable.
  void operator()(ArrayPayload *payload) const noexcept {
    destroy_array_payload(payload);
  }
};

using ArrayPayloadPtr = std::unique_ptr<ArrayPayload, ArrayPayloadDeleter>;

// Creates array payload.
inline ArrayPayloadPtr make_array_payload() noexcept {
  return ArrayPayloadPtr(new (std::nothrow) ArrayPayload());
}

// Performs the array release operation.
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

// Performs the finish child array operation.
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
    if (child->release)
      child->release(child.get());
    return st;
  }
  *slot = child.release();
  return sanitize::Status::OK();
}

class BaseBuilder : public ColumnBuilder {
public:
  // Resets row counts, validity bits, and builder-specific values.
  sanitize::Status reset() override {
    length_ = 0;
    null_count_ = 0;
    validity_.clear();
    return reset_values();
  }

  // Returns the number of appended rows.
  [[nodiscard]] int64_t length() const noexcept override { return length_; }

protected:
  // Resets values owned by a concrete builder.
  virtual sanitize::Status reset_values() = 0;

  // Appends one validity bit and advances the row count.
  void push_validity(bool valid) {
    if ((length_ & 7) == 0)
      validity_.push_back(0);
    if (valid)
      validity_[static_cast<std::size_t>(length_ >> 3)] |=
          static_cast<uint8_t>(1u << (length_ & 7));
    else
      ++null_count_;
    ++length_;
  }

  // Returns the validity bitmap when the array contains nulls.
  static const void *validity_buffer(const ArrayPayload *payload,
                                     int64_t null_count) noexcept {
    return (payload && null_count > 0 && !payload->validity.empty())
               ? static_cast<const void *>(payload->validity.data())
               : nullptr;
  }

  // Initializes ArrowArray fields shared by all concrete builders.
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

  int64_t length_ = 0;
  int64_t null_count_ = 0;
  std::vector<uint8_t> validity_;
};

template <typename BuilderT, typename... Args>
// Creates column builder.
std::unique_ptr<ColumnBuilder> make_column_builder(Args &&...args) {
  return std::unique_ptr<ColumnBuilder>(
      new (std::nothrow) BuilderT(std::forward<Args>(args)...));
}

// Creates scalar builder.
sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_scalar_builder(sanitize::LogicalKind kind);

// Creates struct builder.
sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_struct_builder(std::vector<std::unique_ptr<ColumnBuilder>> children);

// Creates list builder.
sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_list_builder(std::unique_ptr<ColumnBuilder> child);

} // namespace sanitize::internal

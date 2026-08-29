// Builds nested Arrow C Data arrays for struct and list cells.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#include "internal/materialization/builders/detail.hh"
#include "internal/memory/size_math.hh"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <utility>
#include <vector>

#include "internal/materialization/batch_appender_internal.hh"
#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

namespace sanitize::internal {
namespace {

using sanitize::Status;

class StructBuilder final : public BaseBuilder {
public:
  /// Takes ownership of struct child builders and shared validity storage.
  StructBuilder(std::vector<std::unique_ptr<ColumnBuilder>> children,
                std::shared_ptr<PoolResource> pool)
      : BaseBuilder(std::move(pool)), children_(std::move(children)) {}

  /// Clears struct validity and every child while retaining their storage.
  Status reset() override {
    SAN_RETURN_NOT_OK(BaseBuilder::reset());
    for (auto &child : children_)
      SAN_RETURN_NOT_OK(child->reset());
    return Status::OK();
  }

  /// Appends one struct validity bit and its child cells in schema order.
  Status append(const Cell &cell) override {
    if (cell.is_null)
      return append_null();
    if (cell.children.size() != children_.size())
      return Status::Invalid("struct cell child count mismatch");
    push_validity(true);
    for (std::size_t i = 0; i < children_.size(); ++i)
      SAN_RETURN_NOT_OK(children_[i]->append(cell.children[i]));
    return Status::OK();
  }

  /// Appends a null struct and aligned null entries to every child column.
  Status append_null() override {
    push_validity(false);
    for (auto &child : children_)
      SAN_RETURN_NOT_OK(child->append_null());
    return Status::OK();
  }

  /// Finalizes validity and child columns into a struct Arrow array.
  Status finish(ArrowArray *out) override {
    auto payload = make_array_payload(pool_);
    if (!payload)
      return Status::OutOfMemory("StructBuilder::finish: OOM payload");
    payload->validity = std::move(validity_);
    payload->buffers.resize(1);
    payload->buffers[0] = validity_buffer(payload.get(), null_count_);
    payload->children.resize(children_.size(), nullptr);
    for (std::size_t i = 0; i < children_.size(); ++i) {
      SAN_RETURN_NOT_OK(finish_child_array(children_[i].get(),
                                           &payload->children[i],
                                           "StructBuilder::finish: OOM child"));
    }
    init_common(out, payload.get(), 1);
    out->private_data = payload.release();
    return Status::OK();
  }

  /// Returns retained buffer capacity in bytes for validity and child builders.
  [[nodiscard]] int64_t bytes() const noexcept override {
    auto total = saturating_size_to_i64(validity_.capacity());
    for (const auto &child : children_) {
      total = saturating_add_i64(total, child->bytes());
    }
    return total;
  }

protected:
  /// Performs no extra reset because StructBuilder::reset clears every child.
  Status reset_values() override { return Status::OK(); }

private:
  std::vector<std::unique_ptr<ColumnBuilder>> children_;
};

class ListBuilder final : public BaseBuilder {
public:
  /// Takes ownership of an element builder and initializes the zero offset.
  ListBuilder(std::unique_ptr<ColumnBuilder> child,
              std::shared_ptr<PoolResource> pool)
      : BaseBuilder(std::move(pool)), child_(std::move(child)),
        offsets_(pool_.get()) {
    offsets_.push_back(0);
  }

  /// Restores the zero offset and clears child values and validity.
  Status reset() override {
    SAN_RETURN_NOT_OK(BaseBuilder::reset());
    offsets_.clear();
    offsets_.push_back(0);
    if (child_)
      SAN_RETURN_NOT_OK(child_->reset());
    return Status::OK();
  }

  /// Appends one list's elements, terminal offset, and validity bit.
  Status append(const Cell &cell) override {
    if (cell.is_null)
      return append_null();
    if (!child_)
      return Status::Invalid("list builder has no child");
    for (const Cell &element : cell.elements)
      SAN_RETURN_NOT_OK(child_->append(element));
    if (child_->length() > std::numeric_limits<int32_t>::max())
      return Status::Invalid("list child length exceeds int32 offset range");
    offsets_.push_back(static_cast<int32_t>(child_->length()));
    push_validity(true);
    return Status::OK();
  }

  /// Appends a null list by repeating the previous child offset.
  Status append_null() override {
    offsets_.push_back(offsets_.empty() ? 0 : offsets_.back());
    push_validity(false);
    return Status::OK();
  }

  /// Finalizes offsets, validity, and element values into a list Arrow array.
  Status finish(ArrowArray *out) override {
    auto payload = make_array_payload(pool_);
    if (!payload)
      return Status::OutOfMemory("ListBuilder::finish: OOM payload");
    payload->validity = std::move(validity_);
    payload->offsets = std::move(offsets_);
    payload->buffers.resize(2);
    payload->buffers[0] = validity_buffer(payload.get(), null_count_);
    payload->buffers[1] =
        payload->offsets.empty() ? nullptr : payload->offsets.data();
    payload->children.resize(1, nullptr);
    SAN_RETURN_NOT_OK(finish_child_array(child_.get(), payload->children.data(),
                                         "ListBuilder::finish: OOM child"));
    init_common(out, payload.get(), 2);
    out->private_data = payload.release();
    return Status::OK();
  }

  /// Returns retained buffer capacity in bytes for list metadata and child
  /// values.
  [[nodiscard]] int64_t bytes() const noexcept override {
    auto total = saturating_size_to_i64(validity_.capacity());
    total = saturating_add_i64(
        total, saturating_capacity_bytes(offsets_.capacity(), sizeof(int32_t)));
    return saturating_add_i64(total, child_ ? child_->bytes() : 0);
  }

protected:
  /// Performs no extra reset because ListBuilder::reset clears offsets and
  /// child values.
  Status reset_values() override { return Status::OK(); }

private:
  std::unique_ptr<ColumnBuilder> child_;
  std::pmr::vector<int32_t> offsets_;
};

} // namespace

sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_struct_builder(std::vector<std::unique_ptr<ColumnBuilder>> children,
                    const std::shared_ptr<PoolResource> &pool) {
  auto builder = make_column_builder<StructBuilder>(std::move(children), pool);
  if (!builder)
    return Status::OutOfMemory("make_struct_builder: OOM builder");
  return builder;
}

sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_list_builder(std::unique_ptr<ColumnBuilder> child,
                  const std::shared_ptr<PoolResource> &pool) {
  auto builder = make_column_builder<ListBuilder>(std::move(child), pool);
  if (!builder)
    return Status::OutOfMemory("make_list_builder: OOM builder");
  return builder;
}

} // namespace sanitize::internal

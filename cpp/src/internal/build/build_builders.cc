// Root Arrow C Data builder and recursive builder factory.

#include "internal/build/build_builder_detail.hh"

#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

namespace sanitize::internal {
namespace {

using sanitize::ColumnPlan;
using sanitize::LogicalKind;
using sanitize::Status;

// Creates builder.
sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_builder(const ColumnPlan &plan) {
  switch (plan.logical_type.kind) {
  case LogicalKind::kNull:
  case LogicalKind::kBool:
  case LogicalKind::kInt64:
  case LogicalKind::kFloat64:
  case LogicalKind::kUtf8:
  case LogicalKind::kTimestampNs:
  case LogicalKind::kDate32:
  case LogicalKind::kTime32s:
    return make_scalar_builder(plan.logical_type.kind);
  case LogicalKind::kStruct: {
    std::vector<std::unique_ptr<ColumnBuilder>> children;
    children.reserve(plan.children.size());
    for (const auto &child_plan : plan.children) {
      SAN_ASSIGN_OR_RAISE(auto child, make_builder(child_plan));
      if (!child)
        return Status::OutOfMemory("make_builder: OOM child builder");
      children.push_back(std::move(child));
    }
    return make_struct_builder(std::move(children));
  }
  case LogicalKind::kList: {
    if (!plan.value)
      return Status::Invalid("make_builder: list plan has no child");
    SAN_ASSIGN_OR_RAISE(auto child, make_builder(*plan.value));
    if (!child)
      return Status::OutOfMemory("make_builder: OOM list child builder");
    return make_list_builder(std::move(child));
  }
  }
  return Status::Invalid("make_builder: unsupported logical kind");
}

class RootStructBuilder final : public ColumnBuilder {
public:
  // Creates a RootStructBuilder.
  explicit RootStructBuilder(
      std::vector<std::unique_ptr<ColumnBuilder>> children)
      : children_(std::move(children)) {}

  // Resets the object state.
  Status reset() override {
    length_ = 0;
    for (auto &child : children_)
      SAN_RETURN_NOT_OK(child->reset());
    return Status::OK();
  }

  // Appends the object state.
  Status append(const Cell &cell) override {
    if (cell.children.size() != children_.size())
      return Status::Invalid("root cell child count mismatch");
    for (std::size_t i = 0; i < children_.size(); ++i)
      SAN_RETURN_NOT_OK(children_[i]->append(cell.children[i]));
    ++length_;
    return Status::OK();
  }

  // Appends null.
  Status append_null() override {
    for (auto &child : children_)
      SAN_RETURN_NOT_OK(child->append_null());
    ++length_;
    return Status::OK();
  }

  // Finishes the current output.
  Status finish(ArrowArray *out) override {
    auto payload = make_array_payload();
    if (!payload)
      return Status::OutOfMemory("RootStructBuilder::finish: OOM payload");
    payload->buffers.assign(1, nullptr);
    payload->children.resize(children_.size(), nullptr);
    for (std::size_t i = 0; i < children_.size(); ++i) {
      SAN_RETURN_NOT_OK(
          finish_child_array(children_[i].get(), &payload->children[i],
                             "RootStructBuilder::finish: OOM child"));
    }
    std::memset(out, 0, sizeof(*out));
    out->length = length_;
    out->null_count = 0;
    out->offset = 0;
    out->n_buffers = 1;
    out->n_children = static_cast<int64_t>(payload->children.size());
    out->buffers = payload->buffers.data();
    out->children =
        payload->children.empty() ? nullptr : payload->children.data();
    out->dictionary = nullptr;
    out->private_data = payload.release();
    out->release = &array_release;
    return Status::OK();
  }

  // Returns the current row count.
  [[nodiscard]] int64_t length() const noexcept override { return length_; }

  // Returns the current byte usage.
  [[nodiscard]] int64_t bytes() const noexcept override {
    int64_t total = 0;
    for (const auto &child : children_)
      total += child->bytes();
    return total;
  }

private:
  std::vector<std::unique_ptr<ColumnBuilder>> children_;
  int64_t length_ = 0;
};

} // namespace

sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_root_builder(const sanitize::CompiledPlan &plan) {
  std::vector<std::unique_ptr<ColumnBuilder>> builders;
  builders.reserve(plan.columns.size());
  for (const auto &column : plan.columns) {
    SAN_ASSIGN_OR_RAISE(auto builder, make_builder(column));
    if (!builder)
      return Status::OutOfMemory("make_root_builder: OOM child builder");
    builders.push_back(std::move(builder));
  }

  auto root = make_column_builder<RootStructBuilder>(std::move(builders));
  if (!root)
    return Status::OutOfMemory("make_root_builder: OOM root builder");
  SAN_RETURN_NOT_OK(root->reset());
  return root;
}

} // namespace sanitize::internal

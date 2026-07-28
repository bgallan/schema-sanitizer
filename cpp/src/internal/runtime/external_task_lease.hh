// Publishes abandonment when a move-only arena task is destroyed before use.
#pragma once

#include <cstddef>

namespace sanitize::internal {

// The owner type and abandonment member are part of the lease type. Every
// OrderedExecutor task therefore keeps a directly typed owner pointer and
// needs neither a stored callback nor a void* thunk/cast on abandonment.
template <class Owner, void (Owner::*Abandon)(std::size_t) noexcept>
class ExternalTaskLease final {
  static_assert(Abandon != nullptr);

public:
  ExternalTaskLease(Owner *owner, std::size_t shard) noexcept
      : owner_(owner), shard_(shard) {}
  ExternalTaskLease(const ExternalTaskLease &) = delete;
  ExternalTaskLease &operator=(const ExternalTaskLease &) = delete;
  ExternalTaskLease(ExternalTaskLease &&other) noexcept
      : owner_(other.owner_), shard_(other.shard_) {
    // owner_ is the sole ownership sentinel. The shard is immutable payload and
    // is never observed after ownership is cleared.
    other.owner_ = nullptr;
  }
  ExternalTaskLease &operator=(ExternalTaskLease &&) = delete;
  ~ExternalTaskLease() {
    if (owner_) {
      (owner_->*Abandon)(shard_);
    }
  }

  [[nodiscard]] std::size_t shard() const noexcept { return shard_; }
  void Complete() noexcept { owner_ = nullptr; }

private:
  Owner *owner_ = nullptr;
  std::size_t shard_ = 0;
};

} // namespace sanitize::internal

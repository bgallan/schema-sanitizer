// Provides modulo-free bounded completion-ring cursors for ordered stages.
#pragma once

#include <cstddef>
#include <utility>

namespace sanitize::internal {

// Carries one admitted packet and the completion slot reserved for its ordinal.
template <class Packet> struct ScheduledOrdinalPacket final {
  Packet packet;
  std::size_t completion_slot = 0;
};

// Submission and consumption are each single-coordinator operations protected
// by OrderedExecutor's mutex, so a branch-based ring avoids repeated division.
class CompletionRingCursor final {
public:
  explicit CompletionRingCursor(std::size_t capacity) noexcept
      : capacity_(capacity) {}

  [[nodiscard]] std::size_t ReserveSubmit() noexcept {
    const auto slot = next_submit_;
    if (++next_submit_ == capacity_) {
      next_submit_ = 0;
    }
    return slot;
  }

  void RollbackSubmit() noexcept {
    next_submit_ = next_submit_ == 0 ? capacity_ - 1 : next_submit_ - 1;
  }

  [[nodiscard]] std::size_t NextTake() const noexcept { return next_take_; }

  void AdvanceTake() noexcept {
    if (++next_take_ == capacity_) {
      next_take_ = 0;
    }
  }

private:
  const std::size_t capacity_;
  std::size_t next_submit_ = 0;
  std::size_t next_take_ = 0;
};

} // namespace sanitize::internal

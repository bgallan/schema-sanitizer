// Shared process-wide file-descriptor admission used by Python and native I/O.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace sanitize::internal {

[[nodiscard]] std::size_t
acquire_process_file_descriptor_permits(std::size_t desired,
                                        std::size_t minimum) noexcept;
[[nodiscard]] std::size_t acquire_process_file_descriptor_permits_wait(
    std::size_t desired, std::size_t minimum,
    std::uint64_t timeout_millis) noexcept;
void release_process_file_descriptor_permits(std::size_t amount) noexcept;
void mark_process_file_descriptors_opened(std::size_t amount) noexcept;
void mark_process_file_descriptors_closed(std::size_t amount) noexcept;
[[nodiscard]] std::size_t process_file_descriptor_permits_in_use() noexcept;
[[nodiscard]] std::size_t process_file_descriptors_opened() noexcept;
[[nodiscard]] std::optional<std::size_t>
process_file_descriptor_count() noexcept;
[[nodiscard]] std::size_t process_file_descriptor_rejections() noexcept;
void record_process_file_descriptor_protocol_violation() noexcept;
void record_process_file_descriptor_uncertain_close_debt(
    std::size_t amount) noexcept;
[[nodiscard]] std::size_t
process_file_descriptor_protocol_violations() noexcept;
[[nodiscard]] std::size_t
process_file_descriptor_uncertain_close_debts() noexcept;
[[nodiscard]] std::size_t process_file_descriptor_capacity() noexcept;

class ProcessFdPermitLease final {
public:
  ProcessFdPermitLease() noexcept = default;
  explicit ProcessFdPermitLease(std::size_t amount,
                                std::uint64_t timeout_millis = 30000U) noexcept
      : amount_(acquire_process_file_descriptor_permits_wait(amount, amount,
                                                             timeout_millis)) {}

  ProcessFdPermitLease(const ProcessFdPermitLease &) = delete;
  ProcessFdPermitLease &operator=(const ProcessFdPermitLease &) = delete;
  ProcessFdPermitLease(ProcessFdPermitLease &&other) noexcept
      : amount_(std::exchange(other.amount_, 0U)),
        opened_(std::exchange(other.opened_, 0U)),
        protocol_violation_(std::exchange(other.protocol_violation_, false)) {}
  ProcessFdPermitLease &operator=(ProcessFdPermitLease &&other) noexcept {
    if (this != &other) {
      reset();
      amount_ = std::exchange(other.amount_, 0U);
      opened_ = std::exchange(other.opened_, 0U);
      protocol_violation_ = std::exchange(other.protocol_violation_, false);
    }
    return *this;
  }
  ~ProcessFdPermitLease() noexcept { reset(); }

  [[nodiscard]] explicit operator bool() const noexcept {
    return amount_ != 0U;
  }
  [[nodiscard]] std::size_t amount() const noexcept { return amount_; }
  [[nodiscard]] std::size_t opened() const noexcept { return opened_; }
  [[nodiscard]] static ProcessFdPermitLease
  TryAcquireUpTo(std::size_t desired, std::size_t minimum = 1U) noexcept {
    return ProcessFdPermitLease(
        acquire_process_file_descriptor_permits(desired, minimum), AdoptTag{});
  }
  [[nodiscard]] static ProcessFdPermitLease
  TryAcquireUpToWait(std::size_t desired, std::size_t minimum,
                     std::uint64_t timeout_millis) noexcept {
    return ProcessFdPermitLease(acquire_process_file_descriptor_permits_wait(
                                    desired, minimum, timeout_millis),
                                AdoptTag{});
  }
  [[nodiscard]] bool shrink(std::size_t target) noexcept {
    if (target > amount_ || target < opened_) {
      return false;
    }
    if (target < amount_) {
      const auto returned = amount_ - target;
      // Publish reduced exact authority before returning aggregate capacity so
      // retries after an asynchronous unwind are idempotent.
      amount_ = target;
      release_process_file_descriptor_permits(returned);
    }
    return true;
  }
  [[nodiscard]] ProcessFdPermitLease split(std::size_t amount = 1U) noexcept {
    const auto unopened = amount_ > opened_ ? amount_ - opened_ : 0U;
    const auto transferred = std::min(amount, unopened);
    amount_ -= transferred;
    return ProcessFdPermitLease(transferred, AdoptTag{});
  }
  void mark_opened(std::size_t amount = 1U) noexcept {
    const auto available = amount_ > opened_ ? amount_ - opened_ : 0U;
    const auto delta = amount < available ? amount : available;
    if (delta != 0U) {
      mark_process_file_descriptors_opened(delta);
      opened_ += delta;
    }
    if (amount > available) {
      // A caller reached the kernel without enough logical authority. Never
      // treat this as success: latch a protocol violation and make destruction
      // fail closed for the capacity we still own.
      protocol_violation_ = true;
      record_process_file_descriptor_protocol_violation();
    }
  }
  void mark_closed(std::size_t amount = 1U) noexcept {
    const auto delta = amount < opened_ ? amount : opened_;
    if (delta != 0U) {
      mark_process_file_descriptors_closed(delta);
      opened_ -= delta;
    }
  }
  void retain_uncertain_close() noexcept {
    // Preserve explicit telemetry before dropping local identity. The global
    // governor remains charged; this counter makes irreversible native debt
    // observable instead of silently shrinking capacity forever.
    record_process_file_descriptor_uncertain_close_debt(amount_);
    amount_ = 0U;
    opened_ = 0U;
    protocol_violation_ = false;
  }
  void commit_physical_close(bool proven_closed,
                             std::size_t amount = 1U) noexcept {
    if (proven_closed) {
      mark_closed(amount);
    } else {
      retain_uncertain_close();
    }
  }
  void reset() noexcept {
    if (opened_ != 0U) {
      // Fail closed: reset/destruction is not proof that the kernel descriptor
      // is gone.  Any caller that forgot commit_physical_close() must retain
      // both physical-open accounting and logical capacity as terminal debt.
      retain_uncertain_close();
      return;
    }
    if (protocol_violation_) {
      retain_uncertain_close();
      return;
    }
    if (amount_ != 0U) {
      release_process_file_descriptor_permits(amount_);
      amount_ = 0U;
    }
  }

private:
  struct AdoptTag final {};
  ProcessFdPermitLease(std::size_t amount, AdoptTag) noexcept
      : amount_(amount) {}

  std::size_t amount_ = 0U;
  std::size_t opened_ = 0U;
  bool protocol_violation_ = false;
};

template <typename Stream>
void close_stream_and_commit(Stream &stream,
                             ProcessFdPermitLease &lease) noexcept {
  if (lease.opened() == 0U) {
    return;
  }
  // Stream fail/eof bits may predate close(), so they cannot prove whether the
  // underlying descriptor is still associated. is_open() is the physical
  // ownership signal we need after the close attempt (including exceptions).
  bool proven_closed = !stream.is_open();
  try {
    if (stream.is_open()) {
      stream.close();
    }
    proven_closed = !stream.is_open();
  } catch (...) {
    proven_closed = !stream.is_open();
  }
  lease.commit_physical_close(proven_closed);
}

template <typename Stream> class ProcessFdStreamCloseGuard final {
public:
  ProcessFdStreamCloseGuard(Stream &stream,
                            ProcessFdPermitLease &lease) noexcept
      : stream_(&stream), lease_(&lease) {}
  ProcessFdStreamCloseGuard(const ProcessFdStreamCloseGuard &) = delete;
  ProcessFdStreamCloseGuard &
  operator=(const ProcessFdStreamCloseGuard &) = delete;
  ~ProcessFdStreamCloseGuard() noexcept {
    if (stream_ != nullptr && lease_ != nullptr) {
      close_stream_and_commit(*stream_, *lease_);
    }
  }

private:
  Stream *stream_;
  ProcessFdPermitLease *lease_;
};

} // namespace sanitize::internal

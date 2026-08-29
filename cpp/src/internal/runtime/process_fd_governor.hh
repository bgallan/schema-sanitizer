// Provides process-wide file-descriptor admission for Python and native
// I/O. Move-only leases preserve capacity through open, close, and
// uncertain-close paths.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace sanitize::internal {

/// Acquires immediately available process file-descriptor permits.
[[nodiscard]] std::size_t
acquire_process_file_descriptor_permits(std::size_t desired,
                                        std::size_t minimum) noexcept;
/// Waits fairly for descriptor permits until the supplied timeout.
[[nodiscard]] std::size_t acquire_process_file_descriptor_permits_wait(
    std::size_t desired, std::size_t minimum,
    std::uint64_t timeout_millis) noexcept;
/// Returns descriptor permits and wakes queued admission waiters.
void release_process_file_descriptor_permits(std::size_t amount) noexcept;
/// Moves acquired permits into the physically opened descriptor count.
void mark_process_file_descriptors_opened(std::size_t amount) noexcept;
/// Retires physically closed descriptors from process accounting.
void mark_process_file_descriptors_closed(std::size_t amount) noexcept;
/// Returns permits currently held or converted into open descriptors.
[[nodiscard]] std::size_t process_file_descriptor_permits_in_use() noexcept;
/// Returns the number of queued descriptor admission waiters.
[[nodiscard]] std::size_t process_file_descriptor_waiters() noexcept;
/// Returns descriptors tracked as physically open by the governor.
[[nodiscard]] std::size_t process_file_descriptors_opened() noexcept;
/// Returns the operating system's current process descriptor count.
[[nodiscard]] std::optional<std::size_t>
process_file_descriptor_count() noexcept;
/// Returns descriptor admissions rejected for capacity or queue pressure.
[[nodiscard]] std::size_t process_file_descriptor_rejections() noexcept;
/// Records a descriptor lease accounting protocol violation.
void record_process_file_descriptor_protocol_violation() noexcept;
/// Records descriptors whose physical close could not be proven.
void record_process_file_descriptor_uncertain_close_debt(
    std::size_t amount) noexcept;
/// Returns the descriptor accounting protocol-violation count.
[[nodiscard]] std::size_t
process_file_descriptor_protocol_violations() noexcept;
/// Returns accumulated uncertain descriptor-close debt.
[[nodiscard]] std::size_t
process_file_descriptor_uncertain_close_debts() noexcept;
/// Returns the current process-wide descriptor permit ceiling.
[[nodiscard]] std::size_t process_file_descriptor_capacity() noexcept;

class ProcessFdPermitLease final {
public:
  /// Creates an empty file-descriptor permit lease.
  ProcessFdPermitLease() noexcept = default;
  /// Waits for and owns a descriptor permit range meeting the minimum.
  explicit ProcessFdPermitLease(std::size_t amount,
                                std::uint64_t timeout_millis = 30000U) noexcept
      : amount_(acquire_process_file_descriptor_permits_wait(amount, amount,
                                                             timeout_millis)) {}

  /// Disables copying the file-descriptor permit lease.
  ProcessFdPermitLease(const ProcessFdPermitLease &) = delete;
  /// Disables copy assignment for the file-descriptor permit lease.
  ProcessFdPermitLease &operator=(const ProcessFdPermitLease &) = delete;
  /// Transfers ownership from another file-descriptor permit lease.
  ProcessFdPermitLease(ProcessFdPermitLease &&other) noexcept
      : amount_(std::exchange(other.amount_, 0U)),
        opened_(std::exchange(other.opened_, 0U)),
        protocol_violation_(std::exchange(other.protocol_violation_, false)) {}
  /// Transfers owned state from another file-descriptor permit lease.
  ProcessFdPermitLease &operator=(ProcessFdPermitLease &&other) noexcept {
    if (this != &other) {
      reset();
      amount_ = std::exchange(other.amount_, 0U);
      opened_ = std::exchange(other.opened_, 0U);
      protocol_violation_ = std::exchange(other.protocol_violation_, false);
    }
    return *this;
  }
  /// Releases unopened permits while preserving uncertain close debt.
  ~ProcessFdPermitLease() noexcept { reset(); }

  /// Reports whether this lease currently owns a nonzero reservation.
  [[nodiscard]] explicit operator bool() const noexcept {
    return amount_ != 0U;
  }
  /// Returns descriptor permits currently owned by this lease.
  [[nodiscard]] std::size_t amount() const noexcept { return amount_; }
  /// Returns owned permits already converted into physical descriptors.
  [[nodiscard]] std::size_t opened() const noexcept { return opened_; }
  /// Acquires immediately available descriptor permits above a
  /// required minimum.
  [[nodiscard]] static ProcessFdPermitLease
  TryAcquireUpTo(std::size_t desired, std::size_t minimum = 1U) noexcept {
    return ProcessFdPermitLease(
        acquire_process_file_descriptor_permits(desired, minimum), AdoptTag{});
  }
  /// Waits fairly for descriptor permits within the supplied timeout.
  [[nodiscard]] static ProcessFdPermitLease
  TryAcquireUpToWait(std::size_t desired, std::size_t minimum,
                     std::uint64_t timeout_millis) noexcept {
    return ProcessFdPermitLease(acquire_process_file_descriptor_permits_wait(
                                    desired, minimum, timeout_millis),
                                AdoptTag{});
  }
  /// Returns unused descriptor permits above the target lease size.
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
  /// Moves a requested number of permits into an independent lease.
  [[nodiscard]] ProcessFdPermitLease split(std::size_t amount = 1U) noexcept {
    const auto unopened = amount_ > opened_ ? amount_ - opened_ : 0U;
    const auto transferred = std::min(amount, unopened);
    amount_ -= transferred;
    return ProcessFdPermitLease(transferred, AdoptTag{});
  }
  /// Converts owned permits into tracked physically open descriptors.
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
  /// Commits proven descriptor closures and retires their permits.
  void mark_closed(std::size_t amount = 1U) noexcept {
    const auto delta = amount < opened_ ? amount : opened_;
    if (delta != 0U) {
      mark_process_file_descriptors_closed(delta);
      opened_ -= delta;
    }
  }
  /// Keeps permits charged when physical descriptor closure is uncertain.
  void retain_uncertain_close() noexcept {
    // Preserve explicit telemetry before dropping local identity. The global
    // governor remains charged; this counter makes irreversible native debt
    // observable instead of silently shrinking capacity forever.
    record_process_file_descriptor_uncertain_close_debt(amount_);
    amount_ = 0U;
    opened_ = 0U;
    protocol_violation_ = false;
  }
  /// Commits or retains descriptor charges according to proven
  /// close status.
  void commit_physical_close(bool proven_closed,
                             std::size_t amount = 1U) noexcept {
    if (proven_closed) {
      mark_closed(amount);
    } else {
      retain_uncertain_close();
    }
  }
  /// Releases unopened permits while preserving uncertain
  /// physical-close debt.
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
  /// Adopts permits already acquired from the process descriptor governor.
  ProcessFdPermitLease(std::size_t amount, AdoptTag) noexcept
      : amount_(amount) {}

  std::size_t amount_ = 0U;
  std::size_t opened_ = 0U;
  bool protocol_violation_ = false;
};

/// Closes a stream and commits descriptor accounting only when closure
/// is proven.
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
  /// Couples a stream's scope exit to descriptor-close accounting.
  ProcessFdStreamCloseGuard(Stream &stream,
                            ProcessFdPermitLease &lease) noexcept
      : stream_(&stream), lease_(&lease) {}
  /// Disables copying the stream-close accounting guard.
  ProcessFdStreamCloseGuard(const ProcessFdStreamCloseGuard &) = delete;
  /// Disables copy assignment for the stream-close accounting guard.
  ProcessFdStreamCloseGuard &
  operator=(const ProcessFdStreamCloseGuard &) = delete;
  /// Closes the stream and commits descriptor accounting on scope exit.
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

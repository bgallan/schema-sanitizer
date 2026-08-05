// Provides hardened process-wide and operation-scoped memory pools.

#include "internal/memory/memory_pool.hh"
#include "internal/memory/memory_budget.hh"

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "sanitize/core/status.hh"

namespace sanitize::internal {

OperationMemoryLedger::OperationMemoryLedger(int64_t limit_bytes) noexcept
    : limit_bytes_(std::max<int64_t>(1, limit_bytes)) {
  (void)shared_process_memory_pool(0);
}

sanitize::Status OperationMemoryLedger::ReserveLocal(int64_t bytes,
                                                     std::string_view stage) {
  if (bytes < 0) {
    return sanitize::Status::Invalid(
        "operation memory reservation is negative");
  }
  if (bytes == 0) {
    return sanitize::Status::OK();
  }
  auto current = bytes_reserved_.load(std::memory_order_relaxed);
  for (;;) {
    if (current > std::numeric_limits<int64_t>::max() - bytes) {
      return sanitize::Status::OutOfMemory(
          "memory_limit_bytes accounting overflow");
    }
    const auto next = current + bytes;
    if (next > limit_bytes_) {
      if (stage.empty()) {
        return sanitize::Status::OutOfMemory(
            "memory_limit_bytes limit exceeded: ", next, " bytes > ",
            limit_bytes_, " bytes");
      }
      return sanitize::Status::OutOfMemory(
          "memory_limit_bytes limit exceeded during ", stage, ": ", next,
          " bytes > ", limit_bytes_, " bytes");
    }
    if (bytes_reserved_.compare_exchange_weak(current, next,
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
      auto peak = peak_bytes_reserved_.load(std::memory_order_relaxed);
      while (next > peak && !peak_bytes_reserved_.compare_exchange_weak(
                                peak, next, std::memory_order_relaxed,
                                std::memory_order_relaxed)) {
      }
      return sanitize::Status::OK();
    }
  }
}

int64_t OperationMemoryLedger::ReleaseLocal(int64_t bytes) noexcept {
  if (bytes <= 0) {
    return 0;
  }
  auto current = bytes_reserved_.load(std::memory_order_relaxed);
  for (;;) {
    const auto next = std::max<int64_t>(0, current - bytes);
    if (bytes_reserved_.compare_exchange_weak(current, next,
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
      if (bytes > current) {
        over_release_count_.fetch_add(1, std::memory_order_relaxed);
        over_release_bytes_.fetch_add(bytes - current,
                                      std::memory_order_relaxed);
      }
      return current - next;
    }
  }
}

sanitize::Status OperationMemoryLedger::Reserve(int64_t bytes,
                                                std::string_view stage) {
  auto local_status = ReserveLocal(bytes, stage);
  if (!local_status.ok() || bytes == 0) {
    return local_status;
  }
  auto process_pool = shared_process_memory_pool(0);
  auto process_status = process_pool->ReserveExternal(bytes, stage);
  if (!process_status.ok()) {
    (void)ReleaseLocal(bytes);
    return process_status;
  }
  return sanitize::Status::OK();
}

void OperationMemoryLedger::Release(int64_t bytes) noexcept {
  const auto released = ReleaseLocal(bytes);
  if (released <= 0) {
    return;
  }
  shared_process_memory_pool(0)->ReleaseExternal(released);
}

sanitize::Status OperationMemoryLedger::ReserveNative(int64_t bytes,
                                                      std::string_view stage) {
  return ReserveLocal(bytes, stage);
}

void OperationMemoryLedger::ReleaseNative(int64_t bytes) noexcept {
  (void)ReleaseLocal(bytes);
}

int64_t OperationMemoryLedger::limit_bytes() const noexcept {
  return limit_bytes_;
}

int64_t OperationMemoryLedger::bytes_reserved() const noexcept {
  return bytes_reserved_.load(std::memory_order_relaxed);
}

int64_t OperationMemoryLedger::peak_bytes_reserved() const noexcept {
  return peak_bytes_reserved_.load(std::memory_order_relaxed);
}

int64_t OperationMemoryLedger::over_release_count() const noexcept {
  return over_release_count_.load(std::memory_order_relaxed);
}

int64_t OperationMemoryLedger::over_release_bytes() const noexcept {
  return over_release_bytes_.load(std::memory_order_relaxed);
}

std::shared_ptr<OperationMemoryLedger>
make_operation_memory_ledger(int64_t limit_bytes) {
  return std::make_shared<OperationMemoryLedger>(limit_bytes);
}

namespace {

constexpr std::uint64_t kDefaultAllocationMagic = 0x53414E4D454D3031ULL;
constexpr std::uint64_t kTrackingAllocationMagic = 0x53414E4D454D3032ULL;
constexpr std::size_t kAllocationGuardBytes = 16;
constexpr std::uint8_t kAllocationGuardPattern = 0xA5;

#include "internal/memory/memory_pool_registry.cc.inc"

struct alignas(std::max_align_t) DefaultAllocationHeader {
  void *raw = nullptr;
  std::uint64_t magic = kDefaultAllocationMagic;
  std::uint64_t reserved = 0;
  std::int64_t size = 0;
  std::size_t alignment = 0;
};

struct alignas(std::max_align_t) TrackingAllocationHeader {
  std::uint8_t *upstream = nullptr;
  std::uint64_t magic = kTrackingAllocationMagic;
  std::int64_t upstream_size = 0;
  std::int64_t requested_size = 0;
  std::int64_t upstream_alignment = 0;
};

static_assert(sizeof(DefaultAllocationHeader) % alignof(std::max_align_t) == 0);
static_assert(sizeof(TrackingAllocationHeader) % alignof(std::max_align_t) ==
              0);

bool normalize_alignment(int64_t alignment, std::size_t *out) noexcept {
  if (!out) {
    return false;
  }
  if (alignment <= 0) {
    *out = alignof(std::max_align_t);
    return true;
  }
  if (static_cast<std::uint64_t>(alignment) >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return false;
  }
  auto a = static_cast<std::size_t>(alignment);
  if ((a & (a - 1)) != 0) {
    const int digits = std::numeric_limits<std::size_t>::digits;
    const std::size_t max_power = std::size_t{1} << (digits - 1);
    if (a > max_power) {
      return false;
    }
    std::size_t p = 1;
    while (p < a) {
      p <<= 1;
    }
    a = p;
  }
  *out = std::max<std::size_t>(a, alignof(std::max_align_t));
  return true;
}

bool checked_allocation_size(std::size_t requested, std::size_t alignment,
                             std::size_t header_size,
                             std::size_t *total) noexcept {
  if (!total || alignment == 0 ||
      requested > std::numeric_limits<std::size_t>::max() - header_size) {
    return false;
  }
  const auto with_header = requested + header_size;
  if (with_header >
          std::numeric_limits<std::size_t>::max() - (alignment - 1U) ||
      with_header + (alignment - 1U) >
          std::numeric_limits<std::size_t>::max() - kAllocationGuardBytes) {
    return false;
  }
  *total = with_header + alignment - 1U + kAllocationGuardBytes;
  return true;
}

void write_allocation_guard(std::uint8_t *buffer, std::size_t size) noexcept {
  std::memset(buffer + size, kAllocationGuardPattern, kAllocationGuardBytes);
}

bool allocation_guard_is_valid(const std::uint8_t *buffer,
                               std::size_t size) noexcept {
  for (std::size_t index = 0; index < kAllocationGuardBytes; ++index) {
    if (buffer[size + index] != kAllocationGuardPattern) {
      return false;
    }
  }
  return true;
}

std::uintptr_t align_up(std::uintptr_t value, std::size_t alignment) noexcept {
  return (value + alignment - 1U) &
         ~(static_cast<std::uintptr_t>(alignment) - 1U);
}

void update_peak(std::atomic<int64_t> *peak, int64_t current) noexcept {
  auto observed = peak->load(std::memory_order_relaxed);
  while (current > observed && !peak->compare_exchange_weak(
                                   observed, current, std::memory_order_relaxed,
                                   std::memory_order_relaxed)) {
  }
}

class DefaultMemoryPool final : public MemoryPool {
public:
  sanitize::Status Allocate(int64_t size, int64_t alignment,
                            uint8_t **out) override {
    if (!out) {
      return sanitize::Status::Invalid("Allocate: out is null");
    }
    *out = nullptr;
    if (size < 0) {
      return sanitize::Status::Invalid("Allocate: negative size");
    }
    if (size == 0) {
      return sanitize::Status::OK();
    }
    if (static_cast<std::uint64_t>(size) >
        static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
      return sanitize::Status::OutOfMemory("Allocate: size out of range");
    }

    std::size_t normalized_alignment = alignof(std::max_align_t);
    if (!normalize_alignment(alignment, &normalized_alignment)) {
      return sanitize::Status::OutOfMemory("Allocate: alignment out of range");
    }
    std::size_t total = 0;
    if (!checked_allocation_size(static_cast<std::size_t>(size),
                                 normalized_alignment,
                                 sizeof(DefaultAllocationHeader), &total) ||
        total > static_cast<std::size_t>(
                    std::numeric_limits<std::int64_t>::max())) {
      return sanitize::Status::OutOfMemory("Allocate: size overflow");
    }
    void *raw = std::malloc(total);
    if (!raw) {
      return sanitize::Status::OutOfMemory("Allocate: out of memory");
    }
    const auto base =
        reinterpret_cast<std::uintptr_t>(raw) + sizeof(DefaultAllocationHeader);
    const auto aligned = align_up(base, normalized_alignment);
    auto *header = reinterpret_cast<DefaultAllocationHeader *>(
        aligned - sizeof(DefaultAllocationHeader));
    *header = DefaultAllocationHeader{.raw = raw,
                                      .magic = kDefaultAllocationMagic,
                                      .reserved = 0,
                                      .size = size,
                                      .alignment = normalized_alignment};
    *out = reinterpret_cast<std::uint8_t *>(aligned);
    write_allocation_guard(*out, static_cast<std::size_t>(size));
    if (!live_allocations_.register_allocation(
            *out, LiveAllocationRecord{
                      .upstream = static_cast<std::uint8_t *>(raw),
                      .upstream_size = static_cast<std::int64_t>(total),
                      .requested_size = size,
                      .alignment =
                          static_cast<std::int64_t>(normalized_alignment)})) {
      header->magic = 0;
      header->raw = nullptr;
      std::free(raw);
      *out = nullptr;
      return sanitize::Status::OutOfMemory(
          "Allocate: unable to register hardened allocation ownership");
    }
    const auto current =
        bytes_allocated_.fetch_add(size, std::memory_order_relaxed) + size;
    allocation_count_.fetch_add(1, std::memory_order_relaxed);
    update_peak(&max_memory_, current);
    return sanitize::Status::OK();
  }

  void Free(uint8_t *buffer, int64_t size,
            int64_t alignment) noexcept override {
    if (!buffer) {
      return;
    }
    LiveAllocationRecord record;
    const bool registry_enabled = hardened_allocation_registry_enabled();
    if (!live_allocations_.claim_allocation(buffer, &record)) {
      invalid_free_count_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    auto *header = reinterpret_cast<DefaultAllocationHeader *>(
        reinterpret_cast<std::uintptr_t>(buffer) -
        sizeof(DefaultAllocationHeader));
    if (registry_enabled) {
      if (header->magic != kDefaultAllocationMagic ||
          header->raw != record.upstream ||
          header->size != record.requested_size ||
          static_cast<std::int64_t>(header->alignment) != record.alignment) {
        corruption_count_.fetch_add(1, std::memory_order_relaxed);
      }
    } else if (header->magic != kDefaultAllocationMagic || !header->raw ||
               header->size <= 0) {
      invalid_free_count_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    const auto expected_size =
        registry_enabled ? record.requested_size : header->size;
    if (size > 0 && size != expected_size) {
      size_mismatch_count_.fetch_add(1, std::memory_order_relaxed);
    }
    std::size_t normalized_alignment = 0;
    if (alignment > 0 &&
        normalize_alignment(alignment, &normalized_alignment) &&
        static_cast<std::int64_t>(normalized_alignment) !=
            (registry_enabled ? record.alignment
                              : static_cast<std::int64_t>(header->alignment))) {
      size_mismatch_count_.fetch_add(1, std::memory_order_relaxed);
    }
    void *raw = registry_enabled ? record.upstream : header->raw;
    const auto actual = registry_enabled ? record.requested_size : header->size;
    if (!allocation_guard_is_valid(buffer, static_cast<std::size_t>(actual))) {
      corruption_count_.fetch_add(1, std::memory_order_relaxed);
    }
    if (secure_memory_cleanup_enabled()) {
      secure_zero_memory(buffer, static_cast<std::size_t>(actual));
    }
    header->magic = 0;
    header->raw = nullptr;
    bytes_allocated_.fetch_sub(actual, std::memory_order_relaxed);
    allocation_count_.fetch_sub(1, std::memory_order_relaxed);
    std::free(raw);
  }

  [[nodiscard]] int64_t bytes_allocated() const override {
    return bytes_allocated_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t max_memory() const override {
    return max_memory_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t allocation_count() const override {
    return allocation_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t invalid_free_count() const override {
    return invalid_free_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t size_mismatch_count() const override {
    return size_mismatch_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] int64_t corruption_count() const override {
    return corruption_count_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] bool wipes_memory_on_free() const noexcept override {
    return secure_memory_cleanup_enabled();
  }
  [[nodiscard]] std::string backend_name() const override {
    return {"schema_sanitizer::DefaultMemoryPool"};
  }

private:
  std::atomic<int64_t> bytes_allocated_{0};
  std::atomic<int64_t> max_memory_{0};
  std::atomic<int64_t> allocation_count_{0};
  std::atomic<int64_t> invalid_free_count_{0};
  std::atomic<int64_t> size_mismatch_count_{0};
  std::atomic<int64_t> corruption_count_{0};
  LiveAllocationRegistry live_allocations_;
};

#include "internal/memory/tracking_memory_pool.cc.inc"

DefaultMemoryPool g_default_pool;

class ProcessMemoryGovernor final {
public:
  class Lease final {
  public:
    Lease() = default;
    Lease(ProcessMemoryGovernor *owner, std::int64_t bytes) noexcept
        : owner_(owner), bytes_(bytes) {}
    Lease(const Lease &) = delete;
    Lease &operator=(const Lease &) = delete;
    Lease(Lease &&other) noexcept
        : owner_(std::exchange(other.owner_, nullptr)),
          bytes_(std::exchange(other.bytes_, 0)) {}
    Lease &operator=(Lease &&other) noexcept {
      if (this != &other) {
        Release();
        owner_ = std::exchange(other.owner_, nullptr);
        bytes_ = std::exchange(other.bytes_, 0);
      }
      return *this;
    }
    ~Lease() { Release(); }
    [[nodiscard]] std::int64_t bytes() const noexcept { return bytes_; }

  private:
    void Release() noexcept {
      if (owner_ && bytes_ > 0) {
        owner_->Release(bytes_);
      }
      owner_ = nullptr;
      bytes_ = 0;
    }

    ProcessMemoryGovernor *owner_ = nullptr;
    std::int64_t bytes_ = 0;
  };

  [[nodiscard]] Lease Acquire(std::int64_t requested, std::int64_t capacity) {
    constexpr std::int64_t kMinimumOperationAdmissionBytes = 1 << 20;
    constexpr std::int64_t kMaximumOperationAdmissionBytes = 8 << 20;
    const auto safe_capacity = std::max<std::int64_t>(1, capacity);
    // Reserve only fixed operation-control overhead here. Actual variable
    // allocations are governed by the shared process pool, so directory and
    // registry substreams do not each reserve the public operation's complete
    // budget and deadlock one another during lookahead.
    std::unique_lock lock(mutex_);
    // Refresh at operation boundaries. Keep enough admission space for an
    // already-issued lease while allowing cgroup/host pressure to reduce new
    // work and recovered capacity to expand it again.
    capacity_bytes_ = std::max({safe_capacity, leased_bytes_,
                                std::int64_t{kMaximumOperationAdmissionBytes}});
    // Scale the control-plane reservation with the requested operation while
    // leaving the shared parent pool authoritative for actual bytes. Under
    // contention, cap it to a fair share so many small operations can enter
    // without letting one large caller monopolize admission.
    const auto proportional =
        std::max<std::int64_t>(kMinimumOperationAdmissionBytes, requested / 64);
    const auto fair_share = std::max<std::int64_t>(
        1,
        capacity_bytes_ / std::max<std::int64_t>(2, waiting_operations_ + 2));
    const auto lease_bytes = std::clamp<std::int64_t>(
        std::min(proportional, fair_share), 1,
        std::min(capacity_bytes_, kMaximumOperationAdmissionBytes));
    if (waiters_.size() >= kMaximumWaitingOperations) {
      throw std::runtime_error("process memory admission wait queue exhausted");
    }
    Waiter waiter;
    waiters_.push_back(&waiter);
    ++waiting_operations_;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (waiters_.front() != &waiter ||
           lease_bytes > capacity_bytes_ - leased_bytes_) {
      if (ready_.wait_until(lock, deadline) == std::cv_status::timeout) {
        if (waiters_.front() == &waiter &&
            lease_bytes <= capacity_bytes_ - leased_bytes_) {
          continue;
        }
        --waiting_operations_;
        const auto position =
            std::find(waiters_.begin(), waiters_.end(), &waiter);
        if (position != waiters_.end()) {
          waiters_.erase(position);
        }
        lock.unlock();
        ready_.notify_all();
        throw std::runtime_error(
            "process memory admission exceeded its bounded deadline");
      }
    }
    --waiting_operations_;
    waiters_.pop_front();
    leased_bytes_ += lease_bytes;
    ready_.notify_all();
    return Lease(this, lease_bytes);
  }

  [[nodiscard]] ProcessMemoryGovernorStats Stats() const noexcept {
    std::lock_guard lock(mutex_);
    return ProcessMemoryGovernorStats{.capacity_bytes = capacity_bytes_,
                                      .leased_bytes = leased_bytes_,
                                      .waiting_operations =
                                          waiting_operations_};
  }

private:
  static constexpr std::size_t kMaximumWaitingOperations = 4096;

  struct Waiter final {};

  void Release(std::int64_t bytes) noexcept {
    {
      std::lock_guard lock(mutex_);
      leased_bytes_ = std::max<std::int64_t>(0, leased_bytes_ - bytes);
    }
    ready_.notify_all();
  }

  mutable std::mutex mutex_;
  std::condition_variable ready_;
  std::int64_t capacity_bytes_ = 0;
  std::int64_t leased_bytes_ = 0;
  std::int64_t waiting_operations_ = 0;
  std::deque<Waiter *> waiters_;
};

ProcessMemoryGovernor &process_memory_governor() {
  // Process-lifetime ownership keeps late runtime/library destruction safe.
  static auto *governor = new ProcessMemoryGovernor();
  return *governor;
}

class GovernedOperationMemoryPool final : public MemoryPool {
public:
  GovernedOperationMemoryPool(ProcessMemoryGovernor::Lease lease,
                              std::shared_ptr<MemoryPool> pool)
      : lease_(std::move(lease)), pool_(std::move(pool)) {}

  sanitize::Status Allocate(int64_t size, int64_t alignment,
                            uint8_t **out) override {
    return pool_->Allocate(size, alignment, out);
  }
  void Free(uint8_t *buffer, int64_t size,
            int64_t alignment) noexcept override {
    pool_->Free(buffer, size, alignment);
  }
  [[nodiscard]] int64_t bytes_allocated() const override {
    return pool_->bytes_allocated();
  }
  [[nodiscard]] int64_t max_memory() const override {
    return pool_->max_memory();
  }
  [[nodiscard]] int64_t allocation_count() const override {
    return pool_->allocation_count();
  }
  [[nodiscard]] int64_t invalid_free_count() const override {
    return pool_->invalid_free_count();
  }
  [[nodiscard]] int64_t size_mismatch_count() const override {
    return pool_->size_mismatch_count();
  }
  [[nodiscard]] int64_t corruption_count() const override {
    return pool_->corruption_count();
  }
  [[nodiscard]] int64_t limit_bytes() const override {
    return pool_->limit_bytes();
  }
  [[nodiscard]] bool wipes_memory_on_free() const noexcept override {
    return pool_->wipes_memory_on_free();
  }
  void ReleaseOperationLease() noexcept override {
    lease_ = ProcessMemoryGovernor::Lease{};
  }
  [[nodiscard]] std::string backend_name() const override {
    return pool_->backend_name();
  }

private:
  ProcessMemoryGovernor::Lease lease_;
  std::shared_ptr<MemoryPool> pool_;
};

} // namespace

bool secure_memory_cleanup_enabled() noexcept { return true; }

void secure_zero_memory(void *data, std::size_t size) noexcept {
  if (!data) {
    return;
  }
  auto *cursor = static_cast<volatile std::uint8_t *>(data);
  while (size-- > 0) {
    *cursor++ = 0;
  }
}

MemoryPool *default_memory_pool() noexcept { return &g_default_pool; }

std::shared_ptr<MemoryPool> shared_default_memory_pool() {
  return std::shared_ptr<MemoryPool>(&g_default_pool, [](MemoryPool *) {});
}

std::shared_ptr<MemoryPool>
shared_process_memory_pool(int64_t process_capacity) {
  static const auto pool = make_tracking_memory_pool(
      shared_default_memory_pool(),
      std::max<int64_t>(1, automatic_memory_limit_bytes()),
      "schema_sanitizer::ProcessMemoryPool");
  if (process_capacity > 0) {
    pool->SetLimit(std::max<int64_t>(1, process_capacity));
  }
  return pool;
}

std::shared_ptr<MemoryPool> make_tracking_memory_pool(
    std::shared_ptr<MemoryPool> parent, int64_t limit, std::string backend_name,
    bool thread_safe_registry,
    std::shared_ptr<OperationMemoryLedger> operation_ledger) {
  if (!parent) {
    parent = shared_default_memory_pool();
  }
  return std::make_shared<TrackingMemoryPool>(
      std::move(parent), limit, std::move(backend_name), thread_safe_registry,
      std::move(operation_ledger));
}

std::shared_ptr<MemoryPool> make_governed_operation_memory_pool(
    std::shared_ptr<MemoryPool> parent, int64_t requested_limit,
    int64_t process_capacity, std::string backend_name,
    std::shared_ptr<OperationMemoryLedger> operation_ledger) {
  auto lease =
      process_memory_governor().Acquire(requested_limit, process_capacity);
  auto pool = make_tracking_memory_pool(parent, requested_limit,
                                        std::move(backend_name), true,
                                        std::move(operation_ledger));
  return std::make_shared<GovernedOperationMemoryPool>(std::move(lease),
                                                       std::move(pool));
}

ProcessMemoryGovernorStats process_memory_governor_stats() noexcept {
  return process_memory_governor().Stats();
}

ProcessResidentMemoryStats process_resident_memory_stats() noexcept {
  const auto pool = shared_process_memory_pool(0);
  return ProcessResidentMemoryStats{.capacity_bytes = pool->limit_bytes(),
                                    .reserved_bytes = pool->resident_bytes(),
                                    .peak_reserved_bytes =
                                        pool->peak_resident_bytes()};
}

} // namespace sanitize::internal

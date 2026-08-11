// Declares native memory pool handles and allocation accounting.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "sanitize/core/status.hh"

namespace sanitize::internal {

// One atomic resident-byte ledger shared by Python-owned control structures
// and native reader/materialization pools for a public operation.
class OperationMemoryLedger final {
public:
  explicit OperationMemoryLedger(int64_t limit_bytes) noexcept;

  OperationMemoryLedger(const OperationMemoryLedger &) = delete;
  OperationMemoryLedger &operator=(const OperationMemoryLedger &) = delete;

  [[nodiscard]] sanitize::Status Reserve(int64_t bytes,
                                         std::string_view stage = {});
  void Release(int64_t bytes) noexcept;

  // Native allocator charges are already accounted by the shared process
  // pool. These variants update only the operation-local combined ledger.
  [[nodiscard]] sanitize::Status ReserveNative(int64_t bytes,
                                               std::string_view stage = {});
  void ReleaseNative(int64_t bytes) noexcept;

  [[nodiscard]] int64_t limit_bytes() const noexcept;
  [[nodiscard]] int64_t bytes_reserved() const noexcept;
  [[nodiscard]] int64_t peak_bytes_reserved() const noexcept;
  [[nodiscard]] int64_t over_release_count() const noexcept;
  [[nodiscard]] int64_t over_release_bytes() const noexcept;
  [[nodiscard]] bool corrupted() const noexcept;

private:
  [[nodiscard]] sanitize::Status ReserveLocal(int64_t bytes,
                                              std::string_view stage);
  [[nodiscard]] int64_t ReleaseLocal(int64_t bytes) noexcept;

  static constexpr std::uint64_t kCorruptedBit = std::uint64_t{1} << 63;
  static constexpr std::uint64_t kBytesMask = kCorruptedBit - 1;

  int64_t limit_bytes_ = 1;
  // One linearizable state word: low 63 bits are resident bytes and the high
  // bit is the irreversible corruption/admission-quarantine latch. Combining
  // them prevents a reserve CAS from slipping between over-release commit and
  // latch publication while keeping the hot path lock-free.
  std::atomic<std::uint64_t> state_{0};
  std::atomic<int64_t> peak_bytes_reserved_{0};
  std::atomic<int64_t> over_release_count_{0};
  std::atomic<int64_t> over_release_bytes_{0};
};

[[nodiscard]] std::shared_ptr<OperationMemoryLedger>
make_operation_memory_ledger(int64_t limit_bytes);

// Minimal allocator interface modeled after Arrow's MemoryPool API surface
// needed by schema-sanitizer internals.
class MemoryPool {
public:
  virtual ~MemoryPool() = default;

  virtual sanitize::Status Allocate(int64_t size, int64_t alignment,
                                    uint8_t **out) = 0;

  sanitize::Status Allocate(int64_t size, uint8_t **out) {
    return Allocate(size, 64, out);
  }

  virtual void Free(uint8_t *buffer, int64_t size,
                    int64_t alignment) noexcept = 0;

  void Free(uint8_t *buffer, int64_t size) noexcept { Free(buffer, size, 64); }

  [[nodiscard]] virtual int64_t bytes_allocated() const = 0;
  [[nodiscard]] virtual int64_t max_memory() const { return -1; }
  [[nodiscard]] virtual int64_t allocation_count() const { return -1; }
  [[nodiscard]] virtual int64_t invalid_free_count() const { return 0; }
  [[nodiscard]] virtual int64_t size_mismatch_count() const { return 0; }
  [[nodiscard]] virtual int64_t corruption_count() const { return 0; }
  [[nodiscard]] virtual int64_t limit_bytes() const { return -1; }
  // Updates a mutable aggregate ceiling. Operation-local pools keep their
  // fixed public limit and may ignore this hook.
  virtual void SetLimit(int64_t) noexcept {}
  // Charges resident bytes owned outside this allocator (for example Python
  // staging metadata) against the same process-wide ceiling as allocations.
  virtual sanitize::Status ReserveExternal(int64_t, std::string_view = {}) {
    return sanitize::Status::OK();
  }
  virtual void ReleaseExternal(int64_t) noexcept {}
  [[nodiscard]] virtual int64_t resident_bytes() const {
    return bytes_allocated();
  }
  [[nodiscard]] virtual int64_t peak_resident_bytes() const {
    return max_memory();
  }
  [[nodiscard]] virtual bool wipes_memory_on_free() const noexcept {
    return false;
  }
  // Releases an operation-admission lease without invalidating allocations
  // intentionally transferred to an analytical result.
  virtual void ReleaseOperationLease() noexcept {}
  [[nodiscard]] virtual std::string backend_name() const = 0;
};

// Returns whether best-effort cleanup of sensitive scratch memory is enabled.
[[nodiscard]] bool secure_memory_cleanup_enabled() noexcept;

// Overwrites a caller-owned byte range using volatile stores.
void secure_zero_memory(void *data, std::size_t size) noexcept;

// Default process-wide pool.
MemoryPool *default_memory_pool() noexcept;

// Returns a non-owning shared handle to the process-wide pool.
std::shared_ptr<MemoryPool> shared_default_memory_pool();

// Returns the process-wide aggregate pool. Positive safe-capacity samples
// refresh its mutable ceiling without clearing live aggregate accounting.
std::shared_ptr<MemoryPool>
shared_process_memory_pool(int64_t process_capacity);

// Creates an accounting pool layered over parent. A positive limit rejects an
// allocation before it reaches the system allocator when the operation quota
// would be exceeded. The returned pool stores allocation sizes in private
// headers, so deallocation accounting does not trust caller-supplied sizes.
std::shared_ptr<MemoryPool> make_tracking_memory_pool(
    std::shared_ptr<MemoryPool> parent, int64_t limit, std::string backend_name,
    bool thread_safe_registry = true,
    std::shared_ptr<OperationMemoryLedger> operation_ledger = nullptr);

// Creates an operation pool after acquiring a fair lease from the safe
// process-wide budget. The lease follows the returned pool's lifetime.
std::shared_ptr<MemoryPool> make_governed_operation_memory_pool(
    std::shared_ptr<MemoryPool> parent, int64_t requested_limit,
    int64_t process_capacity, std::string backend_name,
    std::shared_ptr<OperationMemoryLedger> operation_ledger = nullptr);

struct ProcessMemoryGovernorStats final {
  int64_t capacity_bytes = 0;
  int64_t leased_bytes = 0;
  int64_t waiting_operations = 0;
};

[[nodiscard]] ProcessMemoryGovernorStats
process_memory_governor_stats() noexcept;

// Exact aggregate accounting for resident bytes charged through every public
// operation ledger, including Python-owned staging buffers and native pools.
struct ProcessResidentMemoryStats final {
  int64_t capacity_bytes = 0;
  int64_t reserved_bytes = 0;
  int64_t peak_reserved_bytes = 0;
};

[[nodiscard]] ProcessResidentMemoryStats
process_resident_memory_stats() noexcept;

struct AllocationRegistryStats final {
  int64_t metadata_bytes = 0;
  int64_t peak_metadata_bytes = 0;
  int64_t capacity_records = 0;
  int64_t live_entries = 0;
  std::uint64_t rejected_registrations = 0;
  std::uint64_t secondary_probes = 0;
  std::uint64_t collision_rejections = 0;
  int64_t max_shard_occupancy = 0;
};

[[nodiscard]] AllocationRegistryStats allocation_registry_stats() noexcept;

inline MemoryPool *memory_pool_from_handle(void *handle) noexcept {
  return handle ? static_cast<MemoryPool *>(handle) : default_memory_pool();
}

} // namespace sanitize::internal

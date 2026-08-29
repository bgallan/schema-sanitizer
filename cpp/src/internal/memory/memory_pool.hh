// Declares native memory-pool handles, quotas, and allocation accounting.
// The interface also tracks external residents and operation-level
// shared ledgers.

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
  /// Initializes an operation ledger capped at the normalized positive limit.
  explicit OperationMemoryLedger(int64_t limit_bytes) noexcept;

  /// Disables copying the operation-wide resident-memory ledger.
  OperationMemoryLedger(const OperationMemoryLedger &) = delete;
  /// Disables copy assignment for the operation-wide
  /// resident-memory ledger.
  OperationMemoryLedger &operator=(const OperationMemoryLedger &) = delete;

  /// Charges both operation-local and process-wide resident-memory ledgers
  /// atomically.
  [[nodiscard]] sanitize::Status Reserve(int64_t bytes,
                                         std::string_view stage = {});
  /// Returns a resident-memory charge to the operation and process ledgers.
  void Release(int64_t bytes) noexcept;

  /// Charges native allocator bytes only to the operation-local combined
  /// ledger because the shared process pool already accounts for them.
  [[nodiscard]] sanitize::Status ReserveNative(int64_t bytes,
                                               std::string_view stage = {});
  /// Returns a native allocator charge from the operation-local ledger.
  void ReleaseNative(int64_t bytes) noexcept;

  /// Returns the immutable resident-memory limit for this operation.
  [[nodiscard]] int64_t limit_bytes() const noexcept;
  /// Returns bytes currently charged to the operation ledger.
  [[nodiscard]] int64_t bytes_reserved() const noexcept;
  /// Returns the operation ledger's resident-byte high-water mark.
  [[nodiscard]] int64_t peak_bytes_reserved() const noexcept;
  /// Returns releases that exceeded the operation's resident charge.
  [[nodiscard]] int64_t over_release_count() const noexcept;
  /// Returns bytes implicated in operation-ledger over-release attempts.
  [[nodiscard]] int64_t over_release_bytes() const noexcept;
  /// Reports whether an over-release quarantined further ledger admission.
  [[nodiscard]] bool corrupted() const noexcept;

private:
  /// Reserves bytes from this operation ledger without process accounting.
  [[nodiscard]] sanitize::Status ReserveLocal(int64_t bytes,
                                              std::string_view stage);
  /// Releases a local charge and returns the number of bytes actually removed.
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

/// Creates a shared resident-memory ledger with the requested operation limit.
[[nodiscard]] std::shared_ptr<OperationMemoryLedger>
make_operation_memory_ledger(int64_t limit_bytes);

// Minimal allocator interface modeled after Arrow's MemoryPool API surface
// needed by schema-sanitizer internals.
class MemoryPool {
public:
  /// Enables polymorphic destruction of native memory-pool implementations.
  virtual ~MemoryPool() = default;

  /// Allocates storage with the requested size and alignment through this pool.
  virtual sanitize::Status Allocate(int64_t size, int64_t alignment,
                                    uint8_t **out) = 0;

  /// Allocates storage using the pool's default alignment.
  sanitize::Status Allocate(int64_t size, uint8_t **out) {
    return Allocate(size, 64, out);
  }

  /// Returns aligned storage previously obtained from this pool.
  virtual void Free(uint8_t *buffer, int64_t size,
                    int64_t alignment) noexcept = 0;

  /// Returns storage using the pool's default alignment.
  void Free(uint8_t *buffer, int64_t size) noexcept { Free(buffer, size, 64); }

  /// Returns payload bytes currently owned by this pool.
  [[nodiscard]] virtual int64_t bytes_allocated() const = 0;
  /// Returns the pool's payload-byte high-water mark.
  [[nodiscard]] virtual int64_t max_memory() const { return -1; }
  /// Returns the pool's current live allocation count.
  [[nodiscard]] virtual int64_t allocation_count() const { return -1; }
  /// Returns frees rejected because the pool did not own their pointers.
  [[nodiscard]] virtual int64_t invalid_free_count() const { return 0; }
  /// Returns frees whose supplied sizes disagreed with allocation metadata.
  [[nodiscard]] virtual int64_t size_mismatch_count() const { return 0; }
  /// Returns detected allocation guard corruptions.
  [[nodiscard]] virtual int64_t corruption_count() const { return 0; }
  /// Returns the pool's current allocation limit.
  [[nodiscard]] virtual int64_t limit_bytes() const { return -1; }
  /// Updates a mutable aggregate ceiling; fixed operation pools may
  /// ignore it.
  virtual void SetLimit(int64_t) noexcept {}
  /// Charges non-allocator residents, such as Python staging metadata,
  /// against the same process-wide ceiling as native allocations.
  virtual sanitize::Status ReserveExternal(int64_t, std::string_view = {}) {
    return sanitize::Status::OK();
  }
  /// Returns a non-allocator resident-memory charge to the pool.
  virtual void ReleaseExternal(int64_t) noexcept {}
  /// Returns allocator and external bytes currently resident in the pool.
  [[nodiscard]] virtual int64_t resident_bytes() const {
    return bytes_allocated();
  }
  /// Returns the pool's combined resident-byte high-water mark.
  [[nodiscard]] virtual int64_t peak_resident_bytes() const {
    return max_memory();
  }
  /// Reports whether the pool securely overwrites released payloads.
  [[nodiscard]] virtual bool wipes_memory_on_free() const noexcept {
    return false;
  }
  /// Releases operation admission without invalidating allocations
  /// transferred intentionally to an analytical result.
  virtual void ReleaseOperationLease() noexcept {}
  /// Returns this allocator backend's stable diagnostic name.
  [[nodiscard]] virtual std::string backend_name() const = 0;
};

/// Reports whether best-effort cleanup of sensitive scratch memory
/// is enabled.
[[nodiscard]] bool secure_memory_cleanup_enabled() noexcept;

/// Overwrites a caller-owned byte range using volatile stores.
void secure_zero_memory(void *data, std::size_t size) noexcept;

/// Returns the process-lifetime default allocator.
MemoryPool *default_memory_pool() noexcept;

/// Returns a non-owning shared handle to the process-lifetime default pool.
std::shared_ptr<MemoryPool> shared_default_memory_pool();

/// Returns the process-wide aggregate pool. Positive safe-capacity samples
/// refresh its mutable ceiling without clearing live aggregate accounting.
std::shared_ptr<MemoryPool>
shared_process_memory_pool(int64_t process_capacity);

/// Creates an accounting pool layered over `parent`. A positive limit
/// rejects allocations before the operation quota reaches the system
/// allocator; private headers make deallocation accounting independent of
/// caller-supplied sizes.
std::shared_ptr<MemoryPool> make_tracking_memory_pool(
    std::shared_ptr<MemoryPool> parent, int64_t limit, std::string backend_name,
    bool thread_safe_registry = true,
    std::shared_ptr<OperationMemoryLedger> operation_ledger = nullptr);

/// Creates an operation pool after acquiring a fair process-budget lease
/// that follows the returned pool's lifetime.
std::shared_ptr<MemoryPool> make_governed_operation_memory_pool(
    std::shared_ptr<MemoryPool> parent, int64_t requested_limit,
    int64_t process_capacity, std::string backend_name,
    std::shared_ptr<OperationMemoryLedger> operation_ledger = nullptr);

struct ProcessMemoryGovernorStats final {
  int64_t capacity_bytes = 0;
  int64_t leased_bytes = 0;
  int64_t waiting_operations = 0;
};

/// Snapshots process-memory capacity, active leases, and queued operations.
[[nodiscard]] ProcessMemoryGovernorStats
process_memory_governor_stats() noexcept;

// Exact aggregate accounting for resident bytes charged through every public
// operation ledger, including Python-owned staging buffers and native pools.
struct ProcessResidentMemoryStats final {
  int64_t capacity_bytes = 0;
  int64_t reserved_bytes = 0;
  int64_t peak_reserved_bytes = 0;
};

/// Snapshots combined process resident-memory usage and its high-water mark.
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

/// Snapshots live-allocation registry occupancy and collision diagnostics.
[[nodiscard]] AllocationRegistryStats allocation_registry_stats() noexcept;

/// Resolves an opaque handle, falling back to the default native
/// memory pool.
inline MemoryPool *memory_pool_from_handle(void *handle) noexcept {
  return handle ? static_cast<MemoryPool *>(handle) : default_memory_pool();
}

} // namespace sanitize::internal

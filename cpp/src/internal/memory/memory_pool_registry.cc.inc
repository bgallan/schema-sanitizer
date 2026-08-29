// Implements bounded live-allocation ownership tracking without
// allocation-time metadata growth. One process-wide bank owns preallocated
// chained-hash indexes and free lists; Allocate/Free only mutate existing
// slots. Aggregate counters expose capacity, occupancy, collision, and
// rejection behavior.

/// Reports whether guarded live-allocation ownership tracking is active.
bool hardened_allocation_registry_enabled() noexcept { return true; }

struct LiveAllocationRecord {
  std::uint8_t *upstream = nullptr;
  std::int64_t upstream_size = 0;
  std::int64_t requested_size = 0;
  std::int64_t alignment = 0;
};

namespace {
std::atomic<std::int64_t> g_live_registry_metadata_bytes{0};
std::atomic<std::int64_t> g_peak_live_registry_metadata_bytes{0};
std::atomic<std::int64_t> g_live_registry_capacity_records{0};
std::atomic<std::int64_t> g_peak_live_registry_capacity_records{0};
std::atomic<std::int64_t> g_live_registry_entries{0};
std::atomic<std::int64_t> g_peak_live_registry_entries{0};
std::atomic<std::uint64_t> g_live_registry_rejections{0};
std::atomic<std::uint64_t> g_live_registry_secondary_probes{0};
std::atomic<std::uint64_t> g_live_registry_collision_rejections{0};
std::atomic<std::int64_t> g_live_registry_max_shard_occupancy{0};
constexpr std::int64_t kMaximumGlobalRegistryMetadataBytes =
    64LL * 1024LL * 1024LL;
constexpr std::int64_t kMinimumGlobalRegistryMetadataBytes = 512LL * 1024LL;

/// Reads the effective cgroup memory hard limit for registry sizing.
std::int64_t current_cgroup_memory_hard_limit_bytes() noexcept {
#if defined(__linux__)
  using cgroup_view_detail::ValueState;
  cgroup_view_detail::UnsignedSample sample{};
  const auto version = cgroup_view_detail::current_version("memory");
  if (version == 2) {
    sample = cgroup_view_detail::effective_unsigned("memory", "memory.max");
  } else if (version == 1) {
    sample = cgroup_view_detail::effective_unsigned("memory",
                                                    "memory.limit_in_bytes");
  } else {
    return 0;
  }
  if (sample.state == ValueState::kUnknown) {
    return 0;
  }
  if (sample.state == ValueState::kUnbounded) {
    return std::numeric_limits<std::int64_t>::max();
  }
  return static_cast<std::int64_t>(std::min<std::uint64_t>(
      sample.value,
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())));
#else
  return std::numeric_limits<std::int64_t>::max();
#endif
}

/// Returns the monotonic cap on process-wide allocation-registry metadata.
std::int64_t registry_metadata_limit_bytes() noexcept {
  // The bank may only shrink after startup.  A lower orchestrator memory.max
  // therefore blocks future metadata reservations without making existing
  // payload headroom grow paradoxically.
  static std::atomic<std::int64_t> limit{[]() noexcept {
    const auto process_limit =
        std::max<std::int64_t>(1, automatic_memory_limit_bytes());
    return std::min<std::int64_t>(
        kMaximumGlobalRegistryMetadataBytes,
        std::max<std::int64_t>(kMinimumGlobalRegistryMetadataBytes,
                               process_limit / 16));
  }()};
  auto current = limit.load(std::memory_order_acquire);
  const auto hard = current_cgroup_memory_hard_limit_bytes();
  if (hard != std::numeric_limits<std::int64_t>::max()) {
    const auto desired = std::min<std::int64_t>(
        current,
        std::max<std::int64_t>(kMinimumGlobalRegistryMetadataBytes, hard / 16));
    while (desired < current && !limit.compare_exchange_weak(
                                    current, desired, std::memory_order_acq_rel,
                                    std::memory_order_acquire)) {
    }
    current = std::min(current, desired);
  }
  return current;
}

/// Raises an atomic signed high-water mark to include the supplied value.
void update_atomic_peak(std::atomic<std::int64_t> *peak,
                        std::int64_t value) noexcept {
  auto observed = peak->load(std::memory_order_relaxed);
  while (value > observed && !peak->compare_exchange_weak(
                                 observed, value, std::memory_order_relaxed,
                                 std::memory_order_relaxed)) {
  }
}

} // namespace

namespace {
constexpr std::size_t kGlobalRegistryShardCount = 256U;
constexpr std::size_t kMinimumGlobalSlotsPerShard = 8U;
constexpr std::size_t kMaximumPoolRecords = 65'536U;
constexpr std::uint64_t kBytesPerRecordBudget = 1024U;
constexpr std::int64_t kRegistryLimitRefreshPeriodNs = 250'000'000LL;
constexpr std::uint32_t kNoGlobalRegistrySlot =
    std::numeric_limits<std::uint32_t>::max();
static_assert((kGlobalRegistryShardCount & (kGlobalRegistryShardCount - 1U)) ==
              0U);

/// Returns a steady-clock timestamp for allocation-registry maintenance.
[[nodiscard]] std::int64_t registry_monotonic_now_ns() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

struct GlobalAllocationSlot final {
  std::uintptr_t owner = 0U;
  std::uint8_t *buffer = nullptr;
  LiveAllocationRecord record{};
  std::uint32_t next_bucket = kNoGlobalRegistrySlot;
  // Empty slots link the free list; occupied slots retain the primary shard
  // that selected them so a two-choice redirect can be retired exactly.
  std::uint32_t free_next_or_origin = kNoGlobalRegistrySlot;
};

/// Computes fixed metadata consumed by the process-wide registry bank.
[[nodiscard]] constexpr std::int64_t
global_registry_fixed_overhead_bytes() noexcept {
  // Dynamic slot and bucket arrays are charged per record below. This is the
  // complete resident state retained once per process outside those arrays.
  return static_cast<std::int64_t>(
      sizeof(std::unique_ptr<GlobalAllocationSlot[]>) +
      sizeof(std::unique_ptr<std::uint32_t[]>) +
      sizeof(std::array<std::uint32_t, kGlobalRegistryShardCount>) * 2U +
      sizeof(
          std::array<std::atomic<std::uint32_t>, kGlobalRegistryShardCount>) +
      sizeof(std::array<std::mutex, kGlobalRegistryShardCount>) +
      sizeof(std::atomic<std::size_t>) + sizeof(std::atomic<std::int64_t>) +
      sizeof(std::mutex) + sizeof(std::size_t) * 2U + sizeof(std::int64_t));
}

struct GlobalRegistryBank final {
  std::unique_ptr<GlobalAllocationSlot[]> slots;
  std::unique_ptr<std::uint32_t[]> bucket_heads;
  std::array<std::uint32_t, kGlobalRegistryShardCount> occupancy{};
  std::array<std::uint32_t, kGlobalRegistryShardCount> free_heads{};
  std::array<std::atomic<std::uint32_t>, kGlobalRegistryShardCount>
      redirected_entries{};
  std::array<std::mutex, kGlobalRegistryShardCount> mutexes{};
  std::atomic<std::size_t> cached_usable_slots_per_shard{0U};
  std::atomic<std::int64_t> next_limit_refresh_ns{0};
  std::mutex limit_refresh_mutex;
  std::size_t slots_per_shard = 0U;
  std::size_t capacity = 0U;
  std::int64_t metadata_bytes = 0;

  /// Preallocates sharded allocation records within the registry
  /// metadata cap.
  GlobalRegistryBank() noexcept {
    const auto limit = registry_metadata_limit_bytes();
    const auto fixed_overhead = global_registry_fixed_overhead_bytes();
    if (limit <= fixed_overhead) {
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return;
    }
    const auto available = static_cast<std::uint64_t>(limit - fixed_overhead);
    constexpr auto bytes_per_record =
        sizeof(GlobalAllocationSlot) + sizeof(std::uint32_t);
    auto records = static_cast<std::size_t>(available / bytes_per_record);
    records -= records % kGlobalRegistryShardCount;
    if (records < kGlobalRegistryShardCount * kMinimumGlobalSlotsPerShard) {
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return;
    }
    slots_per_shard = records / kGlobalRegistryShardCount;
    if (slots_per_shard >= kNoGlobalRegistrySlot) {
      slots_per_shard = 0U;
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return;
    }
    capacity = slots_per_shard * kGlobalRegistryShardCount;
    try {
      slots = std::make_unique<GlobalAllocationSlot[]>(capacity);
      bucket_heads = std::make_unique<std::uint32_t[]>(capacity);
    } catch (...) {
      slots.reset();
      bucket_heads.reset();
      slots_per_shard = 0U;
      capacity = 0U;
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return;
    }
    std::fill_n(bucket_heads.get(), capacity, kNoGlobalRegistrySlot);
    for (std::size_t shard = 0U; shard < kGlobalRegistryShardCount; ++shard) {
      auto *begin = slots.get() + shard * slots_per_shard;
      for (std::size_t index = 0U; index < slots_per_shard; ++index) {
        begin[index].free_next_or_origin =
            index + 1U < slots_per_shard
                ? static_cast<std::uint32_t>(index + 1U)
                : kNoGlobalRegistrySlot;
      }
      free_heads[shard] = 0U;
    }
    cached_usable_slots_per_shard.store(slots_per_shard,
                                        std::memory_order_release);
    next_limit_refresh_ns.store(registry_monotonic_now_ns() +
                                    kRegistryLimitRefreshPeriodNs,
                                std::memory_order_release);
    metadata_bytes =
        static_cast<std::int64_t>(capacity * bytes_per_record + fixed_overhead);
    g_live_registry_metadata_bytes.store(metadata_bytes,
                                         std::memory_order_release);
    g_peak_live_registry_metadata_bytes.store(metadata_bytes,
                                              std::memory_order_release);
    g_live_registry_capacity_records.store(static_cast<std::int64_t>(capacity),
                                           std::memory_order_release);
    g_peak_live_registry_capacity_records.store(
        static_cast<std::int64_t>(capacity), std::memory_order_release);
  }

  /// Returns record slots usable by each registry shard after
  /// sentinel reservation.
  [[nodiscard]] std::size_t usable_slots_per_shard() noexcept {
    if (!slots || !bucket_heads || slots_per_shard == 0U) {
      return 0U;
    }
    // memory.max may shrink after startup.  The physical bank remains fixed
    // (so no teardown allocation/free is needed), but new registrations are
    // admitted only into the shrink-only usable prefix. Resolving the cgroup
    // hierarchy opens several procfs/control files, so sample it at most four
    // times per second rather than once per allocation. A continuously active
    // process observes a shrink within 250 ms; after an idle period, the first
    // registration serializes the refresh before it can admit a new entry.
    const auto now = registry_monotonic_now_ns();
    const auto next = next_limit_refresh_ns.load(std::memory_order_acquire);
    if (now < next) {
      return cached_usable_slots_per_shard.load(std::memory_order_acquire);
    }
    std::lock_guard refresh_lock(limit_refresh_mutex);
    const auto locked_now = registry_monotonic_now_ns();
    const auto refreshed_next =
        next_limit_refresh_ns.load(std::memory_order_acquire);
    if (locked_now < refreshed_next) {
      return cached_usable_slots_per_shard.load(std::memory_order_acquire);
    }
    const auto current_limit = registry_metadata_limit_bytes();
    const auto fixed_overhead = global_registry_fixed_overhead_bytes();
    constexpr auto bytes_per_record =
        sizeof(GlobalAllocationSlot) + sizeof(std::uint32_t);
    std::size_t desired = 0U;
    if (current_limit > fixed_overhead) {
      const auto usable_records =
          static_cast<std::size_t>((current_limit - fixed_overhead) /
                                   static_cast<std::int64_t>(bytes_per_record));
      desired =
          std::min(slots_per_shard, usable_records / kGlobalRegistryShardCount);
    }
    auto observed =
        cached_usable_slots_per_shard.load(std::memory_order_acquire);
    while (desired < observed &&
           !cached_usable_slots_per_shard.compare_exchange_weak(
               observed, desired, std::memory_order_acq_rel,
               std::memory_order_acquire)) {
    }
    const auto usable = std::min(observed, desired);
    next_limit_refresh_ns.store(registry_monotonic_now_ns() +
                                    kRegistryLimitRefreshPeriodNs,
                                std::memory_order_release);
    return usable;
  }

  /// Returns the lazily cached usable record capacity per shard.
  [[nodiscard]] std::size_t cached_usable_slots() const noexcept {
    return cached_usable_slots_per_shard.load(std::memory_order_acquire);
  }
};

static_assert(
    sizeof(GlobalRegistryBank) ==
        static_cast<std::size_t>(global_registry_fixed_overhead_bytes()),
    "registry metadata accounting must include padding and fixed state");

/// Returns the lazily initialized process-wide allocation registry bank.
GlobalRegistryBank &global_registry_bank() noexcept {
  static GlobalRegistryBank bank;
  return bank;
}
} // namespace

/// Returns metadata bytes currently committed by the
/// live-allocation registry.
std::int64_t live_allocation_registry_metadata_bytes() noexcept {
  return global_registry_bank().metadata_bytes;
}

/// Returns the registry metadata high-water mark in bytes.
[[maybe_unused]] std::int64_t
peak_live_allocation_registry_metadata_bytes() noexcept {
  (void)global_registry_bank();
  return g_peak_live_registry_metadata_bytes.load(std::memory_order_relaxed);
}

/// Returns the number of allocation records available across
/// registry shards.
[[maybe_unused]] std::int64_t
live_allocation_registry_capacity_records() noexcept {
  return static_cast<std::int64_t>(global_registry_bank().capacity);
}

/// Returns the number of allocations currently registered as live.
[[maybe_unused]] std::int64_t live_allocation_registry_entries() noexcept {
  return g_live_registry_entries.load(std::memory_order_relaxed);
}

/// Returns registrations rejected because bounded registry capacity
/// was exhausted.
[[maybe_unused]] std::uint64_t live_allocation_registry_rejections() noexcept {
  return g_live_registry_rejections.load(std::memory_order_relaxed);
}

/// Returns metadata bytes reserved for the registry's fixed capacity.
std::int64_t live_allocation_registry_metadata_capacity_bytes() noexcept {
  return global_registry_bank().metadata_bytes;
}

/// Returns hash probes that advanced beyond an allocation's primary slot.
[[maybe_unused]] std::uint64_t
live_allocation_registry_secondary_probes() noexcept {
  return g_live_registry_secondary_probes.load(std::memory_order_relaxed);
}

/// Returns registrations rejected after bounded collision probing failed.
[[maybe_unused]] std::uint64_t
live_allocation_registry_collision_rejections() noexcept {
  return g_live_registry_collision_rejections.load(std::memory_order_relaxed);
}

/// Returns the highest live-record occupancy observed in any
/// registry shard.
[[maybe_unused]] std::int64_t
live_allocation_registry_max_shard_occupancy() noexcept {
  return g_live_registry_max_shard_occupancy.load(std::memory_order_relaxed);
}

class LiveAllocationRegistry {
public:
  /// Binds allocation tracking to a fixed process-wide registry
  /// shard budget.
  explicit LiveAllocationRegistry(bool /*thread_safe*/ = true,
                                  std::int64_t budget_bytes = -1)
      : owner_token_(reinterpret_cast<std::uintptr_t>(this)) {
    (void)global_registry_bank();
    std::size_t target = kMaximumPoolRecords;
    if (budget_bytes > 0) {
      const auto budget_u64 = static_cast<std::uint64_t>(budget_bytes);
      target = static_cast<std::size_t>(std::min<std::uint64_t>(
          kMaximumPoolRecords,
          std::max<std::uint64_t>(1U, budget_u64 / kBytesPerRecordBudget)));
    }
    record_limit_ = std::max<std::size_t>(1U, target);
  }

  /// Purges this pool owner's live registry records before destruction.
  ~LiveAllocationRegistry() { purge_owner(); }

  /// Disables copying the bounded live-allocation registry.
  LiveAllocationRegistry(const LiveAllocationRegistry &) = delete;
  /// Disables copy assignment for the bounded live-allocation registry.
  LiveAllocationRegistry &operator=(const LiveAllocationRegistry &) = delete;

  /// Returns metadata bytes charged for this registry's fixed slot budget.
  [[nodiscard]] std::int64_t metadata_bytes() const noexcept { return 0; }
  /// Returns the maximum live records this registry can hold.
  [[nodiscard]] std::size_t capacity() const noexcept { return record_limit_; }

  /// Registers allocation ownership in a bounded shard without
  /// allocating metadata.
  bool register_allocation(std::uint8_t *buffer,
                           LiveAllocationRecord record) noexcept {
    if (!hardened_allocation_registry_enabled()) {
      return true;
    }
    auto &bank = global_registry_bank();
    if (!buffer || !bank.slots || bank.capacity == 0U) {
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
    if (!reserve_owner_entry()) {
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
    const auto usable = bank.usable_slots_per_shard();
    if (usable == 0U) {
      live_entries_.fetch_sub(1, std::memory_order_relaxed);
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
    InsertResult result = InsertResult::kFull;
    const auto primary = shard_index(buffer);
    bool requires_slow_path = false;
    {
      std::lock_guard lock(bank.mutexes[primary]);
      if (!redirected_to_secondary(bank, primary)) {
        result = register_in_primary(bank, usable, primary, buffer, record);
      } else {
        requires_slow_path = true;
      }
    }
    if (result == InsertResult::kFull || requires_slow_path) {
      const auto secondary = secondary_shard_index(buffer, primary);
      g_live_registry_secondary_probes.fetch_add(1U, std::memory_order_relaxed);
      const auto first = std::min(primary, secondary);
      const auto second = std::max(primary, secondary);
      std::scoped_lock lock(bank.mutexes[first], bank.mutexes[second]);
      result = register_in_two_shards(bank, usable, primary, secondary, buffer,
                                      record);
    }
    const bool inserted = result == InsertResult::kInserted;
    if (result == InsertResult::kFull) {
      g_live_registry_collision_rejections.fetch_add(1U,
                                                     std::memory_order_relaxed);
    }
    if (!inserted) {
      live_entries_.fetch_sub(1, std::memory_order_relaxed);
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
    }
    return inserted;
  }

  /// Atomically removes and returns ownership metadata for a
  /// live allocation.
  bool claim_allocation(std::uint8_t *buffer,
                        LiveAllocationRecord *record) noexcept {
    if (!hardened_allocation_registry_enabled()) {
      return true;
    }
    auto &bank = global_registry_bank();
    if (!buffer || !bank.slots || bank.capacity == 0U) {
      return false;
    }
    const auto primary = shard_index(buffer);
    bool claimed = false;
    {
      std::lock_guard lock(bank.mutexes[primary]);
      claimed = claim_in_one_shard(bank, primary, buffer, record);
    }
    if (!claimed && redirected_to_secondary(bank, primary)) {
      const auto secondary = secondary_shard_index(buffer, primary);
      g_live_registry_secondary_probes.fetch_add(1U, std::memory_order_relaxed);
      std::lock_guard lock(bank.mutexes[secondary]);
      claimed = claim_in_one_shard(bank, secondary, buffer, record);
    }
    if (claimed) {
      live_entries_.fetch_sub(1, std::memory_order_relaxed);
      g_live_registry_entries.fetch_sub(1, std::memory_order_relaxed);
    }
    return claimed;
  }

private:
  enum class InsertResult : std::uint8_t { kInserted, kDuplicate, kFull };

  /// Publishes one live registration and raises the global occupancy peak.
  static void observe_registration() noexcept {
    const auto current =
        g_live_registry_entries.fetch_add(1, std::memory_order_relaxed) + 1;
    update_atomic_peak(&g_peak_live_registry_entries, current);
  }

  /// Atomically reserves one record within this pool owner's fixed limit.
  [[nodiscard]] bool reserve_owner_entry() noexcept {
    auto current = live_entries_.load(std::memory_order_relaxed);
    const auto limit = static_cast<std::int64_t>(record_limit_);
    while (current < limit) {
      if (live_entries_.compare_exchange_weak(current, current + 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  /// Hashes a buffer address to its primary registry shard.
  [[nodiscard]] static std::size_t
  shard_index(const std::uint8_t *buffer) noexcept {
    const auto address = reinterpret_cast<std::uintptr_t>(buffer);
    const auto mixed = (address >> 6U) ^ (address >> 17U) ^ (address >> 29U);
    return static_cast<std::size_t>(mixed) & (kGlobalRegistryShardCount - 1U);
  }

  /// Hashes a buffer address to a shard distinct from its primary shard.
  [[nodiscard]] static std::size_t
  secondary_shard_index(const std::uint8_t *buffer,
                        std::size_t primary) noexcept {
    const auto address = reinterpret_cast<std::uintptr_t>(buffer);
    const auto mixed = (address >> 11U) ^ (address >> 23U) ^ (address >> 37U);
    auto secondary =
        static_cast<std::size_t>(mixed) & (kGlobalRegistryShardCount - 1U);
    if (secondary == primary) {
      secondary ^= 1U;
    }
    return secondary;
  }

  /// Returns the first allocation slot belonging to one registry shard.
  static GlobalAllocationSlot *shard_begin(GlobalRegistryBank &bank,
                                           std::size_t shard) noexcept {
    return bank.slots.get() + shard * bank.slots_per_shard;
  }

  /// Hashes a buffer address to a bucket within the selected shard.
  [[nodiscard]] static std::size_t bucket_index(const GlobalRegistryBank &bank,
                                                const std::uint8_t *buffer,
                                                std::size_t shard) noexcept {
    auto mixed = reinterpret_cast<std::uintptr_t>(buffer);
    // The bucket is selected by buffer address; owner remains part of the
    // equality key so different pool generations cannot claim each other.
    mixed ^= static_cast<std::uintptr_t>(0x9e3779b97f4a7c15ULL) +
             (mixed << 6U) + (mixed >> 2U);
    mixed ^= mixed >> 30U;
    mixed *= static_cast<std::uintptr_t>(0xbf58476d1ce4e5b9ULL);
    mixed ^= mixed >> 27U;
    mixed *= static_cast<std::uintptr_t>(0x94d049bb133111ebULL);
    mixed ^= mixed >> 31U;
    return shard * bank.slots_per_shard +
           static_cast<std::size_t>(mixed % bank.slots_per_shard);
  }

  /// Finds this owner's matching buffer slot within one registry shard.
  [[nodiscard]] std::uint32_t
  find_slot_index(GlobalRegistryBank &bank, std::size_t shard,
                  std::uint8_t *buffer) const noexcept {
    const auto bucket = bucket_index(bank, buffer, shard);
    auto index = bank.bucket_heads[bucket];
    auto *begin = shard_begin(bank, shard);
    while (index != kNoGlobalRegistrySlot) {
      const auto &slot = begin[index];
      if (slot.owner == owner_token_ && slot.buffer == buffer) {
        return index;
      }
      index = slot.next_bucket;
    }
    return kNoGlobalRegistrySlot;
  }

  /// Reports whether a primary shard has entries stored in secondary shards.
  [[nodiscard]] bool
  redirected_to_secondary(const GlobalRegistryBank &bank,
                          std::size_t primary) const noexcept {
    return bank.redirected_entries[primary].load(std::memory_order_acquire) !=
           0U;
  }

  /// Retires unusable suffix slots and reports whether a free slot remains.
  [[nodiscard]] static bool has_usable_free_slot(GlobalRegistryBank &bank,
                                                 std::size_t usable,
                                                 std::size_t shard) noexcept {
    auto &head = bank.free_heads[shard];
    auto *begin = shard_begin(bank, shard);
    // The usable prefix only shrinks. Once a free suffix slot becomes
    // inadmissible it can be retired permanently from this free list; a live
    // suffix slot remains indexed until its owner reclaims it.
    while (head != kNoGlobalRegistrySlot &&
           static_cast<std::size_t>(head) >= usable) {
      const auto retired = head;
      head = begin[retired].free_next_or_origin;
      begin[retired].free_next_or_origin = kNoGlobalRegistrySlot;
    }
    return head != kNoGlobalRegistrySlot;
  }

  /// Removes and returns one usable slot from a shard's free list.
  [[nodiscard]] static std::uint32_t
  pop_usable_free_slot(GlobalRegistryBank &bank, std::size_t usable,
                       std::size_t shard) noexcept {
    if (!has_usable_free_slot(bank, usable, shard)) {
      return kNoGlobalRegistrySlot;
    }
    auto &head = bank.free_heads[shard];
    const auto index = head;
    auto *begin = shard_begin(bank, shard);
    head = begin[index].free_next_or_origin;
    begin[index].free_next_or_origin = kNoGlobalRegistrySlot;
    return index;
  }

  /// Returns an eligible slot to its shard's free list.
  static void return_free_slot(GlobalRegistryBank &bank, std::size_t shard,
                               std::uint32_t index) noexcept {
    if (static_cast<std::size_t>(index) >= bank.cached_usable_slots()) {
      return;
    }
    auto *begin = shard_begin(bank, shard);
    begin[index].free_next_or_origin = bank.free_heads[shard];
    bank.free_heads[shard] = index;
  }

  /// Populates a slot, links it into its bucket, and publishes registration.
  void link_slot(GlobalRegistryBank &bank, std::size_t shard,
                 std::size_t origin_shard, std::uint32_t index,
                 std::uint8_t *buffer, LiveAllocationRecord record) noexcept {
    auto *begin = shard_begin(bank, shard);
    auto &slot = begin[index];
    const auto bucket = bucket_index(bank, buffer, shard);
    slot.record = record;
    slot.buffer = buffer;
    slot.owner = owner_token_;
    slot.next_bucket = bank.bucket_heads[bucket];
    slot.free_next_or_origin = static_cast<std::uint32_t>(origin_shard);
    bank.bucket_heads[bucket] = index;
    // The global count becomes visible while the publishing shard lock is
    // still held, before a claim can unlink and decrement this entry.
    observe_registration();
  }

  /// Inserts a unique record into its primary shard when capacity permits.
  InsertResult register_in_primary(GlobalRegistryBank &bank, std::size_t usable,
                                   std::size_t primary, std::uint8_t *buffer,
                                   LiveAllocationRecord record) noexcept {
    if (find_slot_index(bank, primary, buffer) != kNoGlobalRegistrySlot) {
      return InsertResult::kDuplicate;
    }
    if (!has_usable_free_slot(bank, usable, primary)) {
      return InsertResult::kFull;
    }
    const auto target_index = pop_usable_free_slot(bank, usable, primary);
    if (target_index == kNoGlobalRegistrySlot) {
      return InsertResult::kFull;
    }
    link_slot(bank, primary, primary, target_index, buffer, record);
    const auto occupancy = ++bank.occupancy[primary];
    update_atomic_peak(&g_live_registry_max_shard_occupancy,
                       static_cast<std::int64_t>(occupancy));
    return InsertResult::kInserted;
  }

  /// Inserts into either eligible shard and records secondary redirection.
  InsertResult register_in_two_shards(GlobalRegistryBank &bank,
                                      std::size_t usable, std::size_t primary,
                                      std::size_t secondary,
                                      std::uint8_t *buffer,
                                      LiveAllocationRecord record) noexcept {
    if (find_slot_index(bank, primary, buffer) != kNoGlobalRegistrySlot) {
      return InsertResult::kDuplicate;
    }
    if (find_slot_index(bank, secondary, buffer) != kNoGlobalRegistrySlot) {
      return InsertResult::kDuplicate;
    }
    auto target_shard = primary;
    auto target_index = pop_usable_free_slot(bank, usable, primary);
    if (target_index == kNoGlobalRegistrySlot) {
      target_shard = secondary;
      target_index = pop_usable_free_slot(bank, usable, secondary);
    }
    if (target_index == kNoGlobalRegistrySlot) {
      return InsertResult::kFull;
    }
    link_slot(bank, target_shard, primary, target_index, buffer, record);
    if (target_shard != primary) {
      bank.redirected_entries[primary].fetch_add(1U, std::memory_order_release);
    }
    const auto occupancy = ++bank.occupancy[target_shard];
    update_atomic_peak(&g_live_registry_max_shard_occupancy,
                       static_cast<std::int64_t>(occupancy));
    return InsertResult::kInserted;
  }

  /// Claims and unlinks this owner's matching record from one shard.
  bool claim_in_one_shard(GlobalRegistryBank &bank, std::size_t shard,
                          std::uint8_t *buffer,
                          LiveAllocationRecord *record) noexcept {
    auto *begin = shard_begin(bank, shard);
    const auto bucket = bucket_index(bank, buffer, shard);
    auto *link = &bank.bucket_heads[bucket];
    while (*link != kNoGlobalRegistrySlot) {
      const auto index = *link;
      auto &slot = begin[index];
      if (slot.owner == owner_token_ && slot.buffer == buffer) {
        const auto origin_shard = slot.free_next_or_origin;
        if (record) {
          *record = slot.record;
        }
        *link = slot.next_bucket;
        slot = GlobalAllocationSlot{};
        return_free_slot(bank, shard, index);
        if (origin_shard < kGlobalRegistryShardCount && origin_shard != shard) {
          bank.redirected_entries[origin_shard].fetch_sub(
              1U, std::memory_order_release);
        }
        if (bank.occupancy[shard] > 0U) {
          --bank.occupancy[shard];
        }
        return true;
      }
      link = &slot.next_bucket;
    }
    return false;
  }

  /// Unlinks a populated slot from its shard bucket without clearing it.
  static bool unlink_slot(GlobalRegistryBank &bank, std::size_t shard,
                          std::uint32_t index) noexcept {
    auto *begin = shard_begin(bank, shard);
    auto &slot = begin[index];
    if (slot.owner == 0U || slot.buffer == nullptr) {
      return false;
    }
    const auto bucket = bucket_index(bank, slot.buffer, shard);
    auto *link = &bank.bucket_heads[bucket];
    while (*link != kNoGlobalRegistrySlot) {
      if (*link == index) {
        *link = slot.next_bucket;
        return true;
      }
      link = &begin[*link].next_bucket;
    }
    return false;
  }

  /// Removes every live registry record owned by this pool instance.
  void purge_owner() noexcept {
    auto outstanding = live_entries_.exchange(0, std::memory_order_acq_rel);
    if (outstanding <= 0) {
      return;
    }
    auto &bank = global_registry_bank();
    std::int64_t removed = 0;
    for (std::size_t shard = 0; shard < kGlobalRegistryShardCount; ++shard) {
      std::lock_guard lock(bank.mutexes[shard]);
      auto *begin = shard_begin(bank, shard);
      for (std::size_t index = 0; index < bank.slots_per_shard; ++index) {
        auto &slot = begin[index];
        if (slot.owner != owner_token_) {
          continue;
        }
        const auto origin_shard = slot.free_next_or_origin;
        if (!unlink_slot(bank, shard, static_cast<std::uint32_t>(index))) {
          continue;
        }
        slot = GlobalAllocationSlot{};
        return_free_slot(bank, shard, static_cast<std::uint32_t>(index));
        if (origin_shard < kGlobalRegistryShardCount && origin_shard != shard) {
          bank.redirected_entries[origin_shard].fetch_sub(
              1U, std::memory_order_release);
        }
        if (bank.occupancy[shard] > 0U) {
          --bank.occupancy[shard];
        }
        ++removed;
      }
    }
    if (removed > 0) {
      g_live_registry_entries.fetch_sub(removed, std::memory_order_relaxed);
    }
  }

  std::uintptr_t owner_token_ = 0U;
  std::size_t record_limit_ = 1U;
  std::atomic<std::int64_t> live_entries_{0};
};

// Implements bounded live-allocation ownership tracking without allocation-time
// metadata growth. Every registry owns one flat table allocated at pool
// construction; Allocate/Free only mutate existing slots.

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
static_assert((kGlobalRegistryShardCount & (kGlobalRegistryShardCount - 1U)) ==
              0U);

struct GlobalAllocationSlot final {
  std::uintptr_t owner = 0U;
  std::uint8_t *buffer = nullptr;
  LiveAllocationRecord record{};
};

struct GlobalRegistryBank final {
  std::unique_ptr<GlobalAllocationSlot[]> slots;
  std::array<std::uint32_t, kGlobalRegistryShardCount> occupancy{};
  std::array<std::mutex, kGlobalRegistryShardCount> mutexes{};
  std::size_t slots_per_shard = 0U;
  std::size_t capacity = 0U;
  std::int64_t metadata_bytes = 0;

  GlobalRegistryBank() noexcept {
    const auto limit = registry_metadata_limit_bytes();
    const auto fixed_overhead =
        static_cast<std::int64_t>(sizeof(occupancy) + sizeof(mutexes));
    if (limit <= fixed_overhead) {
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return;
    }
    const auto available = static_cast<std::uint64_t>(limit - fixed_overhead);
    auto records = static_cast<std::size_t>(
        available / static_cast<std::uint64_t>(sizeof(GlobalAllocationSlot)));
    records -= records % kGlobalRegistryShardCount;
    if (records < kGlobalRegistryShardCount * kMinimumGlobalSlotsPerShard) {
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return;
    }
    slots_per_shard = records / kGlobalRegistryShardCount;
    capacity = slots_per_shard * kGlobalRegistryShardCount;
    try {
      slots = std::make_unique<GlobalAllocationSlot[]>(capacity);
    } catch (...) {
      slots_per_shard = 0U;
      capacity = 0U;
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return;
    }
    metadata_bytes = static_cast<std::int64_t>(
        capacity * sizeof(GlobalAllocationSlot) + fixed_overhead);
    g_live_registry_metadata_bytes.store(metadata_bytes,
                                         std::memory_order_release);
    g_peak_live_registry_metadata_bytes.store(metadata_bytes,
                                              std::memory_order_release);
    g_live_registry_capacity_records.store(static_cast<std::int64_t>(capacity),
                                           std::memory_order_release);
    g_peak_live_registry_capacity_records.store(
        static_cast<std::int64_t>(capacity), std::memory_order_release);
  }

  [[nodiscard]] std::size_t usable_slots_per_shard() const noexcept {
    if (!slots || slots_per_shard == 0U) {
      return 0U;
    }
    // memory.max may shrink after startup.  The physical bank remains fixed
    // (so no teardown allocation/free is needed), but new registrations are
    // admitted only into the shrink-only usable prefix.
    const auto current_limit = registry_metadata_limit_bytes();
    const auto fixed_overhead =
        static_cast<std::int64_t>(sizeof(occupancy) + sizeof(mutexes));
    if (current_limit <= fixed_overhead) {
      return 0U;
    }
    const auto usable_records = static_cast<std::size_t>(
        (current_limit - fixed_overhead) /
        static_cast<std::int64_t>(sizeof(GlobalAllocationSlot)));
    return std::min(slots_per_shard,
                    usable_records / kGlobalRegistryShardCount);
  }
};

GlobalRegistryBank &global_registry_bank() noexcept {
  static GlobalRegistryBank bank;
  return bank;
}
} // namespace

std::int64_t live_allocation_registry_metadata_bytes() noexcept {
  return global_registry_bank().metadata_bytes;
}

[[maybe_unused]] std::int64_t
peak_live_allocation_registry_metadata_bytes() noexcept {
  (void)global_registry_bank();
  return g_peak_live_registry_metadata_bytes.load(std::memory_order_relaxed);
}

[[maybe_unused]] std::int64_t
live_allocation_registry_capacity_records() noexcept {
  return static_cast<std::int64_t>(global_registry_bank().capacity);
}

[[maybe_unused]] std::int64_t live_allocation_registry_entries() noexcept {
  return g_live_registry_entries.load(std::memory_order_relaxed);
}

[[maybe_unused]] std::uint64_t live_allocation_registry_rejections() noexcept {
  return g_live_registry_rejections.load(std::memory_order_relaxed);
}

std::int64_t live_allocation_registry_metadata_capacity_bytes() noexcept {
  return global_registry_bank().metadata_bytes;
}

[[maybe_unused]] std::uint64_t
live_allocation_registry_secondary_probes() noexcept {
  return g_live_registry_secondary_probes.load(std::memory_order_relaxed);
}

[[maybe_unused]] std::uint64_t
live_allocation_registry_collision_rejections() noexcept {
  return g_live_registry_collision_rejections.load(std::memory_order_relaxed);
}

[[maybe_unused]] std::int64_t
live_allocation_registry_max_shard_occupancy() noexcept {
  return g_live_registry_max_shard_occupancy.load(std::memory_order_relaxed);
}

class LiveAllocationRegistry {
public:
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

  ~LiveAllocationRegistry() { purge_owner(); }

  LiveAllocationRegistry(const LiveAllocationRegistry &) = delete;
  LiveAllocationRegistry &operator=(const LiveAllocationRegistry &) = delete;

  [[nodiscard]] std::int64_t metadata_bytes() const noexcept { return 0; }
  [[nodiscard]] std::size_t capacity() const noexcept { return record_limit_; }

  bool register_allocation(std::uint8_t *buffer,
                           LiveAllocationRecord record) noexcept {
    if (!hardened_allocation_registry_enabled()) {
      return true;
    }
    auto &bank = global_registry_bank();
    if (!buffer || !bank.slots || bank.capacity == 0U ||
        live_entries_.load(std::memory_order_relaxed) >=
            static_cast<std::int64_t>(record_limit_)) {
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
    const auto usable = bank.usable_slots_per_shard();
    if (usable == 0U) {
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
    const auto primary = shard_index(buffer);
    const auto secondary = secondary_shard_index(buffer, primary);
    InsertResult result = InsertResult::kFull;
    if (primary == secondary) {
      std::lock_guard lock(bank.mutexes[primary]);
      result =
          register_in_shards(bank, usable, primary, secondary, buffer, record);
    } else {
      const auto first = std::min(primary, secondary);
      const auto second = std::max(primary, secondary);
      std::scoped_lock lock(bank.mutexes[first], bank.mutexes[second]);
      result =
          register_in_shards(bank, usable, primary, secondary, buffer, record);
    }
    const bool inserted = result == InsertResult::kInserted;
    if (result == InsertResult::kFull) {
      g_live_registry_collision_rejections.fetch_add(1U,
                                                     std::memory_order_relaxed);
    }
    observe_registration(inserted);
    return inserted;
  }

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
    const auto secondary = secondary_shard_index(buffer, primary);
    bool claimed = false;
    if (primary == secondary) {
      std::lock_guard lock(bank.mutexes[primary]);
      claimed = claim_in_shards(bank, primary, secondary, buffer, record);
    } else {
      const auto first = std::min(primary, secondary);
      const auto second = std::max(primary, secondary);
      std::scoped_lock lock(bank.mutexes[first], bank.mutexes[second]);
      claimed = claim_in_shards(bank, primary, secondary, buffer, record);
    }
    if (claimed) {
      live_entries_.fetch_sub(1, std::memory_order_relaxed);
      g_live_registry_entries.fetch_sub(1, std::memory_order_relaxed);
    }
    return claimed;
  }

private:
  enum class InsertResult : std::uint8_t { kInserted, kDuplicate, kFull };

  void observe_registration(bool inserted) noexcept {
    if (!inserted) {
      g_live_registry_rejections.fetch_add(1U, std::memory_order_relaxed);
      return;
    }
    live_entries_.fetch_add(1, std::memory_order_relaxed);
    const auto current =
        g_live_registry_entries.fetch_add(1, std::memory_order_relaxed) + 1;
    update_atomic_peak(&g_peak_live_registry_entries, current);
  }

  [[nodiscard]] static std::size_t
  shard_index(const std::uint8_t *buffer) noexcept {
    const auto address = reinterpret_cast<std::uintptr_t>(buffer);
    const auto mixed = (address >> 6U) ^ (address >> 17U) ^ (address >> 29U);
    return static_cast<std::size_t>(mixed) & (kGlobalRegistryShardCount - 1U);
  }

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

  static GlobalAllocationSlot *shard_begin(GlobalRegistryBank &bank,
                                           std::size_t shard) noexcept {
    return bank.slots.get() + shard * bank.slots_per_shard;
  }

  GlobalAllocationSlot *find_slot(GlobalRegistryBank &bank, std::size_t usable,
                                  std::size_t shard, std::uint8_t *buffer,
                                  GlobalAllocationSlot **empty) noexcept {
    auto *begin = shard_begin(bank, shard);
    for (std::size_t index = 0; index < usable; ++index) {
      auto &slot = begin[index];
      if (slot.owner == owner_token_ && slot.buffer == buffer) {
        return &slot;
      }
      if (slot.owner == 0U && empty && !*empty) {
        *empty = &slot;
      }
    }
    return nullptr;
  }

  InsertResult register_in_shards(GlobalRegistryBank &bank, std::size_t usable,
                                  std::size_t primary, std::size_t secondary,
                                  std::uint8_t *buffer,
                                  LiveAllocationRecord record) noexcept {
    GlobalAllocationSlot *primary_empty = nullptr;
    GlobalAllocationSlot *secondary_empty = nullptr;
    if (find_slot(bank, usable, primary, buffer, &primary_empty)) {
      return InsertResult::kDuplicate;
    }
    if (secondary != primary &&
        find_slot(bank, usable, secondary, buffer, &secondary_empty)) {
      return InsertResult::kDuplicate;
    }
    GlobalAllocationSlot *target = primary_empty;
    auto target_shard = primary;
    if (secondary != primary && secondary_empty &&
        (!primary_empty ||
         bank.occupancy[secondary] < bank.occupancy[primary])) {
      g_live_registry_secondary_probes.fetch_add(1U, std::memory_order_relaxed);
      target = secondary_empty;
      target_shard = secondary;
    }
    if (!target) {
      return InsertResult::kFull;
    }
    target->record = record;
    target->buffer = buffer;
    target->owner = owner_token_;
    const auto occupancy = ++bank.occupancy[target_shard];
    update_atomic_peak(&g_live_registry_max_shard_occupancy,
                       static_cast<std::int64_t>(occupancy));
    return InsertResult::kInserted;
  }

  bool claim_in_one_shard(GlobalRegistryBank &bank, std::size_t shard,
                          std::uint8_t *buffer,
                          LiveAllocationRecord *record) noexcept {
    auto *begin = shard_begin(bank, shard);
    // Claims must inspect the full physical bank so allocations registered
    // before a memory.max shrink remain reclaimable.
    for (std::size_t index = 0; index < bank.slots_per_shard; ++index) {
      auto &slot = begin[index];
      if (slot.owner != owner_token_ || slot.buffer != buffer) {
        continue;
      }
      if (record) {
        *record = slot.record;
      }
      slot = GlobalAllocationSlot{};
      if (bank.occupancy[shard] > 0U) {
        --bank.occupancy[shard];
      }
      return true;
    }
    return false;
  }

  bool claim_in_shards(GlobalRegistryBank &bank, std::size_t primary,
                       std::size_t secondary, std::uint8_t *buffer,
                       LiveAllocationRecord *record) noexcept {
    if (claim_in_one_shard(bank, primary, buffer, record)) {
      return true;
    }
    if (secondary != primary) {
      g_live_registry_secondary_probes.fetch_add(1U, std::memory_order_relaxed);
      return claim_in_one_shard(bank, secondary, buffer, record);
    }
    return false;
  }

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
        slot = GlobalAllocationSlot{};
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

// Implements optional live-allocation ownership tracking.

bool hardened_allocation_registry_enabled() noexcept { return true; }

struct LiveAllocationRecord {
  std::uint8_t *upstream = nullptr;
  std::int64_t upstream_size = 0;
  std::int64_t requested_size = 0;
  std::int64_t alignment = 0;
};

class LiveAllocationRegistry {
public:
  explicit LiveAllocationRegistry(bool thread_safe = true)
      : thread_safe_(thread_safe) {
    try {
      if (thread_safe_) {
        shared_ = std::make_unique<SharedState>();
      } else {
        local_ = std::make_unique<LocalState>();
      }
    } catch (...) {
      // The registry is part of the hardened ownership contract. Allocation
      // attempts fail closed when its state could not be created.
    }
  }

  bool register_allocation(std::uint8_t *buffer,
                           LiveAllocationRecord record) noexcept {
    if (!hardened_allocation_registry_enabled()) {
      return true;
    }
    try {
      if (!thread_safe_) {
        return local_ && local_->live.emplace(buffer, record).second;
      }
      if (!shared_) {
        return false;
      }
      auto &shard = shard_for(buffer);
      std::lock_guard lock(shard.mutex);
      InlineAllocationSlot *empty_slot = nullptr;
      for (auto &slot : shard.inline_live) {
        if (slot.buffer == buffer) {
          return false;
        }
        if (!slot.buffer && !empty_slot) {
          empty_slot = &slot;
        }
      }
      if (shard.overflow_live.contains(buffer)) {
        return false;
      }
      if (empty_slot) {
        empty_slot->record = record;
        empty_slot->buffer = buffer;
        return true;
      }
      return shard.overflow_live.emplace(buffer, record).second;
    } catch (...) {
      return false;
    }
  }

  bool claim_allocation(std::uint8_t *buffer,
                        LiveAllocationRecord *record) noexcept {
    if (!hardened_allocation_registry_enabled()) {
      return true;
    }
    if (!thread_safe_) {
      if (!local_) {
        return false;
      }
      const auto found = local_->live.find(buffer);
      if (found == local_->live.end()) {
        return false;
      }
      if (record) {
        *record = found->second;
      }
      local_->live.erase(found);
      maybe_release_buckets(local_->live);
      return true;
    }
    if (!shared_) {
      return false;
    }
    auto &shard = shard_for(buffer);
    std::lock_guard lock(shard.mutex);
    for (auto &slot : shard.inline_live) {
      if (slot.buffer != buffer) {
        continue;
      }
      if (record) {
        *record = slot.record;
      }
      slot.buffer = nullptr;
      slot.record = LiveAllocationRecord{};
      return true;
    }
    const auto found = shard.overflow_live.find(buffer);
    if (found == shard.overflow_live.end()) {
      return false;
    }
    if (record) {
      *record = found->second;
    }
    shard.overflow_live.erase(found);
    maybe_release_buckets(shard.overflow_live);
    return true;
  }

private:
  static constexpr std::size_t kShardCount = 256;
  static constexpr std::size_t kInlineRecordsPerShard = 8;
  static constexpr std::size_t kRetainedBucketsPerShard = 256;
  static_assert((kShardCount & (kShardCount - 1U)) == 0U);

  using AllocationMap =
      std::unordered_map<std::uint8_t *, LiveAllocationRecord>;

  static void maybe_release_buckets(AllocationMap &live) noexcept {
    if (live.empty() && live.bucket_count() > kRetainedBucketsPerShard) {
      AllocationMap empty;
      live.swap(empty);
    }
  }

  struct InlineAllocationSlot final {
    std::uint8_t *buffer = nullptr;
    LiveAllocationRecord record;
  };
  struct Shard final {
    std::mutex mutex;
    std::array<InlineAllocationSlot, kInlineRecordsPerShard> inline_live{};
    AllocationMap overflow_live;
  };
  struct SharedState final {
    std::array<Shard, kShardCount> shards{};
  };
  struct LocalState final {
    AllocationMap live;
  };

  [[nodiscard]] static std::size_t
  shard_index(const std::uint8_t *buffer) noexcept {
    const auto address = reinterpret_cast<std::uintptr_t>(buffer);
    const auto mixed = (address >> 6U) ^ (address >> 17U) ^ (address >> 29U);
    return static_cast<std::size_t>(mixed) & (kShardCount - 1U);
  }

  [[nodiscard]] Shard &shard_for(const std::uint8_t *buffer) noexcept {
    return shared_->shards[shard_index(buffer)];
  }

  bool thread_safe_ = true;
  std::unique_ptr<SharedState> shared_;
  std::unique_ptr<LocalState> local_;
};

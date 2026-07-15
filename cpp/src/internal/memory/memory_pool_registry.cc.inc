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
  bool register_allocation(std::uint8_t *buffer,
                           LiveAllocationRecord record) noexcept {
    if (!hardened_allocation_registry_enabled()) {
      return true;
    }
    try {
      std::lock_guard lock(mutex_);
      return live_.emplace(buffer, record).second;
    } catch (...) {
      return false;
    }
  }

  bool claim_allocation(std::uint8_t *buffer,
                        LiveAllocationRecord *record) noexcept {
    if (!hardened_allocation_registry_enabled()) {
      return true;
    }
    std::lock_guard lock(mutex_);
    const auto found = live_.find(buffer);
    if (found == live_.end()) {
      return false;
    }
    if (record) {
      *record = found->second;
    }
    live_.erase(found);
    // A diagnostic run can briefly track millions of allocations. Release a
    // large empty bucket array instead of retaining that high-water mark for
    // the lifetime of the process or operation pool.
    if (live_.empty() && live_.bucket_count() > kRetainedBucketLimit) {
      decltype(live_) empty;
      live_.swap(empty);
    }
    return true;
  }

private:
  static constexpr std::size_t kRetainedBucketLimit = 4096;
  std::mutex mutex_;
  std::unordered_map<std::uint8_t *, LiveAllocationRecord> live_;
};

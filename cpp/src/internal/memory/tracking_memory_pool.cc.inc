// Implements operation-scoped quota and allocation tracking.

class TrackingMemoryPool final : public MemoryPool {
public:
  TrackingMemoryPool(std::shared_ptr<MemoryPool> parent, int64_t limit,
                     std::string backend_name, bool thread_safe_registry)
      : parent_(std::move(parent)), limit_(limit),
        backend_name_(std::move(backend_name)),
        live_allocations_(thread_safe_registry) {}

  sanitize::Status Allocate(int64_t size, int64_t alignment,
                            uint8_t **out) override {
    if (!out) {
      return sanitize::Status::Invalid("Allocate: out is null");
    }
    *out = nullptr;
    if (!parent_) {
      return sanitize::Status::Invalid("Allocate: parent pool is null");
    }
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
    std::size_t total_size = 0;
    if (!checked_allocation_size(
            static_cast<std::size_t>(size), normalized_alignment,
            sizeof(TrackingAllocationHeader), &total_size) ||
        total_size >
            static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
      return sanitize::Status::OutOfMemory("Allocate: size overflow");
    }
    const auto quota_charge = static_cast<int64_t>(total_size);
    auto charged = charged_bytes_.load(std::memory_order_relaxed);
    for (;;) {
      if (charged > std::numeric_limits<int64_t>::max() - quota_charge) {
        return sanitize::Status::OutOfMemory(
            "memory quota accounting overflow");
      }
      const auto next = charged + quota_charge;
      const auto limit = limit_.load(std::memory_order_relaxed);
      if (limit > 0 && next > limit) {
        return sanitize::Status::OutOfMemory(
            "memory_limit_bytes limit exceeded including allocator overhead: ",
            next, " bytes > ", limit, " bytes");
      }
      if (charged_bytes_.compare_exchange_weak(charged, next,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
        break;
      }
    }

    auto current = bytes_allocated_.load(std::memory_order_relaxed);
    for (;;) {
      if (current > std::numeric_limits<int64_t>::max() - size) {
        charged_bytes_.fetch_sub(quota_charge, std::memory_order_relaxed);
        return sanitize::Status::OutOfMemory("memory accounting overflow");
      }
      const auto next = current + size;
      if (bytes_allocated_.compare_exchange_weak(current, next,
                                                 std::memory_order_relaxed,
                                                 std::memory_order_relaxed)) {
        current = next;
        break;
      }
    }

    uint8_t *upstream = nullptr;
    const auto upstream_alignment = static_cast<int64_t>(
        std::max(normalized_alignment, alignof(std::max_align_t)));
    auto status = parent_->Allocate(static_cast<int64_t>(total_size),
                                    upstream_alignment, &upstream);
    if (!status.ok()) {
      bytes_allocated_.fetch_sub(size, std::memory_order_relaxed);
      charged_bytes_.fetch_sub(quota_charge, std::memory_order_relaxed);
      return status;
    }
    const auto base = reinterpret_cast<std::uintptr_t>(upstream) +
                      sizeof(TrackingAllocationHeader);
    const auto aligned = align_up(base, normalized_alignment);
    auto *header = reinterpret_cast<TrackingAllocationHeader *>(
        aligned - sizeof(TrackingAllocationHeader));
    *header = TrackingAllocationHeader{
        .upstream = upstream,
        .magic = kTrackingAllocationMagic,
        .upstream_size = static_cast<int64_t>(total_size),
        .requested_size = size,
        .upstream_alignment = upstream_alignment};
    *out = reinterpret_cast<uint8_t *>(aligned);
    write_allocation_guard(*out, static_cast<std::size_t>(size));
    if (!live_allocations_.register_allocation(
            *out, LiveAllocationRecord{
                      .upstream = upstream,
                      .upstream_size = static_cast<std::int64_t>(total_size),
                      .requested_size = size,
                      .alignment = upstream_alignment})) {
      header->magic = 0;
      header->upstream = nullptr;
      parent_->Free(upstream, static_cast<int64_t>(total_size),
                    upstream_alignment);
      bytes_allocated_.fetch_sub(size, std::memory_order_relaxed);
      charged_bytes_.fetch_sub(quota_charge, std::memory_order_relaxed);
      *out = nullptr;
      return sanitize::Status::OutOfMemory(
          "Allocate: unable to register hardened allocation ownership");
    }
    allocation_count_.fetch_add(1, std::memory_order_relaxed);
    update_peak(&max_memory_, current);
    return sanitize::Status::OK();
  }

  void Free(uint8_t *buffer, int64_t size,
            int64_t alignment) noexcept override {
    if (!buffer || !parent_) {
      return;
    }
    LiveAllocationRecord record;
    const bool registry_enabled = hardened_allocation_registry_enabled();
    if (!live_allocations_.claim_allocation(buffer, &record)) {
      invalid_free_count_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    auto *header = reinterpret_cast<TrackingAllocationHeader *>(
        reinterpret_cast<std::uintptr_t>(buffer) -
        sizeof(TrackingAllocationHeader));
    if (registry_enabled) {
      if (header->magic != kTrackingAllocationMagic ||
          header->upstream != record.upstream ||
          header->upstream_size != record.upstream_size ||
          header->requested_size != record.requested_size ||
          header->upstream_alignment != record.alignment) {
        corruption_count_.fetch_add(1, std::memory_order_relaxed);
      }
    } else if (header->magic != kTrackingAllocationMagic || !header->upstream ||
               header->requested_size <= 0 || header->upstream_size <= 0) {
      invalid_free_count_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    const auto expected_size =
        registry_enabled ? record.requested_size : header->requested_size;
    if (size > 0 && size != expected_size) {
      size_mismatch_count_.fetch_add(1, std::memory_order_relaxed);
    }
    std::size_t normalized_alignment = 0;
    if (alignment > 0 &&
        normalize_alignment(alignment, &normalized_alignment) &&
        static_cast<int64_t>(normalized_alignment) !=
            (registry_enabled ? record.alignment
                              : header->upstream_alignment)) {
      size_mismatch_count_.fetch_add(1, std::memory_order_relaxed);
    }
    auto *upstream = registry_enabled ? record.upstream : header->upstream;
    const auto upstream_size =
        registry_enabled ? record.upstream_size : header->upstream_size;
    const auto requested_size =
        registry_enabled ? record.requested_size : header->requested_size;
    const auto upstream_alignment =
        registry_enabled ? record.alignment : header->upstream_alignment;
    if (!allocation_guard_is_valid(buffer,
                                   static_cast<std::size_t>(requested_size))) {
      corruption_count_.fetch_add(1, std::memory_order_relaxed);
    }
    if (secure_memory_cleanup_enabled() && !parent_->wipes_memory_on_free()) {
      secure_zero_memory(buffer, static_cast<std::size_t>(requested_size));
    }
    header->magic = 0;
    header->upstream = nullptr;
    bytes_allocated_.fetch_sub(requested_size, std::memory_order_relaxed);
    charged_bytes_.fetch_sub(upstream_size, std::memory_order_relaxed);
    allocation_count_.fetch_sub(1, std::memory_order_relaxed);
    parent_->Free(upstream, upstream_size, upstream_alignment);
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
  [[nodiscard]] int64_t limit_bytes() const override {
    return limit_.load(std::memory_order_relaxed);
  }
  void SetLimit(int64_t limit) noexcept override {
    limit_.store(std::max<int64_t>(1, limit), std::memory_order_relaxed);
  }
  [[nodiscard]] bool wipes_memory_on_free() const noexcept override {
    return secure_memory_cleanup_enabled();
  }
  [[nodiscard]] std::string backend_name() const override {
    return backend_name_;
  }

private:
  std::shared_ptr<MemoryPool> parent_;
  std::atomic<int64_t> limit_{-1};
  std::string backend_name_;
  std::atomic<int64_t> bytes_allocated_{0};
  std::atomic<int64_t> charged_bytes_{0};
  std::atomic<int64_t> max_memory_{0};
  std::atomic<int64_t> allocation_count_{0};
  std::atomic<int64_t> invalid_free_count_{0};
  std::atomic<int64_t> size_mismatch_count_{0};
  std::atomic<int64_t> corruption_count_{0};
  LiveAllocationRegistry live_allocations_;
};

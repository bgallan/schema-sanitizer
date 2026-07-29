// Defines compact immutable packet-local evidence for parallel inference.
#pragma once

#include "internal/inference/statistics/state.hh"
#include "internal/materialization/ingest_stream/parallel_packets.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/runtime/execution_policy.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

#include "internal/runtime/thread_compat.hh"
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <string>
#include <string_view>
#include <vector>

namespace sanitize::internal {

// Compact packet-local key table. Key bytes are stored once in a tracked
// contiguous buffer, nodes retain 32-bit indices, and open-addressing slots
// avoid one allocator node per distinct key.
class InferenceEvidenceKeys final {
public:
  explicit InferenceEvidenceKeys(std::pmr::memory_resource *resource)
      : bytes_(resource), entries_(resource), slots_(resource) {}

  sanitize::Result<std::uint32_t> Intern(std::string_view value);
  [[nodiscard]] std::string_view View(std::uint32_t index) const noexcept;
  [[nodiscard]] StrId Resolve(std::uint32_t index,
                              StringInterner *strings) const;
  [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }

private:
  struct Entry final {
    std::uint32_t hash = 0;
    std::uint32_t offset = 0;
    std::uint32_t size = 0;
    mutable StrId resolved_id = std::numeric_limits<StrId>::max();
  };

  sanitize::Status EnsureCapacity(std::size_t required_entries);
  void InsertSlot(std::pmr::vector<std::uint32_t> *slots,
                  std::uint32_t entry_index) const noexcept;
  [[nodiscard]] std::uint32_t Hash(std::string_view value) const noexcept;
  [[nodiscard]] std::uint32_t Find(std::string_view value,
                                   std::uint32_t hash) const noexcept;

  std::pmr::vector<char> bytes_;
  std::pmr::vector<Entry> entries_;
  std::pmr::vector<std::uint32_t> slots_;
};

// One preorder node. Direct children start at index + 1 and are visited by
// jumping each child's subtree_end, avoiding one vector allocation per value.
struct InferenceEvidenceNode {
  enum class Kind : std::uint8_t {
    kNull = 0,
    kScalar,
    kObject,
    kArray,
    kFlattened,
  };

  explicit InferenceEvidenceNode(std::pmr::memory_resource *) noexcept {}

  InferenceEvidenceNode(InferenceEvidenceNode &&) noexcept = default;
  InferenceEvidenceNode &operator=(InferenceEvidenceNode &&) noexcept = default;
  InferenceEvidenceNode(const InferenceEvidenceNode &) = delete;
  InferenceEvidenceNode &operator=(const InferenceEvidenceNode &) = delete;

  [[nodiscard]] bool empty_container(std::size_t index) const noexcept {
    return (kind == Kind::kObject || kind == Kind::kArray) &&
           subtree_end == index + 1;
  }

  std::uint32_t subtree_end = 0;
  std::uint32_t scalar_kind_mask = 0;
  std::uint32_t key_index = std::numeric_limits<std::uint32_t>::max();
  Kind kind = Kind::kNull;
};

static_assert(sizeof(InferenceEvidenceNode) <= 16U);

// Identifies one row inside the packet-level preorder node vector.
struct InferenceEvidenceRow {
  std::uint32_t begin = 0;
  std::uint32_t end = 0;
  std::uint32_t source_bytes = 0;
};

static_assert(sizeof(InferenceEvidenceRow) <= 12U);

// One bounded packet-local root scalar field aggregate. The common narrow
// JSONL path stays entirely inline. Wider flat schemas grow into a packet-local
// tracked overflow vector instead of falling back to the much larger generic
// preorder evidence representation.
inline constexpr std::size_t kInlineFlatInferenceFields = 16;
inline constexpr std::size_t kMaxFlatInferenceFields = 512;
inline constexpr std::size_t kInlineFlatInferenceKeyBytes = 64;

struct FlatInferenceField {
  [[nodiscard]] std::string_view key() const noexcept {
    return std::string_view(key_bytes.data(), key_size);
  }

  [[nodiscard]] bool matches(std::string_view value) const noexcept {
    return key() == value;
  }

  [[nodiscard]] bool assign_key(std::string_view value) noexcept {
    if (value.size() > key_bytes.size()) {
      return false;
    }
    std::copy(value.begin(), value.end(), key_bytes.begin());
    key_size = value.size();
    return true;
  }

  std::array<char, kInlineFlatInferenceKeyBytes> key_bytes{};
  std::size_t key_size = 0;
  std::uint32_t scalar_kind_mask = 0;
};

struct FlatInferenceOverflow {
  FlatInferenceOverflow(std::shared_ptr<MemoryPool> pool,
                        std::shared_ptr<PoolResource> resource)
      : memory_pool(std::move(pool)), arena(std::move(resource)),
        fields(arena.get()) {}

  std::shared_ptr<MemoryPool> memory_pool;
  std::shared_ptr<PoolResource> arena;
  std::pmr::vector<FlatInferenceField> fields;
};

struct FlatInferenceStorage {
  sanitize::Status ensure_field(std::size_t index,
                                const std::shared_ptr<MemoryPool> &parent_pool,
                                std::int64_t packet_memory_limit) {
    if (index >= kMaxFlatInferenceFields) {
      return sanitize::Status::NotImplemented(
          "flat inference packet exceeds bounded field capacity");
    }
    if (index < kInlineFlatInferenceFields) {
      return sanitize::Status::OK();
    }
    try {
      if (!overflow) {
        auto pool = make_tracking_memory_pool(
            parent_pool, packet_memory_limit,
            "schema_sanitizer::FlatInferenceEvidencePacket",
            /*thread_safe_registry=*/false);
        auto resource = std::make_shared<PoolResource>(pool);
        overflow = std::make_unique<FlatInferenceOverflow>(std::move(pool),
                                                           std::move(resource));
        overflow->fields.reserve(kInlineFlatInferenceFields);
      }
      const auto overflow_index = index - kInlineFlatInferenceFields;
      while (overflow->fields.size() <= overflow_index) {
        overflow->fields.emplace_back();
      }
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "flat inference overflow allocation failed");
    }
    return sanitize::Status::OK();
  }

  [[nodiscard]] FlatInferenceField &field(std::size_t index) noexcept {
    return index < inline_fields.size()
               ? inline_fields[index]
               : overflow->fields[index - inline_fields.size()];
  }

  [[nodiscard]] const FlatInferenceField &
  field(std::size_t index) const noexcept {
    return index < inline_fields.size()
               ? inline_fields[index]
               : overflow->fields[index - inline_fields.size()];
  }

  std::array<FlatInferenceField, kInlineFlatInferenceFields> inline_fields{};
  std::unique_ptr<FlatInferenceOverflow> overflow;
  std::size_t field_count = 0;
};

// Owns either bounded flat evidence or generic rows/nodes. Narrow flat storage
// is fixed-size; wide overflow and generic nodes use packet-local tracked PMR.
struct InferenceEvidencePacket {
  InferenceEvidencePacket()
      : keys(std::pmr::null_memory_resource()),
        rows(std::pmr::null_memory_resource()),
        nodes(std::pmr::null_memory_resource()) {}

  InferenceEvidencePacket(std::shared_ptr<MemoryPool> pool,
                          std::shared_ptr<PoolResource> resource)
      : memory_pool(std::move(pool)), arena(std::move(resource)),
        keys(arena.get()), rows(arena.get()), nodes(arena.get()) {}

  InferenceEvidencePacket(InferenceEvidencePacket &&) noexcept = default;
  InferenceEvidencePacket &
  operator=(InferenceEvidencePacket &&) noexcept = default;
  InferenceEvidencePacket(const InferenceEvidencePacket &) = delete;
  InferenceEvidencePacket &operator=(const InferenceEvidencePacket &) = delete;

  std::shared_ptr<MemoryPool> memory_pool;
  std::shared_ptr<PoolResource> arena;
  InferenceEvidenceKeys keys;
  std::pmr::vector<InferenceEvidenceRow> rows;
  std::pmr::vector<InferenceEvidenceNode> nodes;
  std::unique_ptr<FlatInferenceStorage> flat_storage;
  std::size_t flat_row_count = 0;
  std::size_t flat_source_bytes = 0;
  bool flat_scalar_aggregate = false;
  bool trusted_stats_reduction = false;
};

// Builds compact evidence packets in worker-private parser state.
class ParallelInferenceEvidenceBuilder final {
public:
  static sanitize::Result<std::shared_ptr<ParallelInferenceEvidenceBuilder>>
  Make(std::string_view frontend_name, const PreparedOptions *opts,
       std::shared_ptr<void> operation_memory_pool,
       const ExecutionPolicy &policy);

  ~ParallelInferenceEvidenceBuilder();

  sanitize::Result<InferenceEvidencePacket>
  Build(OwnedRowPacket &&owned, std::size_t worker_index,
        sanitize::internal::StopToken stop);

private:
  struct WorkerState;

  ParallelInferenceEvidenceBuilder(std::string frontend_name,
                                   const PreparedOptions *opts,
                                   std::shared_ptr<MemoryPool> parent_pool,
                                   std::int64_t packet_memory_limit);

  sanitize::Status append_row(const RowRef &row, WorkerState *worker,
                              InferenceEvidencePacket *packet,
                              sanitize::internal::StopToken stop) const;

  sanitize::Status append_flat_row(const RowRef &row, WorkerState *worker,
                                   InferenceEvidencePacket *packet,
                                   sanitize::internal::StopToken stop) const;

  std::string frontend_name_;
  const PreparedOptions *opts_ = nullptr;
  std::shared_ptr<MemoryPool> parent_pool_;
  std::int64_t packet_memory_limit_ = 1;
  bool parse_json_raw_ = false;
  std::vector<std::unique_ptr<WorkerState>> workers_;
};

// Applies one compact evidence row in shape-then-statistics order.
sanitize::Status reduce_inference_evidence_row(
    InferenceContext *ctx, const InferenceEvidencePacket &packet,
    const InferenceEvidenceRow &row, const PreparedOptions &opts,
    IngestDiagnostics *diagnostics);

} // namespace sanitize::internal

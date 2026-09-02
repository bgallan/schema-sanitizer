// Prefetches independent JSONL path frontends while preserving file order. The
// pipeline preserves source offsets and ownership while enforcing plan order
// and memory bounds.

#include "frontends/builtin_frontends.hh"

#include "internal/memory/memory_budget.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/runtime/ordered_executor.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/chunk_source.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace sanitize::internal {
namespace {

struct GroupBatchStorage {

  /// Creates pooled storage that retains every prefetched child batch in output
  /// order.
  GroupBatchStorage(std::shared_ptr<void> pool,
                    std::shared_ptr<PoolResource> resource)
      : pool_keepalive(std::move(pool)),
        resource_keepalive(std::move(resource)),
        batches(resource_keepalive.get()) {}

  std::shared_ptr<void> pool_keepalive;
  std::shared_ptr<PoolResource> resource_keepalive;
  std::pmr::vector<RowBatch> batches;
};

struct FetchedBatch {
  RowBatch batch;
  bool exhausted = false;
};

struct PendingFetch {
  sanitize::Status status = sanitize::Status::OK();
  std::optional<FetchedBatch> value;
};

struct FetchTask {
  std::size_t child_index = 0;
  std::int64_t capacity = 0;
};

struct JsonlPathChild {
  std::string path;
  std::string source_name;
  FrontendHandle frontend;
  bool first_fetch_consumed = false;
};

using FetchExecutor = OrderedExecutor<FetchTask, FetchedBatch>;

class JsonlPathGroupFrontend final {
public:
  /// Initializes ordered JSONL path traversal with bounded prefetch and stable
  /// source names.
  JsonlPathGroupFrontend(std::vector<std::string> paths,
                         std::vector<std::string> source_names, Options options,
                         std::shared_ptr<OperationTaskArena> task_arena)
      : options_(std::move(options)), task_arena_(std::move(task_arena)),
        coordination_resource_(std::make_shared<PoolResource>()),
        pending_(
            std::make_unique<PendingVector>(coordination_resource_.get())) {
    children_.reserve(paths.size());
    pending_->resize(paths.size());
    for (std::size_t index = 0; index < paths.size(); ++index) {
      children_.push_back(
          JsonlPathChild{.path = std::move(paths[index]),
                         .source_name = std::move(source_names[index]),
                         .frontend = FrontendHandle{},
                         .first_fetch_consumed = false});
    }
    reset();
  }

  /// Rewinds the JSON text frontend to its initial input position and clears
  /// per-pass state.
  void reset() noexcept {
    active_executor_.reset();
    active_coordination_charge_.reset();
    active_start_ = 0;
    active_size_ = 0;
    active_taken_ = 0;
    index_ = 0;
    prefetch_end_ = 0;
    done_ = children_.empty();
    for (auto &pending : *pending_) {
      pending.reset();
    }
    for (auto &child : children_) {
      child.first_fetch_consumed = false;
      if (child.frontend) {
        child.frontend.reset();
      }
    }
  }

  /// Propagates the compiled plan to all open grouped JSON sources.
  void set_plan(const CompiledPlan *plan) noexcept {
    plan_ = plan;
    for (auto &child : children_) {
      if (child.frontend) {
        child.frontend.set_plan(plan_);
      }
    }
  }

  /// Propagates the materialization mode to all open grouped JSON sources.
  void set_materialization_mode(FrontendMaterializationMode mode) noexcept {
    materialization_mode_ = mode;
    for (auto &child : children_) {
      if (child.frontend) {
        child.frontend.set_materialization_mode(mode);
      }
    }
  }

  /// Rebuilds grouped coordination storage and updates every open JSON source.
  void set_memory_pool(std::shared_ptr<void> pool) noexcept {
    active_executor_.reset();
    active_coordination_charge_.reset();
    try {
      auto resource = std::make_shared<PoolResource>(pool);
      auto pending = std::make_unique<PendingVector>(resource.get());
      pending->resize(children_.size());
      pending_.reset();
      coordination_resource_ = std::move(resource);
      pending_ = std::move(pending);
      memory_pool_ = std::move(pool);
      pool_status_ = sanitize::Status::OK();
    } catch (const std::bad_alloc &) {
      pool_status_ = sanitize::Status::OutOfMemory(
          "grouped JSONL coordination allocation failed");
      return;
    }
    for (auto &child : children_) {
      if (child.frontend) {
        child.frontend.set_memory_pool(memory_pool_);
      }
    }
  }

  /// Propagates task-arena ownership to all open grouped JSON sources.
  void set_task_arena(std::shared_ptr<OperationTaskArena> task_arena) noexcept {
    task_arena_ = std::move(task_arena);
    for (auto &child : children_) {
      if (child.frontend) {
        child.frontend.set_task_arena(task_arena_);
      }
    }
  }

  /// Reads and materializes the next bounded row batch from the JSON text
  /// frontend.
  sanitize::Result<RowBatch> next_batch(std::int64_t capacity) try {
    RowBatch out;
    if (capacity <= 0 || done_) {
      return out;
    }
    if (!pool_status_.ok()) {
      return pool_status_;
    }

    std::shared_ptr<GroupBatchStorage> storage;
    try {
      storage = std::make_shared<GroupBatchStorage>(memory_pool_,
                                                    coordination_resource_);
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "grouped JSONL batch-owner allocation failed");
    }
    const auto group_budget =
        memory_budget_from_limit(options_.memory_limit_bytes);
    const auto max_retained_child_batches = static_cast<std::size_t>(
        std::max<std::int64_t>(1, group_budget.async_prefetch_files));
    std::int64_t produced = 0;
    while (produced < capacity && !done_) {
      SAN_RETURN_NOT_OK(ensure_prefetched(capacity));
      if (done_) {
        break;
      }

      if ((*pending_)[index_]) {
        auto &pending = *(*pending_)[index_];
        if (!pending.status.ok()) {
          if (produced == 0) {
            return pending.status;
          }
          break;
        }
        FetchedBatch fetched = std::move(*pending.value);
        (*pending_)[index_].reset();
        children_[index_].first_fetch_consumed = true;
        const bool can_return_direct =
            produced == 0 && !fetched.exhausted && storage->batches.empty();
        if (can_return_direct) {
          return std::move(fetched.batch);
        }
        append_batch(&out, storage.get(), std::move(fetched.batch), &produced);
        if (fetched.exhausted) {
          advance_child();
          if (storage->batches.size() >= max_retained_child_batches) {
            break;
          }
          continue;
        }
        break;
      }

      const std::int64_t wanted = capacity - produced;
      auto next = children_[index_].frontend.next_batch(wanted);
      if (!next.ok()) {
        if (produced == 0) {
          return next.status();
        }
        PendingFetch deferred;
        deferred.status = next.status();
        (*pending_)[index_] = std::move(deferred);
        break;
      }
      RowBatch batch = std::move(next).ValueOrDie();
      const bool exhausted =
          static_cast<std::int64_t>(batch.rows.size()) < wanted;
      append_batch(&out, storage.get(), std::move(batch), &produced);
      if (exhausted) {
        advance_child();
        if (storage->batches.size() >= max_retained_child_batches) {
          break;
        }
        continue;
      }
      break;
    }

    if (!out.rows.empty()) {
      out.owner = std::move(storage);
    }
    return out;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "grouped JSONL coordination exceeds memory_limit_bytes");
  }

private:
  /// Lazily constructs one child frontend and applies the group's active
  /// runtime settings.
  sanitize::Status open_child(std::size_t child_index) {
    auto &child = children_[child_index];
    if (child.frontend) {
      return sanitize::Status::OK();
    }
    std::vector<std::string> paths;
    std::vector<std::string> source_names;
    paths.push_back(child.path);
    source_names.push_back(child.source_name);
    SAN_ASSIGN_OR_RAISE(
        auto source,
        sanitize::chunk_source_from_paths_with_source_names_encoding(
            std::move(paths), std::move(source_names), "\n",
            options_.input_text_encoding, options_.memory_limit_bytes));
    child.frontend = make_jsonl_frontend(std::move(source), options_);
    if (!child.frontend) {
      return sanitize::Status::Invalid(
          "grouped JSONL child frontend creation failed");
    }
    child.frontend.set_plan(plan_);
    child.frontend.set_materialization_mode(materialization_mode_);
    child.frontend.set_memory_pool(memory_pool_);
    return sanitize::Status::OK();
  }

  /// Opens a child and returns its first batch together with exhaustion state.
  sanitize::Result<FetchedBatch> fetch_first(FetchTask task) {
    SAN_RETURN_NOT_OK(open_child(task.child_index));
    SAN_ASSIGN_OR_RAISE(
        RowBatch batch,
        children_[task.child_index].frontend.next_batch(task.capacity));
    const bool exhausted =
        static_cast<std::int64_t>(batch.rows.size()) < task.capacity;
    return FetchedBatch{.batch = std::move(batch), .exhausted = exhausted};
  }

  /// Ensures the current child has a stored first-fetch result ready for
  /// consumption.
  sanitize::Status ensure_prefetched(std::int64_t capacity) {
    if (index_ >= children_.size()) {
      done_ = true;
      return sanitize::Status::OK();
    }
    if (index_ >= prefetch_end_) {
      SAN_RETURN_NOT_OK(start_prefetch(capacity));
    }
    if ((*pending_)[index_] || children_[index_].first_fetch_consumed) {
      return sanitize::Status::OK();
    }
    if (!active_executor_) {
      return sanitize::Status::Invalid(
          "grouped JSONL prefetch result is unavailable");
    }
    SAN_ASSIGN_OR_RAISE(auto outcome, active_executor_->TakeNext());
    const std::size_t child = active_start_ + active_taken_;
    ++active_taken_;
    store_fetch(child, std::move(outcome.result));
    return sanitize::Status::OK();
  }

  /// Starts a bounded cohort of ordered first-batch fetches at the current
  /// child.
  sanitize::Status start_prefetch(std::int64_t capacity) {
    active_executor_.reset();
    active_coordination_charge_.reset();
    active_start_ = index_;
    active_size_ = 0;
    active_taken_ = 0;

    const std::size_t arena_workers =
        task_arena_ ? task_arena_->worker_count() : std::size_t{1};
    const auto budget = memory_budget_from_limit(options_.memory_limit_bytes);
    const std::size_t budget_workers = static_cast<std::size_t>(
        std::max<std::int64_t>(1, budget.async_concurrency));
    const std::size_t cohort_size = std::min<std::size_t>(
        children_.size() - index_, std::min(arena_workers, budget_workers));
    try {
      // OrderedExecutor deliberately uses bounded standard-library queues.
      // Reserve their conservative coordination footprint against the same
      // operation pool so that path prefetch cannot grow outside the public
      // memory budget even though those implementation containers are not PMR.
      constexpr std::size_t kCoordinationBytesPerChild = 1024U;
      if (cohort_size > std::numeric_limits<std::size_t>::max() /
                            kCoordinationBytesPerChild) {
        return sanitize::Status::OutOfMemory(
            "grouped JSONL coordination size overflow");
      }
      active_coordination_charge_ =
          std::make_unique<std::pmr::vector<std::byte>>(
              coordination_resource_.get());
      active_coordination_charge_->resize(cohort_size *
                                          kCoordinationBytesPerChild);
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "grouped JSONL prefetch coordination exceeds memory_limit_bytes");
    }
    const std::int64_t total_prefetch_rows =
        capacity > (std::numeric_limits<std::int64_t>::max() / 2)
            ? capacity
            : capacity * 2;
    const std::int64_t uniform_capacity = std::max<std::int64_t>(
        1, total_prefetch_rows / static_cast<std::int64_t>(cohort_size));
    const bool use_head_start = arena_workers >= 16 && cohort_size > 1;
    const std::int64_t head_capacity =
        use_head_start
            ? std::max<std::int64_t>(
                  1,
                  std::min<std::int64_t>(
                      uniform_capacity,
                      std::max<std::int64_t>(4096, (uniform_capacity * 3) / 4)))
            : uniform_capacity;
    const std::int64_t trailing_capacity =
        use_head_start ? std::max<std::int64_t>(
                             1, (total_prefetch_rows - head_capacity) /
                                    static_cast<std::int64_t>(cohort_size - 1))
                       : uniform_capacity;
    const auto capacity_for =
        [head_capacity, trailing_capacity](std::size_t ordinal) noexcept {
          return ordinal == 0 ? head_capacity : trailing_capacity;
        };
    prefetch_end_ = index_ + cohort_size;
    for (std::size_t child = index_; child < prefetch_end_; ++child) {
      (*pending_)[child].reset();
      children_[child].first_fetch_consumed = false;
    }

    if (!task_arena_ || task_arena_->inline_mode() || cohort_size == 1) {
      for (std::size_t child = index_; child < prefetch_end_; ++child) {
        store_fetch(child, fetch_first(FetchTask{
                               .child_index = child,
                               .capacity = capacity_for(child - index_)}));
      }
      return sanitize::Status::OK();
    }

    SAN_ASSIGN_OR_RAISE(active_executor_,
                        FetchExecutor::Make(
                            cohort_size, cohort_size, cohort_size,
                            [this](FetchTask &&task, std::size_t,
                                   sanitize::internal::StopToken) {
                              return fetch_first(std::move(task));
                            },
                            task_arena_, TaskArenaLane::kOutput,
                            TaskTelemetryKind::kInput));
    active_size_ = cohort_size;
    for (std::size_t ordinal = 0; ordinal < cohort_size; ++ordinal) {
      SAN_RETURN_NOT_OK(active_executor_->Submit(FetchExecutor::Packet{
          .ordinal = ordinal,
          .payload = FetchTask{.child_index = index_ + ordinal,
                               .capacity = capacity_for(ordinal)}}));
    }
    return active_executor_->FinishSubmission();
  }

  /// Stores either a fetched batch or its failure in the child's pending slot.
  void store_fetch(std::size_t child, sanitize::Result<FetchedBatch> result) {
    PendingFetch pending;
    if (result.ok()) {
      pending.value = std::move(result).ValueOrDie();
    } else {
      pending.status = result.status();
    }
    (*pending_)[child] = std::move(pending);
  }

  /// Merges a nonempty child batch and retains its owner for the returned row
  /// views.
  static void append_batch(RowBatch *out, GroupBatchStorage *storage,
                           RowBatch batch, std::int64_t *produced) {
    if (batch.rows.empty()) {
      return;
    }
    *produced += static_cast<std::int64_t>(batch.rows.size());
    out->reader_diagnostics.merge(batch.reader_diagnostics);
    out->rows.insert(out->rows.end(), batch.rows.begin(), batch.rows.end());
    storage->batches.push_back(std::move(batch));
  }

  /// Releases an exhausted child and advances ordered traversal to its
  /// successor.
  void advance_child() noexcept {
    if (index_ < children_.size()) {
      // An exhausted child will never be revisited until reset(). Drop its
      // scanner, caches, and retained batch owner immediately instead of
      // multiplying operation-pool retention by the number of input paths.
      (*pending_)[index_].reset();
      children_[index_].frontend = FrontendHandle{};
      children_[index_].first_fetch_consumed = false;
    }
    ++index_;
    if (index_ >= prefetch_end_ && active_taken_ >= active_size_) {
      active_executor_.reset();
      active_coordination_charge_.reset();
      active_start_ = 0;
      active_size_ = 0;
      active_taken_ = 0;
    }
    if (index_ >= children_.size()) {
      done_ = true;
    }
  }

  using PendingVector = std::pmr::vector<std::optional<PendingFetch>>;

  Options options_;
  std::shared_ptr<OperationTaskArena> task_arena_;
  // Allocator owners precede every PMR-backed coordination object so reverse
  // destruction releases containers before their operation pool.
  std::shared_ptr<void> memory_pool_;
  std::shared_ptr<PoolResource> coordination_resource_;
  std::unique_ptr<PendingVector> pending_;
  std::unique_ptr<std::pmr::vector<std::byte>> active_coordination_charge_;
  std::vector<JsonlPathChild> children_;
  sanitize::Status pool_status_ = sanitize::Status::OK();
  const CompiledPlan *plan_ = nullptr;
  FrontendMaterializationMode materialization_mode_ =
      FrontendMaterializationMode::kDefault;
  std::size_t index_ = 0;
  std::size_t prefetch_end_ = 0;
  bool done_ = false;
  std::size_t active_start_ = 0;
  std::size_t active_size_ = 0;
  std::size_t active_taken_ = 0;
  std::unique_ptr<FetchExecutor> active_executor_;
};

/// Rewinds the JSON text frontend to its initial input position and clears
/// per-pass state.
void group_reset(void *self) noexcept {
  static_cast<JsonlPathGroupFrontend *>(self)->reset();
}

/// Reads and materializes the next bounded row batch from the JSON text
/// frontend.
sanitize::Result<RowBatch> group_next_batch(void *self, std::int64_t capacity) {
  return static_cast<JsonlPathGroupFrontend *>(self)->next_batch(capacity);
}

/// Forwards a compiled plan through the grouped JSON frontend callback table.
void group_set_plan(void *self, const CompiledPlan *plan) noexcept {
  static_cast<JsonlPathGroupFrontend *>(self)->set_plan(plan);
}

/// Forwards the materialization mode through the grouped JSON callback table.
void group_set_materialization_mode(void *self,
                                    FrontendMaterializationMode mode) noexcept {
  static_cast<JsonlPathGroupFrontend *>(self)->set_materialization_mode(mode);
}

/// Forwards memory-pool ownership through the grouped JSON callback table.
void group_set_memory_pool(void *self, std::shared_ptr<void> pool) noexcept {
  static_cast<JsonlPathGroupFrontend *>(self)->set_memory_pool(std::move(pool));
}

/// Forwards task-arena ownership through the grouped JSON callback table.
void group_set_task_arena(
    void *self, std::shared_ptr<OperationTaskArena> task_arena) noexcept {
  static_cast<JsonlPathGroupFrontend *>(self)->set_task_arena(
      std::move(task_arena));
}

/// Destroys the heap-owned JSON text frontend state after its final callback
/// completes.
void group_destroy(void *self) noexcept {
  delete static_cast<JsonlPathGroupFrontend *>(self);
}

const FrontendVTable kJsonlPathGroupVTable{
    .reset = &group_reset,
    .next_batch = &group_next_batch,
    .set_plan = &group_set_plan,
    .destroy = &group_destroy,
    .set_memory_pool = &group_set_memory_pool,
    .set_materialization_mode = &group_set_materialization_mode,
    .set_task_arena = &group_set_task_arena,
};

} // namespace

sanitize::Result<FrontendHandle> make_jsonl_path_group_frontend(
    std::vector<std::string> paths, std::vector<std::string> source_names,
    const Options &options, std::shared_ptr<OperationTaskArena> task_arena) {
  if (paths.empty() || paths.size() != source_names.size()) {
    return sanitize::Status::Invalid("invalid grouped JSONL path source");
  }
  std::unique_ptr<JsonlPathGroupFrontend> frontend;
  try {
    frontend = std::unique_ptr<JsonlPathGroupFrontend>(
        new (std::nothrow)
            JsonlPathGroupFrontend(std::move(paths), std::move(source_names),
                                   options, std::move(task_arena)));
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "grouped JSONL coordination allocation failed");
  }
  if (!frontend) {
    return sanitize::Status::OutOfMemory(
        "grouped JSONL frontend allocation failed");
  }
  auto *raw = frontend.release();
  return FrontendHandle{raw, &kJsonlPathGroupVTable};
}

} // namespace sanitize::internal

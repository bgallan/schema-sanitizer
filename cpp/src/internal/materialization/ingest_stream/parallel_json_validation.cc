// Implements worker-side source-ordered JSONL validation and token handoff.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#include "internal/materialization/ingest_stream/parallel_json_validation.hh"

#include "frontends/json/text_row_pipeline.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/json/validated_row.hh"

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <new>
#include <utility>

namespace sanitize::internal {
namespace {

struct ValidatedPacketStorage final {
  /// Initializes ownership for validated row tokens retained by a worker
  /// packet.
  ValidatedPacketStorage(std::shared_ptr<void> operation_pool,
                         std::shared_ptr<const void> source_owner_value)
      : source_owner(std::move(source_owner_value)),
        resource(std::move(operation_pool)), tokens(&resource),
        rows(&resource) {}

  /// Moves validated rows and token ownership into the completed packet after
  /// worker preparation succeeds.
  void finalize() noexcept {
    const auto *base = tokens.data();
    for (auto &row : rows) {
      row.fields = row.field_count == 0 ? nullptr : base + row.field_offset;
    }
  }

  std::shared_ptr<const void> source_owner;
  PoolResource resource;
  std::pmr::vector<JsonValidatedFieldToken> tokens;
  std::pmr::vector<JsonValidatedRowTokens> rows;
};

constexpr auto kRawOnlyFlag = std::to_underlying(RowFlags::kRawOnly);
constexpr auto kTokenFlag = std::to_underlying(RowFlags::kJsonValidatedTokens);
constexpr auto kPlanOrderedTokenFlag =
    std::to_underlying(RowFlags::kJsonPlanOrderedTokens);

/// Checks whether a JSON packet can retain validated fields in compiled-plan
/// order.
[[nodiscard]] bool
plan_order_token_candidate(const sanitize::CompiledPlan &plan) noexcept {
  for (const auto &column : plan.columns) {
    const auto kind = column.logical_type.kind;
    if (column.has_variant_sibling || kind == sanitize::LogicalKind::kStruct ||
        kind == sanitize::LogicalKind::kList) {
      return false;
    }
  }
  return true;
}

} // namespace

struct ParallelJsonRowValidator::WorkerState {
  std::shared_ptr<MemoryPool> memory_pool;
  std::shared_ptr<PoolResource> resource;
  std::unique_ptr<JsonOnDemandDoc> doc;
};

ParallelJsonRowValidator::ParallelJsonRowValidator(
    std::shared_ptr<void> operation_memory_pool,
    std::shared_ptr<const sanitize::CompiledPlan> plan,
    sanitize::OnErrorPolicy on_error) noexcept
    : operation_memory_pool_(std::move(operation_memory_pool)),
      plan_(std::move(plan)), on_error_(on_error),
      plan_order_candidate_(plan_ && plan_order_token_candidate(*plan_)) {}

sanitize::Result<std::shared_ptr<ParallelJsonRowValidator>>
ParallelJsonRowValidator::Make(
    std::shared_ptr<void> operation_memory_pool,
    std::shared_ptr<const sanitize::CompiledPlan> plan,
    sanitize::OnErrorPolicy on_error, const ExecutionPolicy &policy) {
  if (!operation_memory_pool) {
    return sanitize::Status::Invalid(
        "ParallelJsonRowValidator::Make: operation memory pool is null");
  }
  if (!plan) {
    return sanitize::Status::Invalid(
        "ParallelJsonRowValidator::Make: plan is null");
  }
  if (policy.effective_workers <= 1) {
    return sanitize::Status::Invalid(
        "ParallelJsonRowValidator::Make: validation requires at least two "
        "effective workers");
  }

  auto validator = std::shared_ptr<ParallelJsonRowValidator>(
      new (std::nothrow) ParallelJsonRowValidator(
          std::move(operation_memory_pool), std::move(plan), on_error));
  if (!validator) {
    return sanitize::Status::OutOfMemory(
        "ParallelJsonRowValidator::Make: allocation failed");
  }

  auto parent =
      std::static_pointer_cast<MemoryPool>(validator->operation_memory_pool_);
  try {
    validator->workers_.reserve(
        static_cast<std::size_t>(policy.effective_workers));
    for (std::int64_t index = 0; index < policy.effective_workers; ++index) {
      auto state = std::make_unique<WorkerState>();
      state->memory_pool =
          make_tracking_memory_pool(parent, policy.worker_arena_bytes,
                                    "schema_sanitizer::JsonValidationWorker[" +
                                        std::to_string(index) + "]");
      state->resource = std::make_shared<PoolResource>(
          std::static_pointer_cast<void>(state->memory_pool),
          /*recycle_exact_blocks=*/true);
      state->doc = std::make_unique<JsonOnDemandDoc>(state->resource.get());
      validator->workers_.push_back(std::move(state));
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "ParallelJsonRowValidator::Make: worker state allocation failed");
  }
  return validator;
}

sanitize::Result<OwnedRowPacket>
ParallelJsonRowValidator::Validate(JsonValidationTask &&task,
                                   std::size_t worker_index,
                                   sanitize::internal::StopToken stop) {
  if (worker_index >= workers_.size()) {
    return sanitize::Status::Invalid(
        "ParallelJsonRowValidator::Validate: worker index out of range");
  }
  if (task.owned.rows.empty()) {
    return sanitize::Status::Invalid(
        "ParallelJsonRowValidator::Validate: packet contains no rows");
  }
  if (stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "ParallelJsonRowValidator::Validate: stop requested");
  }

  task.owned.json_tokenized_rows = 0;
  task.owned.json_tokenized_fields = 0;
  task.owned.json_plan_ordered_rows = 0;
  task.owned.json_token_fallback_rows = 0;
  task.owned.json_skipped_rows = 0;

  std::shared_ptr<ValidatedPacketStorage> storage;
  try {
    storage = std::make_shared<ValidatedPacketStorage>(operation_memory_pool_,
                                                       task.owned.owner);
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "ParallelJsonRowValidator::Validate: token storage allocation failed");
  }

  bool capture_tokens = task.max_token_fields > 0;
  if (capture_tokens) {
    try {
      storage->rows.reserve(task.owned.rows.size());
    } catch (const std::bad_alloc &) {
      capture_tokens = false;
    }
  }

  auto &doc = *workers_[worker_index]->doc;
  std::size_t write_index = 0;
  for (std::size_t read_index = 0; read_index < task.owned.rows.size();
       ++read_index) {
    if (stop.stop_requested()) {
      return sanitize::Status::Cancelled(
          "ParallelJsonRowValidator::Validate: stop requested");
    }
    auto &row = task.owned.rows[read_index];
    row.flags = kRawOnlyFlag;
    row.direct_ctx = nullptr;
    const auto token_begin = storage->tokens.size();
    auto validation_result =
        validate_json_text_row(&doc, row.raw, row.base_offset,
                               capture_tokens ? &storage->tokens : nullptr,
                               capture_tokens ? task.max_token_fields : 0,
                               plan_order_candidate_ ? plan_.get() : nullptr);
    if (!validation_result.ok()) {
      storage->tokens.resize(token_begin);
      const auto status = validation_result.status();
      if (status.code() != sanitize::StatusCode::kInvalid ||
          json_error_exceeds_hard_safety_limit(status) ||
          on_error_ == sanitize::OnErrorPolicy::kStop) {
        return status;
      }
      if (on_error_ == sanitize::OnErrorPolicy::kSkipRow) {
        ++task.owned.json_skipped_rows;
        continue;
      }
      row.fields = nullptr;
      row.size = 0;
      row.raw = {};
      row.flags = std::to_underlying(RowFlags::kNone);
      row.direct_ctx = nullptr;
    } else {
      const auto validation = std::move(validation_result).ValueOrDie();
      if (!validation.tokenized_object) {
        ++task.owned.json_token_fallback_rows;
      } else {
        try {
          storage->rows.push_back(JsonValidatedRowTokens{
              .fields = nullptr,
              .field_offset = validation.field_offset,
              .field_count = validation.field_count,
          });
        } catch (const std::bad_alloc &) {
          storage->tokens.resize(token_begin);
          capture_tokens = false;
          ++task.owned.json_token_fallback_rows;
        }
        if (storage->tokens.size() != token_begin) {
          row.flags = static_cast<std::uint8_t>(
              kRawOnlyFlag | kTokenFlag |
              (validation.plan_ordered_tokens ? kPlanOrderedTokenFlag : 0));
          row.direct_ctx = &storage->rows.back();
          ++task.owned.json_tokenized_rows;
          task.owned.json_tokenized_fields += validation.field_count;
          if (validation.plan_ordered_tokens) {
            ++task.owned.json_plan_ordered_rows;
          }
        }
      }
    }
    if (write_index != read_index) {
      task.owned.rows[write_index] = row;
    }
    ++write_index;
  }
  task.owned.rows = task.owned.rows.first(write_index);

  storage->finalize();
  task.owned.owner = std::move(storage);
  return std::move(task.owned);
}

} // namespace sanitize::internal

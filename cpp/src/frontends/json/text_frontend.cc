// Materializes one JSON text slice into a planned row batch. The pipeline
// preserves source offsets and ownership while enforcing plan order and memory
// bounds.

#include "frontends/builtin_frontends.hh"
#include "frontends/json/root_field_filter.hh"
#include "frontends/json/text_batch_storage.hh"
#include "frontends/json/text_row_materializer.hh"
#include "frontends/json/text_row_pipeline.hh"
#include "internal/materialization/ingest_stream/column_partition.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/row_scanner.hh" // is_ws
#include "internal/parsing/streaming/json/scanner.hh"
#include "internal/planning/planned_name_matcher.hh"
#include "internal/runtime/execution_policy.hh"
#include "sanitize/core/status.hh"
#include "sanitize/planning/plan.hh"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
namespace sanitize::internal {
namespace {
class JsonTextRows final {
public:
  /// Resolves JSON row validation and materialization policy from prepared
  /// options.
  JsonTextRows(const Options &options, bool require_object_rows,
               bool line_delimited)
      : default_key_(options.default_key_name),
        default_key_hash_(
            sanitize::detail::hash_key64(std::string_view(default_key_))),
        field_name_policy_(options.field_name_policy),
        direct_rows_(static_cast<bool>(options.arrow_schema_contract)),
        parallel_rows_(parallel_json_row_frontend_enabled(options)),
        line_delimited_(line_delimited), on_error_(options.on_error),
        stop_on_error_(options.on_error == OnErrorPolicy::kStop),
        raw_only_(line_delimited && parallel_rows_ &&
                  options.on_error == OnErrorPolicy::kStop),
        require_object_rows_(require_object_rows),
        token_index_max_fields_(
            json_token_index_max_fields(options.memory_limit_bytes)) {}

  /// Refreshes JSON row filtering and materialization for the compiled plan.
  void set_plan(const CompiledPlan *plan) noexcept {
    plan_ = plan;
    refresh_policy();
    root_filter_.reset(plan_, field_name_policy_);
  }

  /// Charges root-field filter storage to the supplied memory pool.
  void set_memory_pool(const std::shared_ptr<void> &pool) noexcept {
    root_filter_.set_memory_pool(pool);
    root_filter_.reset(plan_, field_name_policy_);
  }

  /// Refreshes JSON row policy for the selected materialization mode.
  void set_materialization_mode(FrontendMaterializationMode mode) noexcept {
    mode_ = mode;
    refresh_policy();
  }

  /// Appends one JSON slice using the raw or materialized frontend path.
  sanitize::Status append(JsonTextBatchStorage *storage,
                          const TextSlice &slice) const {
    const std::string_view probe = trim_leading_json_ws(slice.view);
    const bool is_object_row = !probe.empty() && probe.front() == '{';
    if (require_object_rows_ && !is_object_row) {
      return sanitize::Status::Invalid("json_array requires object elements");
    }
    storage->keep_data_owner(slice.owner);
    storage->keep_source_name(slice.source_file_owner);
    uint8_t row_flags =
        plan_ordered_ ? std::to_underlying(RowFlags::kPlanOrdered)
                      : (raw_only_ ? std::to_underlying(RowFlags::kRawOnly)
                                   : std::to_underlying(RowFlags::kNone));
    const void *direct_ctx = nullptr;
    if (raw_only_ && validate_raw_) {
      SAN_ASSIGN_OR_RAISE(
          auto validation,
          validate_json_text_row(&storage->doc, slice.view, slice.base_offset,
                                 &storage->validated_tokens,
                                 storage->max_validated_tokens));
      if (validation.tokenized_object) {
        direct_ctx = storage->retain_validated_row(validation.field_offset,
                                                   validation.field_count);
        if (direct_ctx) {
          row_flags |= std::to_underlying(RowFlags::kJsonValidatedTokens);
        }
      }
    }
    storage->batch.start_row(slice.view, slice.base_offset, row_flags,
                             direct_ctx, slice.source_file);
    sanitize::Status status = sanitize::Status::OK();
    if (plan_ordered_) {
      if (!plan_) {
        status = sanitize::Status::Invalid(
            "JSON plan-ordered row is missing its compiled plan");
      } else {
        status = append_plan_ordered_json_row(
            &storage->doc, &storage->batch, &storage->plan_ordered_scratch,
            *plan_, field_name_policy_, slice.view, slice.base_offset);
      }
    } else if (row_flags == std::to_underlying(RowFlags::kNone)) {
      status = append_materialized_slice(storage, slice);
    }
    if (!status.ok()) {
      storage->batch.abort_current_row();
      return status;
    }
    storage->batch.end_row();
    return sanitize::Status::OK();
  }

  /// Appends a null row at the failed slice's original source offset.
  void append_null_row(JsonTextBatchStorage *storage,
                       const TextSlice &slice) const {
    storage->keep_data_owner(slice.owner);
    storage->keep_source_name(slice.source_file_owner);
    storage->batch.start_row({}, slice.base_offset,
                             std::to_underlying(RowFlags::kNone), nullptr,
                             slice.source_file);
    storage->batch.end_row();
  }

  /// Records JSON row failures while honoring the configured diagnostic policy.
  [[nodiscard]] OnErrorPolicy on_error() const noexcept { return on_error_; }

  /// Returns the field-count ceiling for retaining a validated JSON token
  /// index.
  [[nodiscard]] std::size_t token_index_max_fields() const noexcept {
    return validate_raw_ ? token_index_max_fields_ : 0;
  }

  /// Reports whether this row policy defers object materialization to a later
  /// stage.
  [[nodiscard]] bool emits_deferred_raw_rows() const noexcept {
    const bool deferred_mode =
        mode_ == FrontendMaterializationMode::kDeferredValidationRaw ||
        mode_ == FrontendMaterializationMode::kWorkerAuthoritativeRaw;
    return deferred_mode && raw_only_ && !validate_raw_ && !plan_ordered_;
  }

  /// Reports whether this row policy rejects non-object top-level JSON values.
  [[nodiscard]] bool requires_object_rows() const noexcept {
    return require_object_rows_;
  }

private:
  /// Derives row ordering and validation flags from the active plan and
  /// frontend mode.
  void refresh_policy() noexcept {
    const auto policy =
        resolve_json_text_row_policy(plan_, line_delimited_, stop_on_error_,
                                     direct_rows_, parallel_rows_, mode_);
    plan_ordered_ = policy.plan_ordered;
    raw_only_ = policy.raw_only;
    validate_raw_ = policy.validate_raw;
  }
  struct ObjectEmitContext {
    FlatRowBatch *batch = nullptr;
    const CompiledPlan *plan = nullptr;
    const JsonTextRows *rows = nullptr;
    std::size_t emitted_fields = 0;
  };

  /// Adds the slice's absolute offset when a parser message lacks its own byte
  /// location.
  [[nodiscard]] static sanitize::Status
  prefixed_parse_error(const TextSlice &slice, std::string_view message) {
    if (message.find(" at byte ") != std::string_view::npos) {
      return sanitize::Status::Invalid(message);
    }
    return sanitize::Status::Invalid(
        std::string("JSON parse error at byte ") +
        std::to_string(static_cast<int64_t>(slice.base_offset)) + ": " +
        std::string(message));
  }

  /// Removes JSON whitespace preceding the first value byte.
  [[nodiscard]] static std::string_view
  trim_leading_json_ws(std::string_view value) noexcept {
    while (!value.empty() && is_ws(static_cast<unsigned char>(value.front()))) {
      value.remove_prefix(1);
    }
    return value;
  }

  /// Appends one accepted object field while enforcing the per-row field
  /// ceiling.
  static sanitize::Status emit_object_field(void *raw_ctx, std::string_view key,
                                            uint64_t key_hash,
                                            ValueView value) {
    auto *ctx = static_cast<ObjectEmitContext *>(raw_ctx);
    if (ctx->plan &&
        (!ctx->rows || !ctx->rows->matches_root_field(key, key_hash))) {
      return sanitize::Status::OK();
    }
    if (ctx->emitted_fields >= kMaxMaterializedFieldsPerRow) {
      return sanitize::Status::Invalid(
          "JSON object field count exceeds safety limit: ",
          ctx->emitted_fields + 1U, " > ", kMaxMaterializedFieldsPerRow);
    }
    ctx->batch->push(
        FieldRef{.key = key, .key_hash = key_hash, .value = value});
    ++ctx->emitted_fields;
    return sanitize::Status::OK();
  }

  /// Tests a source key against the cached compiled root-field filter.
  [[nodiscard]] bool matches_root_field(std::string_view key,
                                        uint64_t key_hash) const {
    return root_filter_.accepts(key, key_hash);
  }

  /// Enumerates a top-level object into the current row and contextualizes
  /// parse failures.
  sanitize::Status append_object_fields(JsonTextBatchStorage *storage,
                                        const TextSlice &slice) const {
    ObjectEmitContext ctx{
        .batch = &storage->batch, .plan = plan_, .rows = this};
    auto status = storage->doc.ForEachObjectFieldC(
        slice.view, &ctx, &emit_object_field, slice.base_offset);
    if (!status.ok()) {
      return prefixed_parse_error(slice, status.message());
    }
    return sanitize::Status::OK();
  }

  /// Reports whether the configured fallback key is represented by the active
  /// plan.
  [[nodiscard]] bool accepts_default_key() const noexcept {
    if (!plan_) {
      return true;
    }
    const std::string_view key(default_key_);
    return plan_->root_layout.find(key, default_key_hash_) != nullptr ||
           matches_planned_field(plan_->root_layout, key, default_key_hash_,
                                 field_name_policy_);
  }

  /// Parses a non-object value and appends it under the accepted fallback key.
  sanitize::Status append_default_value(JsonTextBatchStorage *storage,
                                        const TextSlice &slice) const {
    if (!accepts_default_key()) {
      return sanitize::Status::OK();
    }
    auto parsed = storage->doc.ParseValue(slice.view, slice.base_offset);
    if (!parsed.ok()) {
      return prefixed_parse_error(slice, parsed.status().message());
    }
    storage->batch.push(FieldRef{.key = std::string_view(default_key_),
                                 .key_hash = default_key_hash_,
                                 .value = *parsed});
    return sanitize::Status::OK();
  }

  /// Dispatches a JSON slice to object-field or fallback-value materialization.
  sanitize::Status append_materialized_slice(JsonTextBatchStorage *storage,
                                             const TextSlice &slice) const {
    const std::string_view probe = trim_leading_json_ws(slice.view);
    if (!probe.empty() && probe.front() == '{') {
      return append_object_fields(storage, slice);
    }
    return append_default_value(storage, slice);
  }
  std::string default_key_;
  uint64_t default_key_hash_ = 0;
  std::string field_name_policy_;
  const CompiledPlan *plan_ = nullptr;
  mutable JsonRootFieldFilter root_filter_;
  bool direct_rows_ = false;
  bool parallel_rows_ = false;
  bool line_delimited_ = false;
  OnErrorPolicy on_error_ = OnErrorPolicy::kStop;
  bool stop_on_error_ = false;
  bool plan_ordered_ = false;
  bool raw_only_ = false;
  bool validate_raw_ = false;
  bool require_object_rows_ = false;
  std::size_t token_index_max_fields_ = 0;
  FrontendMaterializationMode mode_ = FrontendMaterializationMode::kDefault;
};
// Streams JSON text slices into row batches.
class JsonTextFrontend final {
public:
  /// Creates a streaming JSON scanner configured for array, object, or JSONL
  /// framing.
  JsonTextFrontend(ChunkSourcePtr src, const Options &options,
                   bool require_top_level_array = false,
                   bool require_object_rows = false,
                   bool line_delimited = false)
      : src_(std::move(src)),
        rows_(options, require_object_rows, line_delimited) {
    chunk_bytes_ =
        internal::memory_budget_from_limit(options.memory_limit_bytes)
            .io_chunk_bytes;
    arena_block_bytes_ = static_cast<std::size_t>(chunk_bytes_);
    scanner_ = std::make_unique<JsonStreamingScanner>(
        src_, chunk_bytes_, require_top_level_array, line_delimited);
    reset_status_ = scanner_->Reset();
  }

  /// Forwards the compiled plan to the JSON row decoder.
  void set_plan(const CompiledPlan *plan) noexcept { rows_.set_plan(plan); }

  /// Updates row policy and scanner framing for the materialization mode.
  void set_materialization_mode(FrontendMaterializationMode mode) noexcept {
    rows_.set_materialization_mode(mode);
    scanner_->set_worker_authoritative_framing(
        mode == FrontendMaterializationMode::kWorkerAuthoritativeRaw);
  }

  /// Charges scanner and row-batch allocations to the supplied memory pool.
  void set_memory_pool(std::shared_ptr<void> pool) noexcept {
    memory_pool_ = std::move(pool);
    rows_.set_memory_pool(memory_pool_);
  }

  /// Rewinds JSON scanning and clears completion state.
  void reset() noexcept {
    done_ = false;
    if (scanner_) {
      reset_status_ = scanner_->Reset();
    } else if (src_) {
      reset_status_ = src_->Reset();
    }
  }

  /// Reads and materializes the next bounded row batch from the JSON text
  /// frontend.
  sanitize::Result<RowBatch> next_batch(int64_t capacity) {
    RowBatch out;
    if (capacity <= 0 || done_) {
      return out;
    }
    if (!reset_status_.ok()) {
      return reset_status_;
    }
    if (!scanner_) {
      return sanitize::Status::Invalid("JSON frontend: scanner is null");
    }
    auto storage = std::make_shared<JsonTextBatchStorage>(memory_pool_,
                                                          arena_block_bytes_);
    const bool direct_raw = rows_.emits_deferred_raw_rows();
    storage->prepare_output_rows(capacity, rows_.token_index_max_fields(),
                                 direct_raw, &out.rows);
    int64_t produced = 0;
    while (produced < capacity) {
      SAN_ASSIGN_OR_RAISE(TextSlice slice,
                          scanner_->next_value(&storage->arena));
      if (slice.view.empty()) {
        if (scanner_->done()) {
          done_ = true;
          break;
        }
        continue;
      }
      out.reader_diagnostics.records += 1;
      out.reader_diagnostics.decoded_bytes +=
          static_cast<std::int64_t>(slice.view.size());
      if (direct_raw) {
        storage->append_deferred_raw(slice, rows_.requires_object_rows(),
                                     &out.rows);
      } else {
        const auto status = rows_.append(storage.get(), slice);
        if (!status.ok()) {
          if (status.code() != sanitize::StatusCode::kInvalid ||
              json_error_exceeds_hard_safety_limit(status) ||
              rows_.requires_object_rows() ||
              rows_.on_error() == OnErrorPolicy::kStop) {
            return status;
          }
          if (rows_.on_error() == OnErrorPolicy::kSkipRow) {
            continue;
          }
          rows_.append_null_row(storage.get(), slice);
        }
      }
      ++produced;
    }
    storage->finish_output_rows(direct_raw, &out.rows);
    out.owner = std::move(storage);
    return out;
  }

private:
  ChunkSourcePtr src_;
  JsonTextRows rows_;
  int64_t chunk_bytes_ = int64_t{1} << 20;
  std::size_t arena_block_bytes_ = std::size_t{1} << 20;
  sanitize::Status reset_status_ = sanitize::Status::OK();
  bool done_ = false;
  std::unique_ptr<JsonStreamingScanner> scanner_;
  std::shared_ptr<void> memory_pool_;
};
struct JsonArrayGroupBatchStorage {

  /// Creates storage that keeps every constituent array-document batch alive.
  JsonArrayGroupBatchStorage(std::shared_ptr<void> pool,
                             std::shared_ptr<PoolResource> resource)
      : pool_keepalive(std::move(pool)),
        resource_keepalive(std::move(resource)),
        batches(resource_keepalive.get()) {}

  std::shared_ptr<void> pool_keepalive;
  std::shared_ptr<PoolResource> resource_keepalive;
  std::pmr::vector<RowBatch> batches;
};
FrontendHandle make_json_array_element_frontend(ChunkSourcePtr json,
                                                const Options &options,
                                                bool require_object_rows);
class JsonArrayGroupFrontend final {
public:
  /// Initializes ordered traversal across a group of JSON array documents.
  JsonArrayGroupFrontend(std::vector<std::string> paths,
                         std::vector<std::string> source_names, Options options,
                         bool require_object_rows)
      : paths_(std::move(paths)), source_names_(std::move(source_names)),
        options_(std::move(options)),
        require_object_rows_(require_object_rows) {}

  /// Rewinds grouped traversal and discards the current per-file frontend.
  void reset() noexcept {
    index_ = 0;
    done_ = paths_.empty();
    current_ = FrontendHandle{};
    open_status_ = sanitize::Status::OK();
  }

  /// Applies the compiled plan to the current and all future file frontends.
  void set_plan(const CompiledPlan *plan) noexcept {
    plan_ = plan;
    if (current_) {
      current_.set_plan(plan_);
    }
  }

  /// Shares the supplied memory pool with current and future file frontends.
  void set_memory_pool(std::shared_ptr<void> pool) noexcept {
    memory_pool_ = std::move(pool);
    if (current_) {
      current_.set_memory_pool(memory_pool_);
    }
  }

  /// Concatenates the next bounded rows while preserving input-file order.
  sanitize::Result<RowBatch> next_batch(int64_t capacity) {
    RowBatch out;
    if (capacity <= 0 || done_) {
      return out;
    }
    if (!open_status_.ok()) {
      return open_status_;
    }
    std::shared_ptr<JsonArrayGroupBatchStorage> storage;
    try {
      auto resource = std::make_shared<PoolResource>(memory_pool_);
      storage = std::make_shared<JsonArrayGroupBatchStorage>(
          memory_pool_, std::move(resource));
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "grouped JSON document batch-owner allocation failed");
    }
    int64_t produced = 0;
    while (produced < capacity && !done_) {
      if (!current_) {
        SAN_RETURN_NOT_OK(open_current());
        if (done_) {
          break;
        }
      }
      SAN_ASSIGN_OR_RAISE(RowBatch child,
                          current_.next_batch(capacity - produced));
      if (child.rows.empty()) {
        current_ = FrontendHandle{};
        ++index_;
        if (index_ >= paths_.size()) {
          done_ = true;
        }
        continue;
      }
      produced += static_cast<int64_t>(child.rows.size());
      out.reader_diagnostics.merge(child.reader_diagnostics);
      out.rows.insert(out.rows.end(), child.rows.begin(), child.rows.end());
      storage->batches.push_back(std::move(child));
    }
    out.owner = std::move(storage);
    return out;
  }

private:
  /// Lazily opens the current array-document path and applies shared runtime
  /// state.
  sanitize::Status open_current() {
    if (index_ >= paths_.size()) {
      done_ = true;
      return sanitize::Status::OK();
    }
    std::vector<std::string> paths;
    std::vector<std::string> source_names;
    paths.push_back(paths_[index_]);
    source_names.push_back(source_names_[index_]);
    SAN_ASSIGN_OR_RAISE(auto src,
                        sanitize::chunk_source_from_paths_with_source_names(
                            std::move(paths), std::move(source_names), "\n",
                            options_.memory_limit_bytes));
    current_ = make_json_array_element_frontend(std::move(src), options_,
                                                require_object_rows_);
    current_.set_memory_pool(memory_pool_);
    if (!current_) {
      return sanitize::Status::Invalid("json_array grouped frontend failed");
    }
    current_.set_plan(plan_);
    return sanitize::Status::OK();
  }
  std::vector<std::string> paths_;
  std::vector<std::string> source_names_;
  Options options_;
  bool require_object_rows_ = true;
  const CompiledPlan *plan_ = nullptr;
  std::size_t index_ = 0;
  bool done_ = false;
  FrontendHandle current_;
  sanitize::Status open_status_ = sanitize::Status::OK();
  std::shared_ptr<void> memory_pool_;
};

/// Rewinds the JSON text frontend to its initial input position and clears
/// per-pass state.
static void json_reset(void *self) noexcept {
  static_cast<JsonTextFrontend *>(self)->reset();
}

/// Reads and materializes the next bounded row batch from the JSON text
/// frontend.
static sanitize::Result<RowBatch> json_next_batch(void *self,
                                                  int64_t capacity) {
  return static_cast<JsonTextFrontend *>(self)->next_batch(capacity);
}

/// Forwards a compiled plan through the JSON text frontend callback table.
static void json_set_plan(void *self, const CompiledPlan *plan) noexcept {
  static_cast<JsonTextFrontend *>(self)->set_plan(plan);
}

/// Forwards the materialization mode through the JSON text callback table.
static void
json_set_materialization_mode(void *self,
                              FrontendMaterializationMode mode) noexcept {
  static_cast<JsonTextFrontend *>(self)->set_materialization_mode(mode);
}

/// Forwards memory-pool ownership through the JSON text callback table.
static void json_set_memory_pool(void *self,
                                 std::shared_ptr<void> pool) noexcept {
  static_cast<JsonTextFrontend *>(self)->set_memory_pool(std::move(pool));
}

/// Destroys the heap-owned JSON text frontend state after its final callback
/// completes.
static void json_destroy(void *self) noexcept {
  delete static_cast<JsonTextFrontend *>(self);
}
static const FrontendVTable kJsonVTable{
    .reset = &json_reset,
    .next_batch = &json_next_batch,
    .set_plan = &json_set_plan,
    .destroy = &json_destroy,
    .set_memory_pool = &json_set_memory_pool,
    .set_materialization_mode = &json_set_materialization_mode};

/// Creates a JSON frontend that treats one array document as an ordered element
/// stream.
FrontendHandle make_json_array_element_frontend(ChunkSourcePtr json,
                                                const Options &options,
                                                bool require_object_rows) {
  auto *fe =
      new JsonTextFrontend(std::move(json), options, true, require_object_rows);
  return {fe, &kJsonVTable};
}

/// Rewinds the JSON text frontend to its initial input position and clears
/// per-pass state.
static void json_array_group_reset(void *self) noexcept {
  static_cast<JsonArrayGroupFrontend *>(self)->reset();
}

/// Reads and materializes the next bounded row batch from the JSON text
/// frontend.
static sanitize::Result<RowBatch>
json_array_group_next_batch(void *self, int64_t capacity) {
  return static_cast<JsonArrayGroupFrontend *>(self)->next_batch(capacity);
}

/// Forwards a compiled plan through the grouped-array JSON callback table.
static void json_array_group_set_plan(void *self,
                                      const CompiledPlan *plan) noexcept {
  static_cast<JsonArrayGroupFrontend *>(self)->set_plan(plan);
}

/// Forwards memory-pool ownership through the grouped-array JSON callback
/// table.
static void
json_array_group_set_memory_pool(void *self,
                                 std::shared_ptr<void> pool) noexcept {
  static_cast<JsonArrayGroupFrontend *>(self)->set_memory_pool(std::move(pool));
}

/// Destroys the heap-owned JSON text frontend state after its final callback
/// completes.
static void json_array_group_destroy(void *self) noexcept {
  delete static_cast<JsonArrayGroupFrontend *>(self);
}
static const FrontendVTable kJsonArrayGroupVTable{
    .reset = &json_array_group_reset,
    .next_batch = &json_array_group_next_batch,
    .set_plan = &json_array_group_set_plan,
    .destroy = &json_array_group_destroy,
    .set_memory_pool = &json_array_group_set_memory_pool};
} // namespace
FrontendHandle make_json_frontend(ChunkSourcePtr json, const Options &options) {
  auto *fe = new JsonTextFrontend(std::move(json), options);
  return {fe, &kJsonVTable};
}
FrontendHandle make_jsonl_frontend(ChunkSourcePtr json,
                                   const Options &options) {
  auto *fe = new JsonTextFrontend(std::move(json), options, false, false, true);
  return {fe, &kJsonVTable};
}
FrontendHandle make_json_array_frontend(ChunkSourcePtr json,
                                        const Options &options) {
  auto *fe = new JsonTextFrontend(std::move(json), options, true, true);
  return {fe, &kJsonVTable};
}
FrontendHandle
make_json_array_group_frontend(std::vector<std::string> paths,
                               std::vector<std::string> source_names,
                               const Options &options) {
  if (paths.size() != source_names.size()) {
    return {};
  }
  auto *fe = new JsonArrayGroupFrontend(std::move(paths),
                                        std::move(source_names), options, true);
  fe->reset();
  return {fe, &kJsonArrayGroupVTable};
}
FrontendHandle
make_json_document_array_group_frontend(std::vector<std::string> paths,
                                        std::vector<std::string> source_names,
                                        const Options &options) {
  if (paths.size() != source_names.size()) {
    return {};
  }
  auto *fe = new JsonArrayGroupFrontend(
      std::move(paths), std::move(source_names), options, false);
  fe->reset();
  return {fe, &kJsonArrayGroupVTable};
}
} // namespace sanitize::internal

// Materializes one JSON text slice into a planned row batch.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "frontends/builtin_frontends.hh"
#include "frontends/json/root_field_filter.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/row_scanner.hh" // is_ws
#include "internal/parsing/streaming/json/scanner.hh"
#include "internal/planning/planned_name_matcher.hh"
#include "sanitize/core/status.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

namespace {

struct BatchStorage {
  PoolResource pmr_pool;
  BumpArena arena;
  JsonOnDemandDoc doc;
  FlatRowBatch batch;

  BatchStorage() : arena(pmr_pool.pool()), doc(&pmr_pool) {}

  // Keeps backing chunks alive when values alias ChunkSource buffers.
  void keep(const std::shared_ptr<const void> &owner) {
    if (!owner || owner.get() == last_owner_ptr) {
      return;
    }
    last_owner_ptr = owner.get();
    keepalive.push_back(owner);
  }

  // Retains source-name storage for generated row metadata.
  void keep_source_name(const std::shared_ptr<const std::string> &owner) {
    keep(std::static_pointer_cast<const void>(owner));
  }

  std::vector<std::shared_ptr<const void>> keepalive;
  const void *last_owner_ptr = nullptr;
};

class JsonTextRows final {
public:
  JsonTextRows(const Options &options, bool require_object_rows)
      : default_key_(options.default_key_name),
        default_key_hash_(
            sanitize::detail::hash_key64(std::string_view(default_key_))),
        field_name_policy_(options.field_name_policy),
        direct_rows_(static_cast<bool>(options.arrow_schema_contract)),
        require_object_rows_(require_object_rows) {}

  void set_plan(const CompiledPlan *plan) noexcept {
    plan_ = plan;
    raw_only_ = plan != nullptr && direct_rows_;
    root_filter_.reset(plan_, field_name_policy_);
  }

  // Appends one JSON slice using the raw or materialized frontend path.
  sanitize::Status append(BatchStorage *storage, const TextSlice &slice) const {
    const std::string_view probe = trim_leading_json_ws(slice.view);
    const bool is_object_row = !probe.empty() && probe.front() == '{';
    if (require_object_rows_ && !is_object_row) {
      return sanitize::Status::Invalid("json_array requires object elements");
    }
    storage->keep(slice.owner);
    storage->keep_source_name(slice.source_file_owner);
    const uint8_t row_flags = (raw_only_ && is_object_row)
                                  ? std::to_underlying(RowFlags::kRawOnly)
                                  : 0;
    storage->batch.start_row(slice.view, slice.base_offset, row_flags, nullptr,
                             slice.source_file);
    if (row_flags == 0) {
      SAN_RETURN_NOT_OK(append_materialized_slice(storage, slice));
    }
    storage->batch.end_row();
    return sanitize::Status::OK();
  }

private:
  struct ObjectEmitContext {
    FlatRowBatch *batch = nullptr;
    const CompiledPlan *plan = nullptr;
    const JsonTextRows *rows = nullptr;
  };

  [[nodiscard]] static sanitize::Status
  prefixed_parse_error(const TextSlice &slice, std::string_view message) {
    return sanitize::Status::Invalid(
        std::string("JSON parse error at byte ") +
        std::to_string(static_cast<int64_t>(slice.base_offset)) + ": " +
        std::string(message));
  }

  [[nodiscard]] static std::string_view
  trim_leading_json_ws(std::string_view value) noexcept {
    while (!value.empty() && is_ws(static_cast<unsigned char>(value.front()))) {
      value.remove_prefix(1);
    }
    return value;
  }

  static sanitize::Status emit_object_field(void *raw_ctx, std::string_view key,
                                            uint64_t key_hash,
                                            ValueView value) {
    auto *ctx = static_cast<ObjectEmitContext *>(raw_ctx);
    if (ctx->plan &&
        (!ctx->rows || !ctx->rows->matches_root_field(key, key_hash))) {
      return sanitize::Status::OK();
    }
    ctx->batch->push(
        FieldRef{.key = key, .key_hash = key_hash, .value = value});
    return sanitize::Status::OK();
  }

  [[nodiscard]] bool matches_root_field(std::string_view key,
                                        uint64_t key_hash) const {
    return root_filter_.accepts(key, key_hash);
  }

  sanitize::Status append_object_fields(BatchStorage *storage,
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

  [[nodiscard]] bool accepts_default_key() const noexcept {
    if (!plan_) {
      return true;
    }
    const std::string_view key(default_key_);
    return plan_->root_layout.find(key, default_key_hash_) != nullptr ||
           matches_planned_field(plan_->root_layout, key, default_key_hash_,
                                 field_name_policy_);
  }

  sanitize::Status append_default_value(BatchStorage *storage,
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

  sanitize::Status append_materialized_slice(BatchStorage *storage,
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
  bool raw_only_ = false;
  bool require_object_rows_ = false;
};
// Streams JSON text slices into row batches.

class JsonTextFrontend final {
public:
  JsonTextFrontend(ChunkSourcePtr src, const Options &options,
                   bool require_top_level_array = false,
                   bool require_object_rows = false)
      : src_(std::move(src)), rows_(options, require_object_rows) {
    chunk_bytes_ = options.io_chunk_bytes > 0 ? options.io_chunk_bytes
                                              : (int64_t{1} << 20);
    scanner_ = std::make_unique<JsonStreamingScanner>(src_, chunk_bytes_,
                                                      require_top_level_array);
    reset_status_ = scanner_->Reset();
  }

  // Installs the compiled plan used by row materialization.
  void set_plan(const CompiledPlan *plan) noexcept { rows_.set_plan(plan); }

  // Rewinds JSON scanning and clears completion state.
  void reset() noexcept {
    done_ = false;
    if (scanner_) {
      reset_status_ = scanner_->Reset();
    } else if (src_) {
      reset_status_ = src_->Reset();
    }
  }

  // Returns the next batch.
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

    auto storage = std::make_shared<BatchStorage>();
    storage->batch.reset(capacity);
    storage->arena.reset();

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

      SAN_RETURN_NOT_OK(rows_.append(storage.get(), slice));
      ++produced;
    }

    storage->batch.export_rows(&out.rows);
    out.owner = std::move(storage);
    return out;
  }

private:
  ChunkSourcePtr src_;
  JsonTextRows rows_;
  int64_t chunk_bytes_ = int64_t{1} << 20;
  sanitize::Status reset_status_ = sanitize::Status::OK();
  bool done_ = false;
  std::unique_ptr<JsonStreamingScanner> scanner_;
};
struct JsonArrayGroupBatchStorage {
  std::vector<RowBatch> batches;
};

FrontendHandle make_json_array_element_frontend(ChunkSourcePtr json,
                                                const Options &options,
                                                bool require_object_rows);

class JsonArrayGroupFrontend final {
public:
  JsonArrayGroupFrontend(std::vector<std::string> paths,
                         std::vector<std::string> source_names, Options options,
                         bool require_object_rows)
      : paths_(std::move(paths)), source_names_(std::move(source_names)),
        options_(std::move(options)),
        require_object_rows_(require_object_rows) {}

  void reset() noexcept {
    index_ = 0;
    done_ = paths_.empty();
    current_ = FrontendHandle{};
    open_status_ = sanitize::Status::OK();
  }

  void set_plan(const CompiledPlan *plan) noexcept {
    plan_ = plan;
    if (current_) {
      current_.set_plan(plan_);
    }
  }

  sanitize::Result<RowBatch> next_batch(int64_t capacity) {
    RowBatch out;
    if (capacity <= 0 || done_) {
      return out;
    }
    if (!open_status_.ok()) {
      return open_status_;
    }

    auto storage = std::make_shared<JsonArrayGroupBatchStorage>();
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
      out.rows.insert(out.rows.end(), child.rows.begin(), child.rows.end());
      storage->batches.push_back(std::move(child));
    }

    out.owner = std::move(storage);
    return out;
  }

private:
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
                            std::move(paths), std::move(source_names), "\n"));
    current_ = make_json_array_element_frontend(std::move(src), options_,
                                                require_object_rows_);
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
};
// Adapts JsonTextFrontend::reset to the frontend vtable.
static void json_reset(void *self) noexcept {
  static_cast<JsonTextFrontend *>(self)->reset();
}

// Adapts JsonTextFrontend::next_batch to the frontend vtable.
static sanitize::Result<RowBatch> json_next_batch(void *self,
                                                  int64_t capacity) {
  return static_cast<JsonTextFrontend *>(self)->next_batch(capacity);
}

// Adapts JsonTextFrontend::set_plan to the frontend vtable.
static void json_set_plan(void *self, const CompiledPlan *plan) noexcept {
  static_cast<JsonTextFrontend *>(self)->set_plan(plan);
}

// Releases a JsonTextFrontend stored behind a frontend handle.
static void json_destroy(void *self) noexcept {
  delete static_cast<JsonTextFrontend *>(self);
}

static const FrontendVTable kJsonVTable{
    .reset = &json_reset,
    .next_batch = &json_next_batch,
    .set_plan = &json_set_plan,
    .destroy = &json_destroy,
};

FrontendHandle make_json_array_element_frontend(ChunkSourcePtr json,
                                                const Options &options,
                                                bool require_object_rows) {
  auto *fe =
      new JsonTextFrontend(std::move(json), options, true, require_object_rows);
  return {fe, &kJsonVTable};
}

static void json_array_group_reset(void *self) noexcept {
  static_cast<JsonArrayGroupFrontend *>(self)->reset();
}

static sanitize::Result<RowBatch>
json_array_group_next_batch(void *self, int64_t capacity) {
  return static_cast<JsonArrayGroupFrontend *>(self)->next_batch(capacity);
}

static void json_array_group_set_plan(void *self,
                                      const CompiledPlan *plan) noexcept {
  static_cast<JsonArrayGroupFrontend *>(self)->set_plan(plan);
}

static void json_array_group_destroy(void *self) noexcept {
  delete static_cast<JsonArrayGroupFrontend *>(self);
}

static const FrontendVTable kJsonArrayGroupVTable{
    .reset = &json_array_group_reset,
    .next_batch = &json_array_group_next_batch,
    .set_plan = &json_array_group_set_plan,
    .destroy = &json_array_group_destroy,
};
} // namespace

FrontendHandle make_json_frontend(ChunkSourcePtr json, const Options &options) {
  auto *fe = new JsonTextFrontend(std::move(json), options);
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

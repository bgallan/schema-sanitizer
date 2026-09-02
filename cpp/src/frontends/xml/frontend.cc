// Implements the hardened XML frontend lifecycle, batching, and vtable. It
// frames XML rows, records parser diagnostics, and returns budget-owned batches
// in source order.

#include "frontends/builtin_frontends.hh"
#include "frontends/xml/frontend_internal.hh"

#include "internal/runtime/thread_compat.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <span>
#include <string_view>
#include <utility>

#include "internal/materialization/batch_sizing.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor.hh"
#include "sanitize/detail/hash.hh"

namespace sanitize::internal::xml_frontend_detail {

struct XmlParseRange {
  std::size_t begin = 0;
  std::size_t end = 0;
};

struct XmlParsedChunk {

  /// Creates pool-backed storage for parsed XML nodes from one work packet.
  explicit XmlParsedChunk(std::pmr::memory_resource *resource)
      : nodes(resource) {}

  std::pmr::vector<XmlNodePtr> nodes;
  ReaderResourceDiagnostics diagnostics;
};

/// Accumulates parser diagnostics into bounded XML frontend diagnostics.
void record_parser_diagnostics(ReaderResourceDiagnostics *target,
                               const XmlParser &parser) noexcept {
  if (!target) {
    return;
  }
  target->parser_max_depth = std::max<int64_t>(
      target->parser_max_depth, static_cast<int64_t>(parser.max_depth()));
  target->decoded_bytes += static_cast<int64_t>(parser.decoded_bytes());
  target->nodes += static_cast<int64_t>(parser.node_count());
  target->records += 1;
}

/// Creates the XML row scanner and derives its memory bounds from prepared
/// options.
XmlFrontend::XmlFrontend(ChunkSourcePtr src, const Options &options)
    : src_(std::move(src)), default_key_(options.default_key_name),
      default_key_hash_(sanitize::detail::hash_key64(default_key_)),
      row_tag_(options.xml_row_tag),
      chunk_bytes_(
          internal::memory_budget_from_limit(options.memory_limit_bytes)
              .io_chunk_bytes),
      memory_limit_bytes_(options.memory_limit_bytes) {}

sanitize::Status XmlFrontend::ensure_initialized() {
  if (initialized_) {
    return parse_status_;
  }
  try {
    xml_resource_ = std::make_shared<PoolResource>(memory_pool_);
    owned_text_ = std::make_unique<std::pmr::string>(xml_resource_.get());
    rows_ = std::make_unique<std::pmr::vector<const XmlNode *>>(
        xml_resource_.get());
    if (row_tag_.empty()) {
      parse_status_ = parse_once();
    } else {
      scanner_ = std::make_unique<XmlRowTagScanner>(
          std::move(src_), row_tag_, chunk_bytes_, memory_limit_bytes_,
          xml_resource_.get());
      parse_status_ = scanner_->Reset();
    }
    initialized_ = true;
    return parse_status_;
  } catch (const std::bad_alloc &) {
    initialized_ = true;
    parse_status_ = memory_limit_bytes_ > 0
                        ? sanitize::Status::OutOfMemory(
                              "memory_limit_bytes limit exceeded during XML "
                              "frontend initialization allocation")
                        : sanitize::Status::OutOfMemory(
                              "XML frontend initialization allocation failed");
    return parse_status_;
  }
}

/// Rewinds the XML frontend to its initial input position and clears per-pass
/// state.
void XmlFrontend::reset() noexcept {
  row_index_ = 0;
  done_ = false;
  document_diagnostics_emitted_ = false;
  if (scanner_) {
    parse_status_ = scanner_->Reset();
  }
}

/// Transitions the XML frontend from inference to execution mode.
void XmlFrontend::set_plan(const CompiledPlan *) noexcept {
  execution_mode_ = true;
  if (!scanner_ && root_) {
    // Analytical ownership begins after materialization. The parsed model owns
    // every value needed for execution, so the duplicate raw document can be
    // released from the operation budget after inference.
    source_owner_.reset();
    source_text_ = {};
    owned_text_.reset();
  }
}

/// Retains the memory pool that owns XML output batches.
void XmlFrontend::set_memory_pool(std::shared_ptr<void> pool) noexcept {
  memory_pool_ = std::move(pool);
}

/// Retains the task arena used for parallel XML row materialization.
void XmlFrontend::set_task_arena(
    std::shared_ptr<OperationTaskArena> task_arena) noexcept {
  task_arena_ = std::move(task_arena);
}

sanitize::Result<std::string_view> XmlFrontend::read_source_text() {
  if (!src_) {
    return sanitize::Status::Invalid("XML frontend: source is null");
  }
  if (memory_limit_bytes_ <= 0) {
    SAN_ASSIGN_OR_RAISE(auto chunk, src_->View());
    source_owner_ = std::move(chunk.owner);
    return chunk.data;
  }

  SAN_RETURN_NOT_OK(src_->Reset());
  owned_text_->clear();
  for (;;) {
    SAN_ASSIGN_OR_RAISE(auto chunk, src_->NextChunk(chunk_bytes_));
    if (chunk.data.empty()) {
      break;
    }
    owned_text_->append(chunk.data);
  }
  return std::string_view(*owned_text_);
}

sanitize::Status XmlFrontend::parse_once() {
  SAN_ASSIGN_OR_RAISE(source_text_, read_source_text());

  XmlParser parser(source_text_, xml_resource_.get());
  SAN_ASSIGN_OR_RAISE(root_, parser.parse_document());
  document_diagnostics_ = {};
  record_parser_diagnostics(&document_diagnostics_, parser);
  SAN_RETURN_NOT_OK(build_xml_node_model(root_.get()));
  select_rows();
  return sanitize::Status::OK();
}

void XmlFrontend::select_rows() {
  rows_->clear();
  if (root_) {
    rows_->push_back(root_.get());
  }
}

/// Reads and materializes the next bounded row batch from the XML frontend.
sanitize::Result<RowBatch> XmlFrontend::next_batch(int64_t capacity) {
  RowBatch out;
  if (capacity <= 0 || done_) {
    return out;
  }
  SAN_RETURN_NOT_OK(ensure_initialized());
  if (!parse_status_.ok()) {
    return parse_status_;
  }

  try {
    // XML rows retain both their parsed tree and the downstream Arrow values
    // until the batch owner is released.  The generic initial estimate covers
    // only one materialized representation; cap the frontend batch at half of
    // that byte target so parser/model ownership cannot crowd out output
    // buffers under small public limits.
    if (memory_limit_bytes_ > 0) {
      const auto xml_row_capacity = std::max<int64_t>(
          1, internal::memory_budget_from_limit(memory_limit_bytes_)
                     .batch_target_bytes /
                 (sanitize::internal::kInitialEstimatedRowBytes * 2));
      capacity = std::min(capacity, xml_row_capacity);
    }
    auto storage = std::make_shared<BatchStorage>(
        memory_pool_, xml_resource_,
        static_cast<std::size_t>(std::max<int64_t>(4096, chunk_bytes_)));
    storage->batch.reset(capacity);
    const auto reserve_rows =
        static_cast<std::size_t>(std::min<int64_t>(capacity, int64_t{4096}));
    if (!execution_mode_) {
      storage->raw_rows.reserve(reserve_rows);
    }
    storage->nodes.reserve(reserve_rows);
    // A streamed XML row exists simultaneously as raw scanner bytes, decoded
    // node strings/containers, FlatRowBatch references, and downstream output
    // scratch. Keep raw input to one eighth of the operation budget so those
    // representations cannot consume the entire pool before the ordered
    // consumer releases the preceding batch owner.
    const auto batch_byte_limit =
        memory_limit_bytes_ > 0
            ? std::max<std::size_t>(
                  1, static_cast<std::size_t>(memory_limit_bytes_ / 8))
            : std::numeric_limits<std::size_t>::max();
    std::size_t retained_raw_bytes = 0;

    if (scanner_) {
      const auto arena_workers =
          task_arena_ ? task_arena_->worker_count() : std::size_t{1};
      const bool collect_for_parallel =
          task_arena_ && !task_arena_->inline_mode() && arena_workers > 1;
      if (collect_for_parallel) {
        std::pmr::vector<std::size_t> base_offsets(xml_resource_.get());
        storage->raw_rows.reserve(reserve_rows);
        base_offsets.reserve(reserve_rows);
        while (static_cast<int64_t>(storage->raw_rows.size()) < capacity) {
          SAN_ASSIGN_OR_RAISE(auto slice, scanner_->next_row());
          if (slice.text.empty()) {
            done_ = true;
            break;
          }
          storage->raw_rows.push_back(storage->raw_arena.append(slice.text));
          base_offsets.push_back(slice.base_offset);
          retained_raw_bytes += slice.text.size();
          if (retained_raw_bytes >= batch_byte_limit) {
            break;
          }
        }
        const auto average_row_bytes =
            storage->raw_rows.empty()
                ? std::size_t{0}
                : retained_raw_bytes / storage->raw_rows.size();
        const bool parallel_parse =
            storage->raw_rows.size() >=
                std::max<std::size_t>(4, arena_workers * 2U) &&
            retained_raw_bytes >= std::size_t{256} * 1024U &&
            average_row_bytes >= 512U;
        if (parallel_parse) {
          SAN_RETURN_NOT_OK(append_streamed_rows_parallel(
              storage.get(), storage->raw_rows, base_offsets));
        } else {
          for (std::size_t index = 0; index < storage->raw_rows.size();
               ++index) {
            XmlParser parser(storage->raw_rows[index], xml_resource_.get(),
                             base_offsets[index]);
            SAN_ASSIGN_OR_RAISE(auto node, parser.parse_document());
            record_parser_diagnostics(&storage->reader_diagnostics, parser);
            SAN_RETURN_NOT_OK(build_xml_node_model(node.get()));
            const XmlNode *node_ptr = node.get();
            storage->nodes.push_back(std::move(node));
            append_row(storage.get(), node_ptr,
                       execution_mode_ ? std::string_view{}
                                       : storage->raw_rows[index],
                       base_offsets[index]);
          }
        }
      } else {
        int64_t produced = 0;
        while (produced < capacity) {
          SAN_ASSIGN_OR_RAISE(auto slice, scanner_->next_row());
          if (slice.text.empty()) {
            done_ = true;
            break;
          }

          XmlParser parser(slice.text, xml_resource_.get(), slice.base_offset);
          SAN_ASSIGN_OR_RAISE(auto node, parser.parse_document());
          record_parser_diagnostics(&storage->reader_diagnostics, parser);
          SAN_RETURN_NOT_OK(build_xml_node_model(node.get()));
          const XmlNode *node_ptr = node.get();
          storage->nodes.push_back(std::move(node));
          if (execution_mode_) {
            append_row(storage.get(), node_ptr, {}, slice.base_offset);
          } else {
            retained_raw_bytes += slice.text.size();
            storage->raw_rows.push_back(storage->raw_arena.append(slice.text));
            append_row(storage.get(), node_ptr, storage->raw_rows.back(),
                       slice.base_offset);
          }
          ++produced;
          if (retained_raw_bytes >= batch_byte_limit) {
            break;
          }
        }
      }
    } else {
      if (!document_diagnostics_emitted_) {
        storage->reader_diagnostics.merge(document_diagnostics_);
        document_diagnostics_emitted_ = true;
      }
      int64_t produced = 0;
      while (produced < capacity && row_index_ < rows_->size()) {
        const XmlNode *node = (*rows_)[row_index_++];
        if (execution_mode_ && root_ && node == root_.get()) {
          storage->nodes.push_back(std::move(root_));
          node = storage->nodes.back().get();
        }
        std::string_view raw;
        std::size_t base_offset = node ? node->start_offset : 0;
        if (!execution_mode_ && node &&
            node->end_offset >= node->start_offset &&
            node->end_offset <= source_text_.size()) {
          raw = source_text_.substr(node->start_offset,
                                    node->end_offset - node->start_offset);
        }
        append_row(storage.get(), node, raw, base_offset);
        ++produced;
      }
      done_ = row_index_ >= rows_->size();
    }

    storage->batch.export_rows(&out.rows);
    if (!storage->nodes.empty()) {
      out.releaser = storage;
    }
    out.reader_diagnostics = storage->reader_diagnostics;
    out.owner = std::move(storage);
    return out;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "XML frontend batch allocation failed");
  }
}

sanitize::Status XmlFrontend::append_streamed_rows_parallel(
    BatchStorage *storage, std::span<const std::string_view> row_texts,
    std::span<const std::size_t> base_offsets) {
  using Executor = OrderedExecutor<XmlParseRange, XmlParsedChunk>;
  const auto worker_count = std::min<std::size_t>(
      {task_arena_->worker_count(), row_texts.size(), std::size_t{16}});
  const auto chunk_count =
      std::min<std::size_t>(row_texts.size(), worker_count * 2U);
  std::pmr::vector<XmlParseRange> ranges(xml_resource_.get());
  ranges.reserve(chunk_count);
  for (std::size_t chunk = 0; chunk < chunk_count; ++chunk) {
    const auto begin = row_texts.size() * chunk / chunk_count;
    const auto end = row_texts.size() * (chunk + 1U) / chunk_count;
    ranges.push_back(XmlParseRange{.begin = begin, .end = end});
  }

  auto worker = [row_texts, base_offsets,
                 resource = xml_resource_](XmlParseRange &&range, std::size_t,
                                           sanitize::internal::StopToken stop)
      -> sanitize::Result<XmlParsedChunk> {
    try {
      XmlParsedChunk parsed(resource.get());
      parsed.nodes.reserve(range.end - range.begin);
      for (std::size_t index = range.begin; index < range.end; ++index) {
        if (stop.stop_requested()) {
          return sanitize::Status::Cancelled(
              "XML frontend parse cancelled before row decoding");
        }
        XmlParser parser(row_texts[index], resource.get(), base_offsets[index]);
        SAN_ASSIGN_OR_RAISE(auto node, parser.parse_document());
        record_parser_diagnostics(&parsed.diagnostics, parser);
        SAN_RETURN_NOT_OK(build_xml_node_model(node.get()));
        parsed.nodes.push_back(std::move(node));
      }
      return parsed;
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "XML frontend parallel parse allocation failed");
    }
  };
  SAN_ASSIGN_OR_RAISE(auto executor,
                      Executor::Make(worker_count, worker_count * 2U,
                                     worker_count * 2U, std::move(worker),
                                     task_arena_, TaskArenaLane::kUpstream,
                                     TaskTelemetryKind::kInput));

  std::size_t submitted = 0;
  std::size_t committed = 0;
  auto take_and_append = [&]() -> sanitize::Status {
    SAN_ASSIGN_OR_RAISE(auto outcome, executor->TakeNext());
    if (!outcome.result.ok()) {
      executor->Cancel();
      return outcome.result.status();
    }
    const auto &range = ranges[static_cast<std::size_t>(outcome.ordinal)];
    auto parsed = std::move(outcome.result).ValueOrDie();
    storage->reader_diagnostics.merge(parsed.diagnostics);
    for (std::size_t offset = 0; offset < parsed.nodes.size(); ++offset) {
      const auto index = range.begin + offset;
      const XmlNode *node_ptr = parsed.nodes[offset].get();
      storage->nodes.push_back(std::move(parsed.nodes[offset]));
      append_row(storage, node_ptr,
                 execution_mode_ ? std::string_view{} : row_texts[index],
                 base_offsets[index]);
    }
    ++committed;
    return sanitize::Status::OK();
  };

  while (submitted < ranges.size()) {
    if (executor->in_flight() >= executor->dispatch_window()) {
      SAN_RETURN_NOT_OK(take_and_append());
    }
    SAN_RETURN_NOT_OK(executor->Submit(typename Executor::Packet{
        .ordinal = submitted, .payload = ranges[submitted]}));
    ++submitted;
  }
  SAN_RETURN_NOT_OK(executor->FinishSubmission());
  while (committed < submitted) {
    SAN_RETURN_NOT_OK(take_and_append());
  }
  return sanitize::Status::OK();
}

void XmlFrontend::append_object_fields(BatchStorage *storage,
                                       const XmlNode *node) const {
  for (const XmlField &field : node->fields) {
    storage->batch.push(FieldRef{
        .key = field.key,
        .key_hash = field.key_hash,
        .value = xml_field_to_value(field),
    });
  }
}

void XmlFrontend::append_row(BatchStorage *storage, const XmlNode *node,
                             std::string_view raw,
                             std::size_t base_offset) const {
  storage->batch.start_row(raw, base_offset, 0, nullptr);
  const ValueView value = xml_node_to_value(node);
  if (value.is_object() && node) {
    append_object_fields(storage, node);
  } else {
    storage->batch.push(FieldRef{
        .key = default_key_,
        .key_hash = default_key_hash_,
        .value = value,
    });
  }
  storage->batch.end_row();
}

} // namespace sanitize::internal::xml_frontend_detail

namespace sanitize::internal {
namespace {

using xml_frontend_detail::XmlFrontend;

/// Rewinds the XML frontend to its initial input position and clears per-pass
/// state.
void xml_reset(void *self) noexcept {
  static_cast<XmlFrontend *>(self)->reset();
}

/// Reads and materializes the next bounded row batch from the XML frontend.
sanitize::Result<RowBatch> xml_next_batch(void *self, int64_t capacity) {
  return static_cast<XmlFrontend *>(self)->next_batch(capacity);
}

/// Forwards a compiled plan through the XML frontend callback table.
void xml_set_plan(void *self, const CompiledPlan *plan) noexcept {
  static_cast<XmlFrontend *>(self)->set_plan(plan);
}

/// Forwards memory-pool ownership through the XML frontend callback table.
void xml_set_memory_pool(void *self, std::shared_ptr<void> pool) noexcept {
  static_cast<XmlFrontend *>(self)->set_memory_pool(std::move(pool));
}

/// Forwards task-arena ownership through the XML frontend callback table.
void xml_set_task_arena(
    void *self, std::shared_ptr<OperationTaskArena> task_arena) noexcept {
  static_cast<XmlFrontend *>(self)->set_task_arena(std::move(task_arena));
}

/// Destroys the heap-owned XML frontend state after its final callback
/// completes.
void xml_destroy(void *self) noexcept {
  delete static_cast<XmlFrontend *>(self);
}

const FrontendVTable kXmlVTable{
    .reset = &xml_reset,
    .next_batch = &xml_next_batch,
    .set_plan = &xml_set_plan,
    .destroy = &xml_destroy,
    .set_memory_pool = &xml_set_memory_pool,
    .set_task_arena = &xml_set_task_arena,
};

} // namespace

FrontendHandle make_xml_frontend(ChunkSourcePtr xml, const Options &options) {
  auto *frontend = new XmlFrontend(std::move(xml), options);
  return {frontend, &kXmlVTable};
}

} // namespace sanitize::internal

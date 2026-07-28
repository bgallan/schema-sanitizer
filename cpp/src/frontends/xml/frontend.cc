// Implements the XML frontend lifecycle, batching, and frontend vtable.

#include "frontends/builtin_frontends.hh"
#include "frontends/xml/frontend_internal.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <stop_token>
#include <string_view>
#include <utility>

#include "internal/memory/memory_budget.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/ordered_executor.hh"
#include "sanitize/detail/hash.hh"

namespace sanitize::internal::xml_frontend_detail {

struct XmlParseRange {
  std::size_t begin = 0;
  std::size_t end = 0;
};

using XmlParsedChunk = std::vector<std::unique_ptr<XmlNode>>;

XmlFrontend::XmlFrontend(ChunkSourcePtr src, const Options &options)
    : src_(std::move(src)), default_key_(options.default_key_name),
      default_key_hash_(sanitize::detail::hash_key64(default_key_)),
      row_tag_(options.xml_row_tag),
      chunk_bytes_(
          internal::memory_budget_from_limit(options.memory_limit_bytes)
              .io_chunk_bytes),
      memory_limit_bytes_(options.memory_limit_bytes) {
  if (row_tag_.empty()) {
    parse_status_ = parse_once();
  } else {
    scanner_ = std::make_unique<XmlRowTagScanner>(
        std::move(src_), row_tag_, chunk_bytes_, memory_limit_bytes_);
    parse_status_ = scanner_->Reset();
  }
}

void XmlFrontend::reset() noexcept {
  row_index_ = 0;
  done_ = false;
  if (scanner_) {
    parse_status_ = scanner_->Reset();
  }
}

void XmlFrontend::set_plan(const CompiledPlan *) noexcept {
  execution_mode_ = true;
  if (!scanner_) {
    // XmlNode owns parsed names, text, attributes and scalar projections. Once
    // inference has finished, execution no longer needs the original document
    // bytes solely to populate RowRef::raw, so release that duplicate copy.
    source_owner_.reset();
    source_text_ = {};
    std::string().swap(owned_text_);
  }
}

void XmlFrontend::set_memory_pool(std::shared_ptr<void> pool) noexcept {
  memory_pool_ = std::move(pool);
}

void XmlFrontend::set_task_arena(
    std::shared_ptr<OperationTaskArena> task_arena) noexcept {
  task_arena_ = std::move(task_arena);
}

sanitize::Result<std::string_view> XmlFrontend::read_source_text() {
  if (memory_limit_bytes_ <= 0) {
    SAN_ASSIGN_OR_RAISE(auto chunk, src_->View());
    source_owner_ = std::move(chunk.owner);
    return chunk.data;
  }

  SAN_RETURN_NOT_OK(src_->Reset());
  owned_text_.clear();
  for (;;) {
    SAN_ASSIGN_OR_RAISE(auto chunk, src_->NextChunk(chunk_bytes_));
    if (chunk.data.empty()) {
      break;
    }
    const auto limit = static_cast<std::uint64_t>(memory_limit_bytes_);
    const auto current = static_cast<std::uint64_t>(owned_text_.size());
    const auto incoming = static_cast<std::uint64_t>(chunk.data.size());
    if (incoming > limit || current > limit - incoming) {
      return sanitize::Status::OutOfMemory(
          "memory_limit_bytes limit exceeded during xml parsing: ",
          current + incoming, " bytes > ", memory_limit_bytes_, " bytes");
    }
    owned_text_.append(chunk.data);
  }
  return std::string_view(owned_text_);
}

sanitize::Status XmlFrontend::parse_once() {
  if (!src_) {
    return sanitize::Status::Invalid("XML frontend: source is null");
  }
  SAN_ASSIGN_OR_RAISE(source_text_, read_source_text());

  XmlParser parser(source_text_);
  SAN_ASSIGN_OR_RAISE(root_, parser.parse_document());
  build_xml_node_model(root_.get());
  select_rows();
  return sanitize::Status::OK();
}

void XmlFrontend::select_rows() {
  rows_.clear();
  if (root_) {
    rows_.push_back(root_.get());
  }
}

sanitize::Result<RowBatch> XmlFrontend::next_batch(int64_t capacity) {
  RowBatch out;
  if (capacity <= 0 || done_) {
    return out;
  }
  if (!parse_status_.ok()) {
    return parse_status_;
  }

  auto storage = std::make_shared<BatchStorage>(
      memory_pool_,
      static_cast<std::size_t>(std::max<int64_t>(4096, chunk_bytes_)));
  storage->batch.reset(capacity);
  const auto reserve_rows =
      static_cast<std::size_t>(std::min<int64_t>(capacity, int64_t{4096}));
  if (!execution_mode_) {
    storage->raw_rows.reserve(reserve_rows);
  }
  storage->nodes.reserve(reserve_rows);
  const auto batch_byte_limit =
      memory_limit_bytes_ > 0
          ? std::max<std::size_t>(
                1, static_cast<std::size_t>(memory_limit_bytes_ / 3))
          : std::numeric_limits<std::size_t>::max();
  std::size_t retained_raw_bytes = 0;

  if (scanner_) {
    const auto arena_workers =
        task_arena_ ? task_arena_->worker_count() : std::size_t{1};
    const bool collect_for_parallel =
        task_arena_ && !task_arena_->inline_mode() && arena_workers > 1;
    if (collect_for_parallel) {
      std::vector<std::size_t> base_offsets;
      storage->raw_rows.reserve(reserve_rows);
      base_offsets.reserve(reserve_rows);
      while (static_cast<int64_t>(storage->raw_rows.size()) < capacity) {
        SAN_ASSIGN_OR_RAISE(auto slice, scanner_->next_row());
        if (slice.text.empty()) {
          done_ = true;
          break;
        }
        try {
          storage->raw_rows.push_back(storage->raw_arena.append(slice.text));
          base_offsets.push_back(slice.base_offset);
        } catch (const std::bad_alloc &) {
          return sanitize::Status::OutOfMemory(
              "XML frontend row staging allocation failed");
        }
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
        for (std::size_t index = 0; index < storage->raw_rows.size(); ++index) {
          XmlParser parser(storage->raw_rows[index]);
          SAN_ASSIGN_OR_RAISE(auto node, parser.parse_document());
          build_xml_node_model(node.get());
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

        XmlParser parser(slice.text);
        SAN_ASSIGN_OR_RAISE(auto node, parser.parse_document());
        build_xml_node_model(node.get());
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
    int64_t produced = 0;
    while (produced < capacity && row_index_ < rows_.size()) {
      const XmlNode *node = rows_[row_index_++];
      std::string_view raw;
      std::size_t base_offset = node ? node->start_offset : 0;
      if (!execution_mode_ && node && node->end_offset >= node->start_offset &&
          node->end_offset <= source_text_.size()) {
        raw = source_text_.substr(node->start_offset,
                                  node->end_offset - node->start_offset);
      }
      append_row(storage.get(), node, raw, base_offset);
      ++produced;
    }
    done_ = row_index_ >= rows_.size();
  }

  storage->batch.export_rows(&out.rows);
  out.owner = std::move(storage);
  return out;
}

sanitize::Status XmlFrontend::append_streamed_rows_parallel(
    BatchStorage *storage, const std::vector<std::string_view> &row_texts,
    const std::vector<std::size_t> &base_offsets) {
  using Executor = OrderedExecutor<XmlParseRange, XmlParsedChunk>;
  const auto worker_count = std::min<std::size_t>(
      {task_arena_->worker_count(), row_texts.size(), std::size_t{16}});
  const auto chunk_count =
      std::min<std::size_t>(row_texts.size(), worker_count * 2U);
  std::vector<XmlParseRange> ranges;
  ranges.reserve(chunk_count);
  for (std::size_t chunk = 0; chunk < chunk_count; ++chunk) {
    const auto begin = row_texts.size() * chunk / chunk_count;
    const auto end = row_texts.size() * (chunk + 1U) / chunk_count;
    ranges.push_back(XmlParseRange{.begin = begin, .end = end});
  }

  auto worker =
      [&row_texts](XmlParseRange &&range, std::size_t,
                   std::stop_token stop) -> sanitize::Result<XmlParsedChunk> {
    XmlParsedChunk nodes;
    nodes.reserve(range.end - range.begin);
    for (std::size_t index = range.begin; index < range.end; ++index) {
      if (stop.stop_requested()) {
        return sanitize::Status::Cancelled(
            "XML frontend parse cancelled before row decoding");
      }
      XmlParser parser(row_texts[index]);
      SAN_ASSIGN_OR_RAISE(auto node, parser.parse_document());
      build_xml_node_model(node.get());
      nodes.push_back(std::move(node));
    }
    return nodes;
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
    auto nodes = std::move(outcome.result).ValueOrDie();
    for (std::size_t offset = 0; offset < nodes.size(); ++offset) {
      const auto index = range.begin + offset;
      const XmlNode *node_ptr = nodes[offset].get();
      storage->nodes.push_back(std::move(nodes[offset]));
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

void xml_reset(void *self) noexcept {
  static_cast<XmlFrontend *>(self)->reset();
}

sanitize::Result<RowBatch> xml_next_batch(void *self, int64_t capacity) {
  return static_cast<XmlFrontend *>(self)->next_batch(capacity);
}

void xml_set_plan(void *self, const CompiledPlan *plan) noexcept {
  static_cast<XmlFrontend *>(self)->set_plan(plan);
}

void xml_set_memory_pool(void *self, std::shared_ptr<void> pool) noexcept {
  static_cast<XmlFrontend *>(self)->set_memory_pool(std::move(pool));
}

void xml_set_task_arena(
    void *self, std::shared_ptr<OperationTaskArena> task_arena) noexcept {
  static_cast<XmlFrontend *>(self)->set_task_arena(std::move(task_arena));
}

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

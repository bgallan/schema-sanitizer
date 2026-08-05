#include "frontends/builtin_frontends.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/planning/plan_compile.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/streaming/json/scanner.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/options/options.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

namespace {

const sanitize::CompiledPlan &fuzz_compiled_plan() {
  static const sanitize::CompiledPlan plan = [] {
    sanitize::LogicalSchema schema;
    {
      sanitize::LogicalField field;
      field.name = "a";
      field.type = std::make_unique<sanitize::LogicalType>(sanitize::LogicalType::Utf8());
      schema.fields.push_back(std::move(field));
    }
    {
      sanitize::LogicalField field;
      field.name = "id";
      field.type = std::make_unique<sanitize::LogicalType>(sanitize::LogicalType::Int64());
      schema.fields.push_back(std::move(field));
    }
    auto compiled = sanitize::compile_plan(schema);
    if (!compiled.ok()) {
      std::abort();
    }
    return std::move(compiled).ValueOrDie();
  }();
  return plan;
}

sanitize::Status walk_value(sanitize::ValueView value, std::size_t depth) {
  if (depth > sanitize::internal::json_scan::kMaxJsonNestingDepth) {
    return sanitize::Status::Invalid("JSON fuzz walk exceeded depth limit");
  }
  if (value.is_object()) {
    return value.for_each_object_field(
        [depth](std::string_view, std::uint64_t,
                sanitize::ValueView child) -> sanitize::Status {
          return walk_value(child, depth + 1U);
        });
  }
  if (value.is_array()) {
    return value.for_each_array_element(
        [depth](sanitize::ValueView child) -> sanitize::Status {
          return walk_value(child, depth + 1U);
        });
  }
  if (value.is_string()) {
    (void)value.as_string_view();
  } else if (value.is_bool()) {
    (void)value.as_bool();
  } else if (value.is_int()) {
    (void)value.as_int();
  } else if (value.is_float()) {
    (void)value.as_float();
  }
  return sanitize::Status::OK();
}


std::int64_t fuzz_memory_limit(const std::uint8_t *data, std::size_t size) {
  constexpr std::int64_t kMinimum = 64LL * 1024LL;
  constexpr std::int64_t kSpan = 8LL * 1024LL * 1024LL - kMinimum;
  std::uint64_t selector = size;
  for (std::size_t index = 0; index < std::min<std::size_t>(size, 4U);
       ++index) {
    selector = selector * 257U + data[index];
  }
  return kMinimum + static_cast<std::int64_t>(selector % kSpan);
}

void consume_json_frontend(const std::uint8_t *data, std::size_t size) {
  sanitize::Options options;
  options.memory_limit_bytes = fuzz_memory_limit(data, size);
  options.threading_mode =
      size > 0U && (data[0] & 1U) != 0U
          ? sanitize::ThreadingMode::kMulti
          : sanitize::ThreadingMode::kSingle;
  switch (size == 0U ? 0U : (data[0] >> 1U) % 3U) {
  case 1U:
    options.on_error = sanitize::OnErrorPolicy::kSkipRow;
    break;
  case 2U:
    options.on_error = sanitize::OnErrorPolicy::kEmitNullRow;
    break;
  default:
    options.on_error = sanitize::OnErrorPolicy::kStop;
    break;
  }

  auto source = sanitize::chunk_source_from_bytes(
      std::string(reinterpret_cast<const char *>(data), size));
  sanitize::FrontendHandle frontend;
  switch (size == 0U ? 0U : (data[0] >> 3U) % 3U) {
  case 1U:
    frontend = sanitize::internal::make_jsonl_frontend(std::move(source),
                                                        options);
    break;
  case 2U:
    frontend = sanitize::internal::make_json_array_frontend(std::move(source),
                                                             options);
    break;
  default:
    frontend = sanitize::internal::make_json_frontend(std::move(source),
                                                       options);
    break;
  }

  auto pool = sanitize::internal::make_tracking_memory_pool(
      sanitize::internal::shared_default_memory_pool(),
      options.memory_limit_bytes, "json-fuzz");
  frontend.set_memory_pool(pool);
  frontend.set_plan(&fuzz_compiled_plan());
  if (size > 1U && (data[1] & 1U) != 0U) {
    frontend.set_materialization_mode(
        sanitize::FrontendMaterializationMode::kValidatedRaw);
  }

  const auto batch_capacity = static_cast<std::int64_t>(
      1U + (size == 0U ? 0U : data[size - 1U] % 64U));
  const auto max_batches =
      std::clamp<std::size_t>(size / 8U + 1U, 1U, 32U);
  for (std::size_t batch = 0; batch < max_batches; ++batch) {
    auto result = frontend.next_batch(batch_capacity);
    if (!result.ok() || result.ValueOrDie().rows.empty()) {
      break;
    }
  }
}

void parse_and_walk(sanitize::internal::JsonOnDemandDoc *document,
                    std::string_view input, std::size_t base_offset = 0) {
  document->Reset();
  auto parsed = document->ParseValue(input, base_offset);
  if (parsed.ok()) {
    (void)walk_value(parsed.ValueOrDie(), 0U);
  }
  document->Reset();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  const std::string_view input(reinterpret_cast<const char *>(data), size);
  sanitize::internal::JsonOnDemandDoc document(std::pmr::new_delete_resource());
  parse_and_walk(&document, input);
  (void)sanitize::internal::json_skip_value(input, 0);

  auto source = sanitize::chunk_source_from_bytes(
      std::string(reinterpret_cast<const char *>(data), size));
  const auto chunk_bytes = static_cast<std::int64_t>(
      std::clamp<std::size_t>(size / 5U + 1U, 1U, 4096U));
  const bool require_array = size > 0U && (data[0] & 1U) != 0U;
  const bool line_delimited = size > 0U && (data[0] & 2U) != 0U;
  sanitize::internal::JsonStreamingScanner scanner(
      std::move(source), chunk_bytes, require_array, line_delimited);
  sanitize::internal::BumpArena arena(nullptr, 4096U);
  if (!scanner.Reset().ok()) {
    return 0;
  }

  const std::size_t max_values =
      size > std::numeric_limits<std::size_t>::max() - 2U ? size : size + 2U;
  for (std::size_t values = 0; values < max_values && !scanner.done();
       ++values) {
    auto slice_result = scanner.next_value(&arena);
    if (!slice_result.ok()) {
      break;
    }
    const auto slice = slice_result.ValueOrDie();
    if (slice.view.empty()) {
      break;
    }
    parse_and_walk(&document, slice.view, slice.base_offset);
    arena.reset();
  }
  consume_json_frontend(data, size);
  return 0;
}

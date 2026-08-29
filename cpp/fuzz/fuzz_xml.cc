// Fuzzes DOM, streaming row, and frontend XML parsing under bounded resources.
// Input bytes choose row-tag, error, threading, chunking, and memory policies
// while a fixed plan makes materialization behavior reproducible.

#include "frontends/builtin_frontends.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/planning/plan_compile.hh"
#include "internal/parsing/streaming/xml/row_scanner.hh"
#include "internal/parsing/xml/document.hh"
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

/// Returns the immutable two-column plan shared by XML fuzz iterations.
const sanitize::CompiledPlan &fuzz_compiled_plan() {
  static const sanitize::CompiledPlan plan = [] {
    sanitize::LogicalSchema schema;
    {
      sanitize::LogicalField field;
      field.name = "row";
      field.type = std::make_unique<sanitize::LogicalType>(sanitize::LogicalType::Utf8());
      schema.fields.push_back(std::move(field));
    }
    {
      sanitize::LogicalField field;
      field.name = "value";
      field.type = std::make_unique<sanitize::LogicalType>(sanitize::LogicalType::Utf8());
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

/// Derives a bounded operation memory limit from the fuzz input prefix.
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

/// Runs bounded XML frontend batches under input-selected policies.
void consume_xml_frontend(const std::uint8_t *data, std::size_t size) {
  sanitize::Options options;
  options.memory_limit_bytes = fuzz_memory_limit(data, size);
  options.threading_mode =
      size > 0U && (data[0] & 1U) != 0U
          ? sanitize::ThreadingMode::kMulti
          : sanitize::ThreadingMode::kSingle;
  options.xml_row_tag = size > 0U && (data[0] & 2U) != 0U ? "row" : "";
  switch (size == 0U ? 0U : (data[0] >> 2U) % 3U) {
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

  auto frontend = sanitize::internal::make_xml_frontend(
      sanitize::chunk_source_from_bytes(
          std::string(reinterpret_cast<const char *>(data), size)),
      options);
  auto pool = sanitize::internal::make_tracking_memory_pool(
      sanitize::internal::shared_default_memory_pool(),
      options.memory_limit_bytes, "xml-fuzz");
  frontend.set_memory_pool(pool);
  frontend.set_plan(&fuzz_compiled_plan());

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

} // namespace

/// Exercises document, row-scanner, and frontend XML paths for one input.
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  const std::string_view input(reinterpret_cast<const char *>(data), size);
  auto *resource = std::pmr::get_default_resource();

  sanitize::internal::XmlParser parser(input, resource);
  auto document = parser.parse_document();
  if (document.ok()) {
    (void)sanitize::internal::build_xml_node_model(
        document.ValueOrDie().get());
  }

  auto source = sanitize::chunk_source_from_bytes(
      std::string(reinterpret_cast<const char *>(data), size));
  const auto chunk_bytes = static_cast<std::int64_t>(
      std::clamp<std::size_t>(size / 5U + 1U, 1U, 4096U));
  const auto budget_input = std::min<std::size_t>(
      size, static_cast<std::size_t>(
                std::numeric_limits<std::int64_t>::max() / 4));
  const auto memory_limit = std::max<std::int64_t>(
      4096, static_cast<std::int64_t>(budget_input * 4U));
  sanitize::internal::XmlRowTagScanner scanner(
      std::move(source), "row", chunk_bytes, memory_limit, resource);
  if (!scanner.Reset().ok()) {
    return 0;
  }

  const std::size_t max_rows =
      size > std::numeric_limits<std::size_t>::max() - 2U ? size : size + 2U;
  for (std::size_t rows = 0; rows < max_rows; ++rows) {
    auto row_result = scanner.next_row();
    if (!row_result.ok()) {
      break;
    }
    const auto row = row_result.ValueOrDie();
    if (row.text.empty()) {
      break;
    }
    sanitize::internal::XmlParser row_parser(row.text, resource,
                                              row.base_offset);
    auto row_document = row_parser.parse_document();
    if (row_document.ok()) {
      (void)sanitize::internal::build_xml_node_model(
          row_document.ValueOrDie().get());
    }
  }
  consume_xml_frontend(data, size);
  return 0;
}

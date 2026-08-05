#include "frontends/builtin_frontends.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/planning/plan_compile.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/streaming/csv/scanner.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/options/options.hh"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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
      field.name = "b";
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

void verify_csv_cell_limit_boundary() {
  static const bool verified = [] {
    sanitize::internal::BumpArena arena(nullptr, 4096U);
    std::vector<std::string_view> cells;
    std::string accepted(sanitize::internal::kMaxCsvCellsPerRecord - 1U, ',');
    auto accepted_status = sanitize::internal::parse_csv_cells(
        accepted, ',', &cells, &arena, 0);
    if (!accepted_status.ok() ||
        cells.size() != sanitize::internal::kMaxCsvCellsPerRecord) {
      std::abort();
    }

    arena.reset();
    cells.clear();
    accepted.push_back(',');
    auto rejected_status = sanitize::internal::parse_csv_cells(
        accepted, ',', &cells, &arena, 0);
    if (rejected_status.ok()) {
      std::abort();
    }
    return true;
  }();
  (void)verified;
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

void consume_csv_frontend(const std::uint8_t *data, std::size_t size,
                          char delimiter) {
  sanitize::Options options;
  options.csv_has_header = size == 0U || (data[0] & 1U) == 0U;
  options.csv_delimiter.assign(1U, delimiter);
  options.memory_limit_bytes = fuzz_memory_limit(data, size);
  options.threading_mode =
      size > 0U && (data[0] & 2U) != 0U
          ? sanitize::ThreadingMode::kMulti
          : sanitize::ThreadingMode::kSingle;
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

  auto frontend = sanitize::internal::make_csv_frontend(
      sanitize::chunk_source_from_bytes(
          std::string(reinterpret_cast<const char *>(data), size)),
      options);
  auto pool = sanitize::internal::make_tracking_memory_pool(
      sanitize::internal::shared_default_memory_pool(),
      options.memory_limit_bytes, "csv-fuzz");
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

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  verify_csv_cell_limit_boundary();

  auto source = sanitize::chunk_source_from_bytes(
      std::string(reinterpret_cast<const char *>(data), size));
  const auto chunk_bytes = static_cast<std::int64_t>(
      std::clamp<std::size_t>(size / 3U + 1U, 1U, 4096U));
  sanitize::internal::CsvStreamingScanner scanner(std::move(source),
                                                  chunk_bytes);
  sanitize::internal::BumpArena arena(nullptr, 4096U);
  std::vector<std::string_view> cells;
  constexpr std::array<char, 4> delimiters = {',', ';', '\t', '|'};
  const auto delimiter_index =
      size == 0U ? std::size_t{0} : data[0] % delimiters.size();
  const char delimiter = delimiters[delimiter_index];

  if (!scanner.Reset().ok()) {
    return 0;
  }
  const std::size_t max_records =
      size > std::numeric_limits<std::size_t>::max() - 2U ? size : size + 2U;
  for (std::size_t records = 0; records < max_records && !scanner.done();
       ++records) {
    auto record_result = scanner.next_record(&arena);
    if (!record_result.ok()) {
      break;
    }
    const auto record = record_result.ValueOrDie();
    (void)sanitize::internal::parse_csv_cells(
        record.view, delimiter, &cells, &arena, record.base_offset);
    cells.clear();
    arena.reset();
  }

  consume_csv_frontend(data, size, delimiter);
  return 0;
}

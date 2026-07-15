#include "internal/memory/arena.hh"
#include "internal/parsing/streaming/csv/scanner.hh"
#include "sanitize/ingest/chunk_source.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  auto source = sanitize::chunk_source_from_bytes(
      std::string(reinterpret_cast<const char *>(data), size));
  const auto chunk_bytes = static_cast<std::int64_t>(
      std::clamp<std::size_t>(size / 3U + 1U, 1U, 4096U));
  sanitize::internal::CsvStreamingScanner scanner(std::move(source),
                                                  chunk_bytes);
  sanitize::internal::BumpArena arena(nullptr, 4096U);
  if (!scanner.Reset().ok()) {
    return 0;
  }
  const std::size_t max_records = size + 2U;
  for (std::size_t records = 0; records < max_records && !scanner.done();
       ++records) {
    auto record = scanner.next_record(&arena);
    if (!record.ok()) {
      break;
    }
    arena.reset();
  }
  return 0;
}

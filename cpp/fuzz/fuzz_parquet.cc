#include "internal/parquet/footer_reader/api.hh"
#include "sanitize/abi/cdata_types.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif

namespace {

std::filesystem::path fuzz_path() {
#if defined(_WIN32)
  const auto process_id = static_cast<unsigned long>(_getpid());
#else
  const auto process_id = static_cast<unsigned long>(getpid());
#endif
  return std::filesystem::temp_directory_path() /
         ("schema-sanitizer-parquet-fuzz-" + std::to_string(process_id) +
          ".parquet");
}

void consume_native_stream(const std::string &path, std::size_t input_size) {
  auto stream_result =
      sanitize::internal::parquet_footer_reader::make_arrow_stream(path);
  if (!stream_result.ok()) {
    return;
  }
  sanitize::UniqueCStream stream(std::move(stream_result).ValueOrDie());
  sanitize::CSchemaGuard schema;
  if (stream->get_schema(stream.get(), schema.get()) != 0) {
    return;
  }

  const auto max_batches =
      std::clamp<std::size_t>(input_size / 8U + 1U, 1U, 64U);
  for (std::size_t batch = 0; batch < max_batches; ++batch) {
    sanitize::CArrayGuard array;
    if (stream->get_next(stream.get(), array.get()) != 0) {
      break;
    }
    if (array.value().release == nullptr) {
      break;
    }
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  const auto path = fuzz_path();
  {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output) {
      return 0;
    }
    output.write(reinterpret_cast<const char *>(data),
                 static_cast<std::streamsize>(size));
  }
  (void)sanitize::internal::parquet_footer_reader::read_footer_info(
      path.string());
  consume_native_stream(path.string(), size);
  std::error_code ignored;
  std::filesystem::remove(path, ignored);
  return 0;
}

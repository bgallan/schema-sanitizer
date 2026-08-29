// Declares the public contract for bounded native Parquet footer and stream
// reading. These definitions support the internal pipeline without expanding
// its public interface.

#pragma once

#include "internal/parquet/footer_reader/model/footer.hh"
#include "sanitize/core/status.hh"

#include <cstdint>
#include <string>
#include <vector>

struct ArrowArrayStream;

namespace sanitize::internal::parquet_footer_reader {

sanitize::Result<FooterInfo> read_footer_info(const std::string &path);

sanitize::Result<std::string>
read_footer_info_json(const std::string &path,
                      const std::vector<std::string> &projected_columns = {});

sanitize::Result<std::string> read_stream_preflight_json(
    const std::string &path,
    const std::vector<std::string> &projected_columns = {},
    std::int64_t memory_limit_bytes = -1);

sanitize::Result<ArrowArrayStream *>
make_arrow_stream(const std::string &path,
                  const std::vector<std::string> &projected_columns = {},
                  std::int64_t memory_limit_bytes = -1);

} // namespace sanitize::internal::parquet_footer_reader

// Public contract for bounded native Parquet footer and stream reading.

#pragma once

#include "internal/parquet/footer_reader/model/footer.hh"
#include "sanitize/core/status.hh"

#include <string>
#include <vector>

struct ArrowArrayStream;

namespace sanitize::internal::parquet_footer_reader {

sanitize::Result<FooterInfo> read_footer_info(const std::string &path);

sanitize::Result<std::string>
read_footer_info_json(const std::string &path,
                      const std::vector<std::string> &projected_columns = {});

sanitize::Result<ArrowArrayStream *>
make_arrow_stream(const std::string &path,
                  const std::vector<std::string> &projected_columns = {});

} // namespace sanitize::internal::parquet_footer_reader

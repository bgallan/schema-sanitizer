// Dispatches built-in frontend names to frontend factories.

#include "sanitize/registry/registry.hh"

#include <string_view>
#include <utility>

#include "internal/frontends/builtin_frontends.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/options/options.hh"

namespace sanitize {

FrontendHandle make_builtin_frontend(std::string_view name, ChunkSourcePtr src,
                                     const Options &opts) {
  if (name == "json")
    return internal::make_json_frontend(std::move(src), opts);
  if (name == "json_array")
    return internal::make_json_array_frontend(std::move(src), opts);
  if (name == "xml")
    return internal::make_xml_frontend(std::move(src), opts);
  if (name == "csv")
    return internal::make_csv_frontend(std::move(src), opts);
  return {};
}

} // namespace sanitize

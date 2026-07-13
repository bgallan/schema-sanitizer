// Shared Arrow C stream callbacks for registry-backed multi-source streams.

#pragma once

#include <string>

#include "api/python_abi3/metadata/stream/stream.hh"
#include "sanitize/core/status.hh"

struct ArrowArray;
struct ArrowArrayStream;
struct ArrowSchema;

namespace core_abi3_internal {

struct NativeMultiSourceStreamOps {
  const char *schema_context = nullptr;
  const char *next_context = nullptr;
  const char *empty_message = nullptr;
  const char *invalid_stream_message = nullptr;
  sanitize::Status (*open_next)(void *state) = nullptr;
  void (*close_current)(void *state) noexcept = nullptr;
  MetadataStreamState *(*metadata)(void *state) noexcept = nullptr;
  std::string &(*last_error)(void *state) noexcept = nullptr;
  bool *(*first_row_pending)(void *state) noexcept = nullptr;
  void (*destroy_state)(void *state) noexcept = nullptr;
};

const char *
native_multi_source_last_error(ArrowArrayStream *stream,
                               const NativeMultiSourceStreamOps &ops);

void native_multi_source_release(ArrowArrayStream *stream,
                                 const NativeMultiSourceStreamOps &ops);

int native_multi_source_get_schema(ArrowArrayStream *stream, ArrowSchema *out,
                                   const NativeMultiSourceStreamOps &ops);

int native_multi_source_get_next(ArrowArrayStream *stream, ArrowArray *out,
                                 const NativeMultiSourceStreamOps &ops);

} // namespace core_abi3_internal

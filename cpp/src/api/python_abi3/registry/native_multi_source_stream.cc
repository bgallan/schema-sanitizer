// Shared Arrow C stream callbacks for registry-backed multi-source streams.

#include "api/python_abi3/registry/native_multi_source_stream.hh"
#include "api/python_abi3/metadata/stream/stream.hh"

#include <cerrno>
#include <cstring>

#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/arrow_c/cdata_stream_runtime.hh"

namespace core_abi3_internal {

const char *
native_multi_source_last_error(ArrowArrayStream *stream,
                               const NativeMultiSourceStreamOps &ops) {
  if (!stream) {
    return ops.invalid_stream_message ? ops.invalid_stream_message
                                      : "invalid native multi-source stream";
  }
  if (!stream->private_data || !ops.last_error) {
    return nullptr;
  }
  return sanitize::internal::cdata_stream::last_error_ptr(
      ops.last_error(stream->private_data));
}

void native_multi_source_release(ArrowArrayStream *stream,
                                 const NativeMultiSourceStreamOps &ops) {
  if (!stream || !stream->release) {
    return;
  }
  void *state = stream->private_data;
  if (state && ops.close_current) {
    ops.close_current(state);
  }
  if (state && ops.destroy_state) {
    ops.destroy_state(state);
  }
  sanitize::internal::detach_task_arena(stream);
  sanitize::internal::cdata_stream::clear_stream(stream);
}

int native_multi_source_get_schema(ArrowArrayStream *stream, ArrowSchema *out,
                                   const NativeMultiSourceStreamOps &ops) {
  if (!stream || !out || !stream->private_data || !ops.last_error ||
      !ops.open_next || !ops.metadata) {
    return EINVAL;
  }
  void *state = stream->private_data;
  return sanitize::internal::cdata_stream::run_schema_callback(
      out, ops.last_error(state),
      ops.schema_context ? ops.schema_context : "multi_source.get_schema",
      [&](ArrowSchema *schema) {
        if (!ops.metadata(state)) {
          SAN_RETURN_NOT_OK(ops.open_next(state));
        }
        MetadataStreamState *metadata = ops.metadata(state);
        if (metadata && metadata->inner) {
          sanitize::internal::inherit_task_arena(stream, metadata->inner);
        }
        if (!metadata) {
          return sanitize::Status::Invalid(
              ops.empty_message ? ops.empty_message
                                : "native multi-source stream has no sources");
        }
        return build_metadata_schema(metadata, schema);
      });
}

int native_multi_source_get_next(ArrowArrayStream *stream, ArrowArray *out,
                                 const NativeMultiSourceStreamOps &ops) {
  if (!stream || !out || !stream->private_data || !ops.last_error ||
      !ops.open_next || !ops.metadata || !ops.close_current ||
      !ops.first_row_pending) {
    return EINVAL;
  }
  void *state = stream->private_data;
  return sanitize::internal::cdata_stream::run_array_callback(
      out, ops.last_error(state),
      ops.next_context ? ops.next_context : "multi_source.get_next",
      [&](ArrowArray *array) -> sanitize::Status {
        for (;;) {
          MetadataStreamState *metadata = ops.metadata(state);
          if (!metadata) {
            SAN_RETURN_NOT_OK(ops.open_next(state));
            metadata = ops.metadata(state);
            if (metadata && metadata->inner) {
              sanitize::internal::inherit_task_arena(stream, metadata->inner);
            }
            if (!metadata) {
              std::memset(array, 0, sizeof(*array));
              return sanitize::Status::OK();
            }
          }
          SAN_RETURN_NOT_OK(build_metadata_array(metadata, array));
          *ops.first_row_pending(state) = metadata->first_row_pending;
          if (array->release) {
            return sanitize::Status::OK();
          }
          ops.close_current(state);
        }
      });
}

} // namespace core_abi3_internal

/*
 * Arrow metadata stream builders.
 *
 * This file owns schema and array construction for the ABI3 metadata stream
 * wrapper. The Python-facing wrapper keeps lifecycle and capsule handling.
 */

#include "api/python_abi3/_core_abi3_metadata_stream_builders.hh"
#include "api/python_abi3/_core_abi3_metadata_stream_builder_parts.hh"

#include <memory>
#include <new>

namespace core_abi3_internal {

sanitize::Status build_metadata_array(MetadataStreamState *stream_state,
                                      ArrowArray *out) {
  if (!stream_state || !stream_state->inner) {
    return sanitize::Status::Invalid("metadata stream is closed");
  }
  std::unique_ptr<MetadataArrayState> state(new (std::nothrow)
                                                MetadataArrayState());
  if (!state) {
    return sanitize::Status::OutOfMemory("metadata stream array OOM");
  }

  const int rc =
      stream_state->inner->get_next(stream_state->inner, state->base.get());
  if (rc != 0) {
    return sanitize::Status::IOError("metadata stream inner get_next failed");
  }
  ArrowArray &base = state->base.value();
  if (!base.release) {
    clear_array(out);
    return sanitize::Status::OK();
  }
  if (base.n_children < 0) {
    return sanitize::Status::Invalid(
        "metadata stream base array has invalid children");
  }

  const int64_t length = base.length;
  state->metadata.resize(stream_state->columns.size());
  state->children.reserve(static_cast<std::size_t>(base.n_children) +
                          state->metadata.size());
  for (int64_t i = 0; i < base.n_children; ++i) {
    state->children.push_back(base.children[i]);
  }
  for (std::size_t i = 0; i < stream_state->columns.size(); ++i) {
    SAN_RETURN_NOT_OK(build_metadata_column_array(
        &state->metadata[i], &stream_state->columns[i], length,
        stream_state->first_row_pending));
    state->children.push_back(state->metadata[i].array);
  }

  clear_array(out);
  out->length = length;
  out->null_count = base.null_count;
  out->offset = base.offset;
  out->n_buffers = base.n_buffers;
  out->buffers = base.buffers ? base.buffers : state->struct_buffers;
  out->n_children = static_cast<int64_t>(state->children.size());
  out->children = state->children.empty() ? nullptr : state->children.data();
  out->dictionary = base.dictionary;
  out->private_data = state.release();
  out->release = &metadata_array_release;
  if (length > 0) {
    stream_state->first_row_pending = false;
  }
  return sanitize::Status::OK();
}

} // namespace core_abi3_internal

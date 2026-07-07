/*
 * Arrow metadata stream schema builder.
 *
 * This file owns construction of wrapper schemas that append generated metadata
 * columns to the schema exported by the inner Arrow C stream.
 */
#include "api/python_abi3/_core_abi3_metadata_stream_builders.hh"

#include "api/python_abi3/_core_abi3_metadata_stream_builder_parts.hh"

#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include <unordered_set>
#include <utility>

namespace core_abi3_internal {

sanitize::Status append_metadata_schema_children(MetadataSchemaState *state) {
  if (!state) {
    return sanitize::Status::Invalid("metadata schema state is null");
  }
  ArrowSchema &base = state->base.value();
  const int64_t base_children = base.n_children;
  if (base_children < 0) {
    return sanitize::Status::Invalid(
        "metadata stream base schema has invalid children");
  }
  std::unordered_set<std::string> names;
  names.reserve(static_cast<std::size_t>(base_children) +
                state->metadata.size());
  for (int64_t i = 0; i < base_children; ++i) {
    const char *name = base.children[i] ? base.children[i]->name : nullptr;
    if (name && !names.insert(name).second) {
      return sanitize::Status::Invalid(
          "metadata stream base schema has duplicate column names");
    }
  }
  for (const auto &child : state->metadata) {
    if (!names.insert(child.name).second) {
      return sanitize::Status::Invalid("generated metadata column '" +
                                       child.name +
                                       "' already exists in output schema");
    }
  }
  state->children.reserve(static_cast<std::size_t>(base_children) +
                          state->metadata.size());
  for (int64_t i = 0; i < base_children; ++i) {
    state->children.push_back(base.children[i]);
  }
  for (auto &child : state->metadata) {
    clear_schema(&child.schema);
    child.schema.format = child.format.c_str();
    child.schema.name = child.name.c_str();
    child.schema.metadata = nullptr;
    child.schema.flags = ARROW_FLAG_NULLABLE;
    child.schema.n_children = 0;
    child.schema.children = nullptr;
    child.schema.dictionary = nullptr;
    child.schema.private_data = nullptr;
    child.schema.release = &metadata_schema_child_release;
    state->children.push_back(&child.schema);
  }
  return sanitize::Status::OK();
}

sanitize::Status build_metadata_schema(MetadataStreamState *stream_state,
                                       ArrowSchema *out) {
  if (!stream_state || !stream_state->inner) {
    return sanitize::Status::Invalid("metadata stream is closed");
  }
  std::unique_ptr<MetadataSchemaState> state(new (std::nothrow)
                                                 MetadataSchemaState());
  if (!state) {
    return sanitize::Status::OutOfMemory("metadata stream schema OOM");
  }
  state->metadata.reserve(stream_state->columns.size());
  for (const auto &column : stream_state->columns) {
    MetadataSchemaChild child;
    child.name = column.name;
    child.format =
        column.placement == MetadataColumnPlacement::AllRowsTimestampMicros
            ? "tsu:"
            : "u";
    state->metadata.push_back(std::move(child));
  }

  const int rc =
      stream_state->inner->get_schema(stream_state->inner, state->base.get());
  if (rc != 0) {
    return sanitize::Status::IOError("metadata stream inner get_schema failed");
  }
  SAN_RETURN_NOT_OK(append_metadata_schema_children(state.get()));

  ArrowSchema &base = state->base.value();
  clear_schema(out);
  out->format = base.format;
  out->name = base.name;
  out->metadata = base.metadata;
  out->flags = base.flags;
  out->n_children = static_cast<int64_t>(state->children.size());
  out->children = state->children.empty() ? nullptr : state->children.data();
  out->dictionary = base.dictionary;
  out->private_data = state.release();
  out->release = &metadata_schema_release;
  return sanitize::Status::OK();
}

} // namespace core_abi3_internal

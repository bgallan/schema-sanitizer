// Internal model and phase boundaries for Arrow C stream batch coalescing.
#pragma once

#include "internal/abi/python_abi3/base.hh"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/status.hh"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace core_abi3_internal::coalesce_detail {

enum class CoalesceKind {
  kStruct,
  kList32,
  kList64,
  kUtf8,
  kLargeUtf8,
  kBinary,
  kLargeBinary,
  kFixedWidth,
  kBool,
  kDictionary,
};

struct CoalesceNodeSpec {
  std::string format;
  CoalesceKind kind = CoalesceKind::kFixedWidth;
  std::size_t fixed_width = 0;
  std::vector<CoalesceNodeSpec> children;
};

struct CoalescedNode {
  ArrowArray array{};
  std::vector<std::uint8_t> validity;
  std::vector<std::uint8_t> data;
  std::vector<std::int32_t> offsets32;
  std::vector<std::int64_t> offsets64;
  std::vector<CoalescedNode> children;
  std::vector<ArrowArray *> child_ptrs;
  std::unique_ptr<CoalescedNode> dictionary;
  ArrowArray *dictionary_ptr = nullptr;
  bool dictionary_ready = false;
  const void *buffers[3]{nullptr, nullptr, nullptr};
};

struct CoalescedArrayState {
  CoalescedNode root;
};

struct ArraySlice {
  const ArrowArray *array = nullptr;
  std::int64_t offset = 0;
  std::int64_t length = 0;
};

struct CoalesceStreamState {
  ArrowArrayStream *inner = nullptr;
  PyObject *stream_obj = nullptr;
  PyObject *stream_capsule = nullptr;
  CoalesceNodeSpec root;
  std::int64_t target_rows = 65536;
  std::size_t target_bytes = 64 * 1024 * 1024;
  std::size_t max_batch_bytes = 512 * 1024 * 1024;
  std::int64_t max_logical_slots = 100'000'000;
  std::int64_t max_logical_buffer_bytes = 1LL << 30;
  ArrowArray pending_array{};
  std::int64_t pending_offset = 0;
  bool inner_eof = false;
  std::string last_error;
  bool closed = false;
};

[[nodiscard]] bool schema_supported(const ArrowSchema &schema,
                                    CoalesceNodeSpec *root);
[[nodiscard]] std::size_t retained_bytes(const CoalescedNode &node);
[[nodiscard]] std::int64_t fitting_slice_rows(const CoalesceNodeSpec &spec,
                                              const ArrowArray &array,
                                              std::int64_t offset,
                                              std::int64_t max_rows,
                                              std::size_t max_bytes) noexcept;
sanitize::Status validate_arrow_node(const CoalesceNodeSpec &spec,
                                     const ArrowArray &array,
                                     std::int64_t offset, std::int64_t length,
                                     std::size_t depth,
                                     std::int64_t max_logical_slots,
                                     std::int64_t max_logical_buffer_bytes);

sanitize::Status append_node(const CoalesceNodeSpec &spec, CoalescedNode *out,
                             const ArraySlice &slice);
sanitize::Status finish_node(CoalescedNode *node, const CoalesceNodeSpec &spec,
                             bool root);
sanitize::Status
export_coalesced_array(std::unique_ptr<CoalescedArrayState> state,
                       ArrowArray *out);

void release_coalesce_stream(CoalesceStreamState *state) noexcept;
const char *coalesce_last_error(ArrowArrayStream *stream);
void coalesce_release(ArrowArrayStream *stream);
int coalesce_get_schema(ArrowArrayStream *stream, ArrowSchema *out);
int coalesce_get_next(ArrowArrayStream *stream, ArrowArray *out);

} // namespace core_abi3_internal::coalesce_detail

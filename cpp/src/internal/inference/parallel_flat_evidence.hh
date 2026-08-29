// Declares flat packet-local inference helpers.
// The code keeps bounded shape discovery and scalar evidence consistent across
// serial and parallel scans.

#pragma once

#include "internal/inference/parallel_evidence.hh"
#include "internal/parsing/json/ondemand/document.hh"

#include "internal/runtime/thread_compat.hh"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace sanitize::internal {

sanitize::Status append_flat_inference_value(
    InferenceEvidencePacket *packet, std::string_view key,
    const ValueView &value, const PreparedOptions &opts,
    sanitize::internal::StopToken stop, std::size_t *ordered_index,
    const std::shared_ptr<MemoryPool> &parent_pool,
    std::int64_t packet_memory_limit);

sanitize::Status append_flat_json_inference_row(
    JsonOnDemandDoc *document, std::string_view raw, std::size_t base_offset,
    InferenceEvidencePacket *packet, const PreparedOptions &opts,
    const std::shared_ptr<MemoryPool> &parent_pool,
    std::int64_t packet_memory_limit, sanitize::internal::StopToken stop);

} // namespace sanitize::internal

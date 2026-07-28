// Declares flat packet-local inference helpers.
#pragma once

#include "internal/inference/parallel_evidence.hh"
#include "internal/parsing/json/ondemand/document.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stop_token>
#include <string_view>

namespace sanitize::internal {

sanitize::Status append_flat_inference_value(
    InferenceEvidencePacket *packet, std::string_view key,
    const ValueView &value, const PreparedOptions &opts, std::stop_token stop,
    std::size_t *ordered_index, const std::shared_ptr<MemoryPool> &parent_pool,
    std::int64_t packet_memory_limit);

sanitize::Status append_flat_json_inference_row(
    JsonOnDemandDoc *document, std::string_view raw, std::size_t base_offset,
    InferenceEvidencePacket *packet, const PreparedOptions &opts,
    const std::shared_ptr<MemoryPool> &parent_pool,
    std::int64_t packet_memory_limit, std::stop_token stop);

} // namespace sanitize::internal

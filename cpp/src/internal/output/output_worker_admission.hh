// Derives bounded text-output worker admission across Arrow batches.
#pragma once

#include "internal/runtime/execution_policy.hh"

#include <algorithm>
#include <cstdint>
#include <limits>

namespace sanitize::internal::ordered_text_output {

struct OutputAdmissionState final {
  std::int64_t accumulated_work_items = 0;
};

[[nodiscard]] constexpr std::int64_t
saturated_work_item_sum(std::int64_t left, std::int64_t right) noexcept {
  const auto maximum = std::numeric_limits<std::int64_t>::max();
  const auto positive_left = std::max<std::int64_t>(0, left);
  const auto positive_right = std::max<std::int64_t>(0, right);
  return positive_left > maximum - positive_right
             ? maximum
             : positive_left + positive_right;
}

[[nodiscard]] constexpr ExecutionPolicy select_output_admission(
    const ExecutionPolicy &base_policy, std::int64_t work_items,
    std::int64_t accumulated_items_per_worker,
    bool geometric_accumulated_admission, bool full_worker_admission,
    OutputAdmissionState *state) noexcept {
  if (full_worker_admission) {
    return base_policy;
  }

  const auto normalized_items = std::max<std::int64_t>(1, work_items);
  if (state) {
    state->accumulated_work_items = saturated_work_item_sum(
        state->accumulated_work_items, normalized_items);
  }
  const auto admission_items = geometric_accumulated_admission && state
                                   ? state->accumulated_work_items
                                   : normalized_items;
  auto desired = execution_policy_for_work_items(
      base_policy, admission_items, accumulated_items_per_worker, 1);
  if (!geometric_accumulated_admission) {
    return desired;
  }

  std::int64_t geometric_workers = 1;
  while (geometric_workers < desired.effective_workers &&
         geometric_workers <= base_policy.effective_workers / 2) {
    geometric_workers *= 2;
  }
  return execution_policy_with_worker_ceiling(
      base_policy, std::min(base_policy.effective_workers, geometric_workers),
      1);
}

[[nodiscard]] constexpr bool
output_admission_requires_sampling(bool full_worker_admission) noexcept {
  return !full_worker_admission;
}

} // namespace sanitize::internal::ordered_text_output

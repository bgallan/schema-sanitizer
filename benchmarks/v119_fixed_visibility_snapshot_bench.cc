#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>

namespace {

struct alignas(64) VisibilityShard final {
  std::atomic<std::uint64_t> nonempty_mask{0};
};

struct State final {
  std::size_t worker_count = 0;
  VisibilityShard primary;
  std::array<VisibilityShard, 3> extra;
};

[[gnu::noinline]] std::uint64_t BaselineSnapshot(
    State &state, std::uint64_t allowed) noexcept {
  std::uint64_t snapshot = 0;
  auto remaining = allowed;
  if ((remaining & std::uint64_t{0xFF}) != 0U) {
    snapshot = state.primary.nonempty_mask.load(std::memory_order_acquire);
    remaining &= ~std::uint64_t{0xFF};
  }
  while (remaining != 0U) {
    const auto shard_index =
        (static_cast<std::size_t>(std::countr_zero(remaining)) >> 3U) - 1U;
    snapshot |= state.extra[shard_index].nonempty_mask.load(
        std::memory_order_acquire);
    remaining &=
        ~(std::uint64_t{0xFF} << ((shard_index + 1U) * 8U));
  }
  return snapshot & allowed;
}

[[gnu::noinline]] std::uint64_t CandidateSnapshot(
    State &state, std::uint64_t allowed) noexcept {
  auto snapshot =
      state.primary.nonempty_mask.load(std::memory_order_acquire);
  if (state.worker_count > 8U) {
    snapshot |=
        state.extra[0].nonempty_mask.load(std::memory_order_acquire);
  }
  if (state.worker_count > 16U) {
    snapshot |=
        state.extra[1].nonempty_mask.load(std::memory_order_acquire);
  }
  if (state.worker_count > 24U) {
    snapshot |=
        state.extra[2].nonempty_mask.load(std::memory_order_acquire);
  }
  return snapshot & allowed;
}

} // namespace

int main(int argc, char **argv) {
  if (argc != 4) {
    return 2;
  }
  const std::string_view mode = argv[1];
  const auto worker_count =
      static_cast<std::size_t>(std::strtoull(argv[2], nullptr, 10));
  const auto iterations = std::strtoull(argv[3], nullptr, 10);
  if (worker_count < 9U || worker_count > 32U ||
      (mode != "baseline" && mode != "candidate")) {
    return 3;
  }

  State state;
  state.worker_count = worker_count;
  state.primary.nonempty_mask.store(0xA5U, std::memory_order_relaxed);
  state.extra[0].nonempty_mask.store(0x5A00U, std::memory_order_relaxed);
  state.extra[1].nonempty_mask.store(0xC30000U, std::memory_order_relaxed);
  state.extra[2].nonempty_mask.store(0x3C000000U,
                                      std::memory_order_relaxed);
  const auto full_allowed =
      (std::uint64_t{1} << worker_count) - std::uint64_t{1};

  std::uint64_t checksum = 0;
  const auto started = std::chrono::steady_clock::now();
  for (std::uint64_t iteration = 0; iteration < iterations; ++iteration) {
    // Model a long steady-state interval with occasional partial-startup
    // prefixes. Both variants return the exact same masked snapshot.
    const auto prefix_width =
        1U + static_cast<std::size_t>(iteration % worker_count);
    const auto allowed =
        iteration % 257U == 0U
            ? (std::uint64_t{1} << prefix_width) - std::uint64_t{1}
            : full_allowed;
    checksum ^= mode == "baseline"
                    ? BaselineSnapshot(state, allowed)
                    : CandidateSnapshot(state, allowed);
  }
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now() - started)
                           .count();
  std::cout << elapsed << ' ' << checksum << '\n';
}

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::size_t kIterations = 2'000'000;

enum class Role : unsigned char { kUpstream, kOutput, kAll, kActivity };

// Mirrors the baseline arena layout: independent producer cursors, startup state,
// and worker activity share the same compact state region.
struct alignas(64) BaselineState final {
  std::atomic<std::size_t> upstream_cursor{0};
  std::atomic<std::size_t> output_cursor{0};
  std::atomic<std::size_t> all_cursor{0};
  std::atomic<bool> stopping{false};
  std::atomic<std::uint64_t> admitted_mask{0};
  std::atomic<std::uint64_t> started_mask{0};
  std::atomic<std::uint64_t> initialized_mask{0};
  std::atomic<std::size_t> active{0};
  std::atomic<std::size_t> peak_active{0};
};

// Mirrors the candidate layout: each independent ticket domain and the worker activity domain
// begins on a separate cache line. Orders and atomic operations are unchanged.
struct CandidateState final {
  alignas(64) std::atomic<std::size_t> upstream_cursor{0};
  alignas(64) std::atomic<std::size_t> output_cursor{0};
  alignas(64) std::atomic<std::size_t> all_cursor{0};
  alignas(64) std::atomic<bool> stopping{false};
  std::atomic<std::uint64_t> admitted_mask{0};
  std::atomic<std::uint64_t> started_mask{0};
  std::atomic<std::uint64_t> initialized_mask{0};
  alignas(64) std::atomic<std::size_t> active{0};
  std::atomic<std::size_t> peak_active{0};
};

[[nodiscard]] std::vector<Role> scenario(std::string_view name) {
  if (name == "cursor_activity") {
    return {Role::kUpstream, Role::kActivity};
  }
  if (name == "two_cursors_activity") {
    return {Role::kUpstream, Role::kOutput, Role::kActivity};
  }
  if (name == "three_cursors_two_activity") {
    return {Role::kUpstream, Role::kOutput, Role::kAll, Role::kActivity,
            Role::kActivity};
  }
  return {};
}

template <typename State>
[[nodiscard]] std::uint64_t run(const std::vector<Role> &roles) {
  State state;
  std::barrier start_gate(static_cast<std::ptrdiff_t>(roles.size() + 1U));
  std::vector<std::jthread> threads;
  threads.reserve(roles.size());
  for (const auto role : roles) {
    threads.emplace_back([&state, &start_gate, role] {
      start_gate.arrive_and_wait();
      for (std::size_t iteration = 0; iteration < kIterations; ++iteration) {
        switch (role) {
        case Role::kUpstream:
          state.upstream_cursor.fetch_add(1, std::memory_order_relaxed);
          break;
        case Role::kOutput:
          state.output_cursor.fetch_add(1, std::memory_order_relaxed);
          break;
        case Role::kAll:
          state.all_cursor.fetch_add(1, std::memory_order_relaxed);
          break;
        case Role::kActivity:
          state.active.fetch_add(1, std::memory_order_relaxed);
          state.active.fetch_sub(1, std::memory_order_release);
          break;
        }
      }
    });
  }
  start_gate.arrive_and_wait();
  const auto started = Clock::now();
  threads.clear();
  const auto elapsed = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                           started)
          .count());
  return elapsed;
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 3) {
    return 2;
  }
  const auto roles = scenario(argv[1]);
  if (roles.empty()) {
    return 3;
  }
  const std::string_view variant = argv[2];
  if (variant == "baseline") {
    std::cout << run<BaselineState>(roles) << '\n';
    return 0;
  }
  if (variant == "candidate") {
    std::cout << run<CandidateState>(roles) << '\n';
    return 0;
  }
  return 4;
}

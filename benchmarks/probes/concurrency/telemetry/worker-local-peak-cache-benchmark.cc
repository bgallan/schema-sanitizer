#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <thread>
#include <vector>

namespace {
constexpr std::size_t kSamples = 64U;

bool update_peak(std::atomic<std::size_t> *peak,
                 std::size_t value) noexcept {
  auto observed = peak->load(std::memory_order_relaxed);
  while (observed < value &&
         !peak->compare_exchange_weak(observed, value,
                                      std::memory_order_relaxed,
                                      std::memory_order_relaxed)) {
  }
  return observed < value;
}

std::uint64_t run(bool candidate, std::size_t workers,
                  std::size_t iterations_per_worker) {
  std::atomic<std::size_t> peak{0};
  std::array<std::atomic<std::size_t>, kSamples> active_samples{};
  for (std::size_t index = 0; index < active_samples.size(); ++index) {
    active_samples[index].store(1U + (index % workers),
                                std::memory_order_relaxed);
  }

  std::barrier start(static_cast<std::ptrdiff_t>(workers + 1U));
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  std::atomic<std::uint64_t> checksum{0};

  for (std::size_t worker = 0; worker < workers; ++worker) {
    threads.emplace_back([&, worker] {
      std::size_t local_peak_active = 0;
      std::uint64_t local_checksum = 0;
      start.arrive_and_wait();
      for (std::size_t iteration = 0;
           iteration < iterations_per_worker; ++iteration) {
        const auto active = active_samples[
            (iteration + worker) & (kSamples - 1U)]
                                .load(std::memory_order_relaxed);
        bool established_new_peak = false;
        if (candidate) {
          if (active > local_peak_active) {
            local_peak_active = active;
            established_new_peak = update_peak(&peak, active);
          }
        } else {
          established_new_peak = update_peak(&peak, active);
        }
        local_checksum += active +
                          static_cast<std::size_t>(established_new_peak);
      }
      checksum.fetch_add(local_checksum, std::memory_order_relaxed);
    });
  }

  start.arrive_and_wait();
  const auto begin = std::chrono::steady_clock::now();
  for (auto &thread : threads) {
    thread.join();
  }
  const auto end = std::chrono::steady_clock::now();

  if (peak.load(std::memory_order_relaxed) != workers ||
      checksum.load(std::memory_order_relaxed) == 0U) {
    std::abort();
  }
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
          .count());
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "usage: bench baseline|candidate workers iterations_per_worker\n";
    return 2;
  }
  const bool candidate = std::string_view(argv[1]) == "candidate";
  const auto workers = static_cast<std::size_t>(std::stoull(argv[2]));
  const auto iterations = static_cast<std::size_t>(std::stoull(argv[3]));
  std::cout << run(candidate, workers, iterations) << '\n';
  return 0;
}

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

enum class Kind : unsigned char { Other, Output };
using Task = std::move_only_function<void(std::size_t, std::stop_token)>;

struct Baseline {
  Task task;
  std::size_t lane_begin = 0;
  std::size_t lane_end = 1;
  Kind kind = Kind::Other;
  std::int64_t queued_at_ns = 0;
};
struct Compact {
  Task task;
  std::uint8_t lane_begin = 0;
  std::uint8_t lane_end = 1;
  Kind kind = Kind::Other;
  std::int64_t queued_at_ns = 0;
};

template <class P>
P make_packet(std::size_t i) {
  const auto begin = static_cast<std::uint8_t>((i * 7U) % 24U);
  const auto width = static_cast<std::uint8_t>(1U + ((i * 5U) % 8U));
  const auto end = static_cast<std::uint8_t>(std::min<unsigned>(32U, begin + width));
  return P{
      .task = [x = static_cast<std::uint64_t>(i)](std::size_t worker, std::stop_token) {
        volatile auto y = x + worker;
        (void)y;
      },
      .lane_begin = static_cast<decltype(P::lane_begin)>(begin),
      .lane_end = static_cast<decltype(P::lane_end)>(end),
      .kind = (i & 3U) == 0U ? Kind::Output : Kind::Other,
      .queued_at_ns = static_cast<std::int64_t>(i),
  };
}

template <class P>
std::uint64_t run(std::size_t iterations, std::size_t queue_size) {
  std::deque<P> q;
  for (std::size_t i = 0; i < queue_size; ++i) q.push_back(make_packet<P>(i));
  std::uint64_t checksum = 0;
  for (std::size_t i = 0; i < iterations; ++i) {
    const std::size_t worker = (i * 13U) & 31U;
    auto reverse = std::find_if(q.rbegin(), q.rend(), [worker](const P &p) {
      return worker >= static_cast<std::size_t>(p.lane_begin) &&
             worker < static_cast<std::size_t>(p.lane_end);
    });
    if (reverse == q.rend()) {
      auto p = std::move(q.front());
      q.pop_front();
      checksum += static_cast<std::uint64_t>(p.queued_at_ns);
      p.queued_at_ns += 1;
      q.push_back(std::move(p));
    } else {
      auto it = std::prev(reverse.base());
      P p = std::move(*it);
      q.erase(it);
      checksum += static_cast<std::uint64_t>(p.queued_at_ns) +
                  static_cast<std::size_t>(p.lane_begin) +
                  static_cast<std::size_t>(p.lane_end);
      p.queued_at_ns += 1;
      q.push_front(std::move(p));
    }
  }
  return checksum + q.size();
}

int main(int argc, char **argv) {
  if (argc != 4) return 2;
  const std::string_view mode(argv[1]);
  const auto iters = static_cast<std::size_t>(std::stoull(argv[2]));
  const auto qsize = static_cast<std::size_t>(std::stoull(argv[3]));
  const auto start = std::chrono::steady_clock::now();
  std::uint64_t sum = 0;
  if (mode == "baseline") sum = run<Baseline>(iters, qsize);
  else if (mode == "compact") sum = run<Compact>(iters, qsize);
  else return 3;
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - start).count();
  std::cout << ns << " " << sum << " " << sizeof(Baseline) << " " << sizeof(Compact) << "\n";
}

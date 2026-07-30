#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <utility>

namespace {
using Abandon = void (*)(void *, std::size_t) noexcept;

inline void escape(const void *p) noexcept {
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" : : "g"(p) : "memory");
#else
  (void)p;
#endif
}
void abandon(void *, std::size_t) noexcept {}

class BaselineLease final {
public:
  BaselineLease(void *owner, std::size_t shard, Abandon callback) noexcept
      : owner_(owner), shard_(shard), callback_(callback) {}
  BaselineLease(BaselineLease &&other) noexcept
      : owner_(other.owner_), shard_(other.shard_), callback_(other.callback_) {
    other.Complete();
  }
  ~BaselineLease() { if (owner_ && callback_) callback_(owner_, shard_); }
  void Complete() noexcept { owner_ = nullptr; callback_ = nullptr; }
private:
  void *owner_;
  std::size_t shard_;
  Abandon callback_;
};

class SentinelLease final {
public:
  SentinelLease(void *owner, std::size_t shard, Abandon callback) noexcept
      : owner_(owner), shard_(shard), callback_(callback) {}
  SentinelLease(SentinelLease &&other) noexcept
      : owner_(other.owner_), shard_(other.shard_), callback_(other.callback_) {
    other.owner_ = nullptr;
  }
  ~SentinelLease() { if (owner_ && callback_) callback_(owner_, shard_); }
  void Complete() noexcept { owner_ = nullptr; }
private:
  void *owner_;
  std::size_t shard_;
  Abandon callback_;
};

template <class Lease>
std::int64_t run(std::size_t iterations) {
  int owner = 0;
  const auto begin = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < iterations; ++i) {
    Lease source(&owner, i & 31U, abandon);
    Lease moved(std::move(source));
    escape(&source);
    escape(&moved);
    moved.Complete();
    escape(&moved);
  }
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - begin)
      .count();
}
} // namespace

int main(int argc, char **argv) {
  const auto iterations = argc > 1
      ? static_cast<std::size_t>(std::strtoull(argv[1], nullptr, 10))
      : 20'000'000U;
  const bool sentinel = argc > 2 && argv[2][0] == 's';
  std::cout << (sentinel ? run<SentinelLease>(iterations)
                         : run<BaselineLease>(iterations)) << '\n';
}

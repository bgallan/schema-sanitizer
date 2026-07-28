#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <type_traits>
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

class V100Lease final {
public:
  V100Lease(void *owner, std::size_t shard, Abandon callback) noexcept
      : owner_(owner), shard_(shard), callback_(callback) {}
  V100Lease(V100Lease &&other) noexcept
      : owner_(other.owner_), shard_(other.shard_), callback_(other.callback_) {
    other.owner_ = nullptr;
  }
  ~V100Lease() {
    if (owner_ && callback_) {
      callback_(owner_, shard_);
    }
  }
  [[nodiscard]] std::size_t shard() const noexcept { return shard_; }
  void Complete() noexcept { owner_ = nullptr; }

private:
  void *owner_;
  std::size_t shard_;
  Abandon callback_;
};

template <void (*AbandonPolicy)(void *, std::size_t) noexcept>
class V101Lease final {
public:
  V101Lease(void *owner, std::size_t shard) noexcept
      : owner_(owner), shard_(shard) {}
  V101Lease(V101Lease &&other) noexcept
      : owner_(other.owner_), shard_(other.shard_) {
    other.owner_ = nullptr;
  }
  ~V101Lease() {
    if (owner_) {
      AbandonPolicy(owner_, shard_);
    }
  }
  [[nodiscard]] std::size_t shard() const noexcept { return shard_; }
  void Complete() noexcept { owner_ = nullptr; }

private:
  void *owner_;
  std::size_t shard_;
};

using StaticLease = V101Lease<abandon>;
static_assert(sizeof(V100Lease) == 3U * sizeof(void *));
static_assert(sizeof(StaticLease) == 2U * sizeof(void *));

template <class Lease>
std::int64_t run(std::size_t iterations) {
  int owner = 0;
  std::size_t shard_total = 0;
  const auto begin = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < iterations; ++i) {
    if constexpr (std::is_same_v<Lease, V100Lease>) {
      Lease source(&owner, i & 31U, abandon);
      Lease moved(std::move(source));
      shard_total += moved.shard();
      escape(&source);
      escape(&moved);
      moved.Complete();
      escape(&moved);
    } else {
      Lease source(&owner, i & 31U);
      Lease moved(std::move(source));
      shard_total += moved.shard();
      escape(&source);
      escape(&moved);
      moved.Complete();
      escape(&moved);
    }
  }
  escape(&shard_total);
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - begin)
      .count();
}
} // namespace

int main(int argc, char **argv) {
  const auto iterations = argc > 1
      ? static_cast<std::size_t>(std::strtoull(argv[1], nullptr, 10))
      : 20'000'000U;
  const bool candidate = argc > 2 && argv[2][0] == 'c';
  std::cout << (candidate ? run<StaticLease>(iterations)
                          : run<V100Lease>(iterations))
            << '\n';
}

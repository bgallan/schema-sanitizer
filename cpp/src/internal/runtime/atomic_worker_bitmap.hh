// Dynamically sized lock-free worker bitmap for wide task arenas.
#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace sanitize::internal {

// Stores one bit per physical worker in 64-bit shards. The hot operations are
// lock-free and scan words rather than individual workers, so arena scheduling
// has no 32-worker representation ceiling and degrades by N/64, not N.
class AtomicWorkerBitmap final {
public:
  explicit AtomicWorkerBitmap(std::size_t bit_count)
      : word_count_((bit_count + 63U) / 64U),
        summary_word_count_((word_count_ + 63U) / 64U),
        words_(
            word_count_ == 0
                ? nullptr
                : std::make_unique<std::atomic<std::uint64_t>[]>(word_count_)),
        nonempty_words_(summary_word_count_ == 0
                            ? nullptr
                            : std::make_unique<std::atomic<std::uint64_t>[]>(
                                  summary_word_count_)) {
    Reset();
  }

  AtomicWorkerBitmap(const AtomicWorkerBitmap &) = delete;
  AtomicWorkerBitmap &operator=(const AtomicWorkerBitmap &) = delete;

  void Set(std::size_t index,
           std::memory_order order = std::memory_order_release) noexcept {
    const auto word = index >> 6U;
    words_[word].fetch_or(bit(index), order);
    mark_word_nonempty(word);
  }

  void Clear(std::size_t index,
             std::memory_order order = std::memory_order_release) noexcept {
    const auto word = index >> 6U;
    const auto previous = words_[word].fetch_and(~bit(index), order);
    if ((previous & ~bit(index)) == 0U) {
      mark_word_empty(word);
    }
  }

  [[nodiscard]] bool
  Test(std::size_t index,
       std::memory_order order = std::memory_order_acquire) const noexcept {
    return (words_[index >> 6U].load(order) & bit(index)) != 0U;
  }

  // Atomically claims the first clear bit in [begin, end), searching from the
  // normalized origin and wrapping once inside the lane.
  [[nodiscard]] std::size_t TrySetFirstClear(std::size_t begin, std::size_t end,
                                             std::size_t origin) noexcept {
    if (begin >= end) {
      return end;
    }
    const auto width = end - begin;
    const auto start = begin + (origin % width);
    auto found = TrySetFirstClearRange(start, end);
    if (found != end || start == begin) {
      return found;
    }
    found = TrySetFirstClearRange(begin, start);
    return found == start ? end : found;
  }

  // Returns the first bit which is set in this bitmap and clear in `excluded`.
  [[nodiscard]] std::size_t FindSetNotIn(const AtomicWorkerBitmap &excluded,
                                         std::size_t begin, std::size_t end,
                                         std::size_t origin) const noexcept {
    if (begin >= end) {
      return end;
    }
    const auto width = end - begin;
    const auto start = begin + (origin % width);
    auto found = FindSetNotInRange(excluded, start, end);
    if (found != end || start == begin) {
      return found;
    }
    found = FindSetNotInRange(excluded, begin, start);
    return found == start ? end : found;
  }

  template <class Visitor>
  void VisitIntersection(const AtomicWorkerBitmap &other, std::size_t begin,
                         std::size_t end, std::size_t origin,
                         Visitor &&visitor) const noexcept {
    if (begin >= end) {
      return;
    }
    const auto width = end - begin;
    const auto start = begin + (origin % width);
    VisitIntersectionRange(other, start, end, visitor);
    if (start != begin) {
      VisitIntersectionRange(other, begin, start, visitor);
    }
  }

  [[nodiscard]] std::size_t Count() const noexcept {
    std::size_t total = 0;
    for (std::size_t index = 0; index < word_count_; ++index) {
      total += static_cast<std::size_t>(
          std::popcount(words_[index].load(std::memory_order_acquire)));
    }
    return total;
  }

  void Reset() noexcept {
    for (std::size_t index = 0; index < word_count_; ++index) {
      words_[index].store(0, std::memory_order_relaxed);
    }
    for (std::size_t index = 0; index < summary_word_count_; ++index) {
      nonempty_words_[index].store(0, std::memory_order_relaxed);
    }
  }

private:
  void mark_word_nonempty(std::size_t word) noexcept {
    nonempty_words_[word >> 6U].fetch_or(bit(word), std::memory_order_release);
  }

  void mark_word_empty(std::size_t word) noexcept {
    auto &summary = nonempty_words_[word >> 6U];
    summary.fetch_and(~bit(word), std::memory_order_acq_rel);
    // A concurrent Set may have published the data word immediately before
    // the summary clear. Recheck after clearing so the summary can never stay
    // falsely empty; a later Set publishes its own summary bit.
    if (words_[word].load(std::memory_order_acquire) != 0U) {
      summary.fetch_or(bit(word), std::memory_order_release);
    }
  }

  [[nodiscard]] static constexpr std::uint64_t bit(std::size_t index) noexcept {
    return std::uint64_t{1} << (index & 63U);
  }

  [[nodiscard]] static constexpr std::uint64_t
  range_mask(std::size_t word, std::size_t begin, std::size_t end) noexcept {
    const auto word_begin = word << 6U;
    const auto low = begin > word_begin ? begin - word_begin : 0U;
    const auto word_end = word_begin + 64U;
    const auto high = end < word_end ? end - word_begin : 64U;
    const auto below_high = high == 64U
                                ? ~std::uint64_t{0}
                                : (std::uint64_t{1} << high) - std::uint64_t{1};
    const auto below_low = low == 0U
                               ? std::uint64_t{0}
                               : (std::uint64_t{1} << low) - std::uint64_t{1};
    return below_high & ~below_low;
  }

  [[nodiscard]] std::size_t TrySetFirstClearRange(std::size_t begin,
                                                  std::size_t end) noexcept {
    if (begin >= end) {
      return end;
    }
    const auto first_word = begin >> 6U;
    const auto last_word = (end - 1U) >> 6U;
    for (auto word = first_word; word <= last_word; ++word) {
      const auto allowed = range_mask(word, begin, end);
      auto observed = words_[word].load(std::memory_order_acquire);
      while (true) {
        const auto available = allowed & ~observed;
        if (available == 0U) {
          break;
        }
        const auto selected = std::uint64_t{1} << std::countr_zero(available);
        if (words_[word].compare_exchange_weak(observed, observed | selected,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
          mark_word_nonempty(word);
          return (word << 6U) +
                 static_cast<std::size_t>(std::countr_zero(selected));
        }
      }
    }
    return end;
  }

  [[nodiscard]] std::size_t
  FindSetNotInRange(const AtomicWorkerBitmap &excluded, std::size_t begin,
                    std::size_t end) const noexcept {
    if (begin >= end) {
      return end;
    }
    const auto first_word = begin >> 6U;
    const auto last_word = (end - 1U) >> 6U;
    const auto first_summary = first_word >> 6U;
    const auto last_summary = last_word >> 6U;
    for (auto summary_word = first_summary; summary_word <= last_summary;
         ++summary_word) {
      auto candidate_words =
          nonempty_words_[summary_word].load(std::memory_order_acquire) &
          range_mask(summary_word, first_word, last_word + 1U);
      while (candidate_words != 0U) {
        const auto word =
            (summary_word << 6U) +
            static_cast<std::size_t>(std::countr_zero(candidate_words));
        const auto candidates =
            words_[word].load(std::memory_order_acquire) &
            ~excluded.words_[word].load(std::memory_order_acquire) &
            range_mask(word, begin, end);
        if (candidates != 0U) {
          return (word << 6U) +
                 static_cast<std::size_t>(std::countr_zero(candidates));
        }
        candidate_words &= candidate_words - 1U;
      }
    }
    return end;
  }

  template <class Visitor>
  void VisitIntersectionRange(const AtomicWorkerBitmap &other,
                              std::size_t begin, std::size_t end,
                              Visitor &visitor) const noexcept {
    if (begin >= end) {
      return;
    }
    const auto first_word = begin >> 6U;
    const auto last_word = (end - 1U) >> 6U;
    const auto first_summary = first_word >> 6U;
    const auto last_summary = last_word >> 6U;
    for (auto summary_word = first_summary; summary_word <= last_summary;
         ++summary_word) {
      auto candidate_words =
          nonempty_words_[summary_word].load(std::memory_order_acquire) &
          other.nonempty_words_[summary_word].load(std::memory_order_acquire) &
          range_mask(summary_word, first_word, last_word + 1U);
      while (candidate_words != 0U) {
        const auto word =
            (summary_word << 6U) +
            static_cast<std::size_t>(std::countr_zero(candidate_words));
        auto candidates = words_[word].load(std::memory_order_acquire) &
                          other.words_[word].load(std::memory_order_acquire) &
                          range_mask(word, begin, end);
        while (candidates != 0U) {
          const auto relative =
              static_cast<std::size_t>(std::countr_zero(candidates));
          visitor((word << 6U) + relative);
          candidates &= candidates - 1U;
        }
        candidate_words &= candidate_words - 1U;
      }
    }
  }

  std::size_t word_count_ = 0;
  std::size_t summary_word_count_ = 0;
  std::unique_ptr<std::atomic<std::uint64_t>[]> words_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> nonempty_words_;
};

} // namespace sanitize::internal

// Deterministic mutation runner for compilers without libFuzzer.

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <stdexcept>
#include <system_error>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size);

namespace {

struct Options {
  std::size_t runs{1};
  std::size_t max_length{1U << 20U};
  std::uint64_t seed{0x5A17'2026ULL};
  std::vector<std::filesystem::path> corpus_paths;
};

[[nodiscard]] bool parse_unsigned(std::string_view text,
                                  std::uint64_t &value) noexcept {
  const auto *first = text.data();
  const auto *last = first + text.size();
  const auto parsed = std::from_chars(first, last, value);
  return parsed.ec == std::errc{} && parsed.ptr == last;
}

[[nodiscard]] Options parse_options(int argc, char **argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    const auto parse_option = [&](std::string_view prefix,
                                  std::uint64_t &target) -> bool {
      if (!argument.starts_with(prefix)) {
        return false;
      }
      if (!parse_unsigned(argument.substr(prefix.size()), target)) {
        throw std::invalid_argument("invalid numeric fuzzer option: " +
                                    std::string(argument));
      }
      return true;
    };
    std::uint64_t parsed = 0;
    if (parse_option("-runs=", parsed) || parse_option("--runs=", parsed)) {
      if (parsed == 0U || parsed > std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument("fuzzer runs must be a positive size_t");
      }
      options.runs = static_cast<std::size_t>(parsed);
    } else if (parse_option("-max_len=", parsed) ||
               parse_option("--max-len=", parsed)) {
      if (parsed == 0U || parsed > std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument("fuzzer max length must be a positive size_t");
      }
      options.max_length = static_cast<std::size_t>(parsed);
    } else if (parse_option("-seed=", parsed) ||
               parse_option("--seed=", parsed)) {
      options.seed = parsed;
    } else if (argument == "--help" || argument == "-help=1") {
      std::cout << "usage: fuzzer [-runs=N] [-seed=N] [-max_len=N] corpus...\n";
      std::exit(0);
    } else if (!argument.empty() && argument.front() == '-') {
      throw std::invalid_argument("unsupported standalone fuzzer option: " +
                                  std::string(argument));
    } else {
      options.corpus_paths.emplace_back(argument);
    }
  }
  return options;
}

[[nodiscard]] std::vector<std::uint8_t>
read_file(const std::filesystem::path &path, std::size_t max_length) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("cannot open fuzz corpus input: " + path.string());
  }
  std::vector<std::uint8_t> bytes;
  bytes.reserve(std::min<std::size_t>(max_length, 4096U));
  char value = 0;
  while (bytes.size() < max_length && input.get(value)) {
    bytes.push_back(static_cast<std::uint8_t>(
        static_cast<unsigned char>(value)));
  }
  return bytes;
}

[[nodiscard]] std::vector<std::vector<std::uint8_t>>
load_corpus(const Options &options) {
  std::vector<std::filesystem::path> files;
  for (const auto &path : options.corpus_paths) {
    std::error_code error;
    if (std::filesystem::is_regular_file(path, error)) {
      files.push_back(path);
    } else if (std::filesystem::is_directory(path, error)) {
      for (std::filesystem::recursive_directory_iterator iterator(path, error),
           end;
           !error && iterator != end; iterator.increment(error)) {
        if (iterator->is_regular_file(error)) {
          files.push_back(iterator->path());
        }
      }
      if (error) {
        throw std::runtime_error("cannot enumerate fuzz corpus: " +
                                 path.string());
      }
    } else {
      throw std::runtime_error("fuzz corpus path is not a regular file or directory: " +
                               path.string());
    }
  }
  std::sort(files.begin(), files.end());
  files.erase(std::unique(files.begin(), files.end()), files.end());

  std::vector<std::vector<std::uint8_t>> corpus;
  corpus.reserve(std::max<std::size_t>(1U, files.size()));
  for (const auto &path : files) {
    corpus.push_back(read_file(path, options.max_length));
  }
  if (corpus.empty()) {
    corpus.emplace_back();
  }
  return corpus;
}

[[nodiscard]] std::size_t random_index(std::mt19937_64 &random,
                                       std::size_t size) {
  if (size == 0U) {
    return 0U;
  }
  return std::uniform_int_distribution<std::size_t>(0U, size - 1U)(random);
}

void mutate(std::vector<std::uint8_t> &bytes,
            std::span<const std::vector<std::uint8_t>> corpus,
            std::size_t max_length, std::mt19937_64 &random) {
  const auto operation = std::uniform_int_distribution<unsigned>(0U, 7U)(random);
  const auto random_byte = [&]() {
    return static_cast<std::uint8_t>(
        std::uniform_int_distribution<unsigned>(0U, 255U)(random));
  };

  switch (operation) {
  case 0U:
    if (!bytes.empty()) {
      const auto offset = random_index(random, bytes.size());
      bytes[offset] ^= static_cast<std::uint8_t>(1U << (random() % 8U));
    }
    break;
  case 1U:
    if (!bytes.empty()) {
      bytes[random_index(random, bytes.size())] = random_byte();
    }
    break;
  case 2U:
    if (bytes.size() < max_length) {
      const auto offset = random_index(random, bytes.size() + 1U);
      bytes.insert(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                   random_byte());
    }
    break;
  case 3U:
    if (!bytes.empty()) {
      bytes.erase(bytes.begin() +
                  static_cast<std::ptrdiff_t>(random_index(random, bytes.size())));
    }
    break;
  case 4U:
    if (!bytes.empty() && bytes.size() < max_length) {
      const auto first = random_index(random, bytes.size());
      const auto available = bytes.size() - first;
      const auto length = std::min<std::size_t>(
          std::max<std::size_t>(1U, random_index(random, available + 1U)),
          max_length - bytes.size());
      const std::vector<std::uint8_t> copy(
          bytes.begin() + static_cast<std::ptrdiff_t>(first),
          bytes.begin() + static_cast<std::ptrdiff_t>(first + length));
      const auto destination = random_index(random, bytes.size() + 1U);
      bytes.insert(bytes.begin() + static_cast<std::ptrdiff_t>(destination),
                   copy.begin(), copy.end());
    }
    break;
  case 5U:
    if (!bytes.empty()) {
      bytes.resize(random_index(random, bytes.size() + 1U));
    }
    break;
  case 6U: {
    const auto &other = corpus[random_index(random, corpus.size())];
    if (!other.empty() && bytes.size() < max_length) {
      const auto source = random_index(random, other.size());
      const auto count = std::min<std::size_t>(other.size() - source,
                                                max_length - bytes.size());
      const auto destination = random_index(random, bytes.size() + 1U);
      bytes.insert(bytes.begin() + static_cast<std::ptrdiff_t>(destination),
                   other.begin() + static_cast<std::ptrdiff_t>(source),
                   other.begin() + static_cast<std::ptrdiff_t>(source + count));
    }
    break;
  }
  case 7U: {
    const auto target = std::min<std::size_t>(
        max_length, std::max<std::size_t>(1U, bytes.size() + 16U));
    const auto count = random_index(random, target + 1U);
    bytes.resize(count);
    std::generate(bytes.begin(), bytes.end(), random_byte);
    break;
  }
  default:
    break;
  }
}

} // namespace

int main(int argc, char **argv) {
  try {
    const auto options = parse_options(argc, argv);
    const auto corpus = load_corpus(options);
    std::mt19937_64 random(options.seed);
    for (std::size_t run = 0; run < options.runs; ++run) {
      std::vector<std::uint8_t> input = corpus[run % corpus.size()];
      if (run >= corpus.size()) {
        const auto mutation_count = 1U + static_cast<unsigned>(run % 8U);
        for (unsigned mutation = 0; mutation < mutation_count; ++mutation) {
          mutate(input, corpus, options.max_length, random);
        }
      }
      const std::uint8_t empty_input = 0;
      const auto *data = input.empty() ? &empty_input : input.data();
      (void)LLVMFuzzerTestOneInput(data, input.size());
    }
    std::cout << "standalone fuzz runs completed: " << options.runs << '\n';
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "standalone fuzzer configuration failed: " << error.what()
              << '\n';
    return 2;
  }
}

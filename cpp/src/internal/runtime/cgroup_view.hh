// Resolves files and effective limits relative to the cgroup hierarchy that
// actually constrains this process.
#pragma once

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string_view>

namespace sanitize::internal::cgroup_view_detail {

enum class ValueState : std::uint8_t { kValue, kUnbounded, kUnknown };

struct UnsignedSample final {
  ValueState state = ValueState::kUnknown;
  std::uint64_t value = 0U;
  // ENOENT is meaningful only for a controller file at the cgroup2 mount
  // root: that root is exempt from resource control and normally omits those
  // files. Keep it distinct from permission, I/O and parse failures so every
  // other failure remains UNKNOWN.
  bool missing = false;
};

#if defined(__linux__)

[[nodiscard]] inline bool line_is_complete(const char *line,
                                           std::FILE *file) noexcept {
  if (line == nullptr || file == nullptr) {
    return false;
  }
  if (std::strchr(line, '\n') != nullptr) {
    return true;
  }
  // A short final procfs/control-file record may omit the newline. If the
  // fixed buffer filled, feof() is not authoritative until another read, so
  // conservatively reject the record rather than parse a valid-looking prefix.
  return std::strlen(line) + 1U < 4096U && std::feof(file) != 0;
}

[[nodiscard]] inline bool
controller_list_contains(const char *list,
                         std::string_view controller) noexcept {
  if (list == nullptr) {
    return false;
  }
  const auto length = std::strlen(list);
  std::size_t begin = 0U;
  while (begin <= length) {
    auto end = begin;
    while (end < length && list[end] != ',') {
      ++end;
    }
    if (end - begin == controller.size() &&
        std::memcmp(list + begin, controller.data(), controller.size()) == 0) {
      return true;
    }
    begin = end + 1U;
  }
  return false;
}

[[nodiscard]] inline bool current_membership(std::string_view controller,
                                             char *path, std::size_t capacity,
                                             bool &unified) noexcept {
  if (path == nullptr || capacity < 2U) {
    return false;
  }
  std::FILE *file = std::fopen("/proc/self/cgroup", "r");
  if (file == nullptr) {
    return false;
  }
  char line[4096]{};
  bool found = false;
  while (std::fgets(line, sizeof(line), file) != nullptr) {
    if (!line_is_complete(line, file)) {
      (void)std::fclose(file);
      return false;
    }
    char *first = std::strchr(line, ':');
    if (first == nullptr) {
      continue;
    }
    char *second = std::strchr(first + 1, ':');
    if (second == nullptr) {
      continue;
    }
    *second = '\0';
    const char *controllers = first + 1;
    char *membership = second + 1;
    membership[std::strcspn(membership, "\r\n")] = '\0';
    const bool is_unified = *controllers == '\0';
    if (!is_unified && !controller_list_contains(controllers, controller)) {
      continue;
    }
    const auto written =
        std::snprintf(path, capacity, "%s", *membership ? membership : "/");
    if (written > 0 && static_cast<std::size_t>(written) < capacity) {
      unified = is_unified;
      found = true;
      break;
    }
  }
  (void)std::fclose(file);
  return found;
}

[[nodiscard]] inline int current_version(std::string_view controller) noexcept {
  char membership[4096]{};
  bool unified = false;
  if (!current_membership(controller, membership, sizeof(membership),
                          unified)) {
    return 0;
  }
  return unified ? 2 : 1;
}

inline void unescape_mount_field(char *value) noexcept {
  if (value == nullptr) {
    return;
  }
  char *read = value;
  char *write = value;
  while (*read != '\0') {
    if (*read == '\\' && read[1] >= '0' && read[1] <= '7' && read[2] >= '0' &&
        read[2] <= '7' && read[3] >= '0' && read[3] <= '7') {
      const auto decoded = static_cast<unsigned char>(
          ((read[1] - '0') << 6) | ((read[2] - '0') << 3) | (read[3] - '0'));
      *write++ = static_cast<char>(decoded);
      read += 4;
      continue;
    }
    *write++ = *read++;
  }
  *write = '\0';
}

[[nodiscard]] inline bool join_cgroup_directory(const char *mountpoint,
                                                const char *mount_root,
                                                const char *membership,
                                                char *output,
                                                std::size_t capacity) noexcept {
  if (!mountpoint || !mount_root || !membership || !output || capacity < 2U) {
    return false;
  }
  const char *relative = membership;
  const auto root_length = std::strlen(mount_root);
  if (std::strcmp(mount_root, "/") == 0) {
    while (*relative == '/') {
      ++relative;
    }
  } else if (std::strncmp(membership, mount_root, root_length) == 0 &&
             (membership[root_length] == '\0' ||
              membership[root_length] == '/')) {
    relative = membership + root_length;
    while (*relative == '/') {
      ++relative;
    }
  } else {
    // This mount does not contain the process membership subtree. Never
    // fabricate mountpoint + unrelated membership and accidentally read
    // another cgroup that happens to exist at that path.
    return false;
  }
  const char *separator = relative[0] == '\0' ? "" : "/";
  const auto written = std::snprintf(output, capacity, "%s%s%s", mountpoint,
                                     separator, relative);
  return written > 0 && static_cast<std::size_t>(written) < capacity;
}

[[nodiscard]] inline bool
join_cgroup_path(const char *mountpoint, const char *mount_root,
                 const char *membership, std::string_view filename,
                 char *output, std::size_t capacity) noexcept {
  char directory[4096]{};
  if (!join_cgroup_directory(mountpoint, mount_root, membership, directory,
                             sizeof(directory))) {
    return false;
  }
  const auto written =
      std::snprintf(output, capacity, "%s/%.*s", directory,
                    static_cast<int>(filename.size()), filename.data());
  return written > 0 && static_cast<std::size_t>(written) < capacity;
}

[[nodiscard]] inline bool
resolve_directory(std::string_view controller, char *output,
                  std::size_t capacity, char *mount_output,
                  std::size_t mount_capacity, char *membership_output = nullptr,
                  std::size_t membership_capacity = 0U,
                  bool *unified_output = nullptr,
                  bool *hierarchy_complete = nullptr) noexcept {
  char membership[4096]{};
  bool unified = false;
  if (!current_membership(controller, membership, sizeof(membership),
                          unified)) {
    return false;
  }
  std::FILE *mounts = std::fopen("/proc/self/mountinfo", "r");
  if (mounts == nullptr) {
    return false;
  }
  char line[8192]{};
  char selected_directory[4096]{};
  char selected_mountpoint[4096]{};
  bool found = false;
  bool selected_hierarchy_complete = false;
  while (std::fgets(line, sizeof(line), mounts) != nullptr) {
    // mountinfo lines are allowed to be large. A filled fixed buffer is not a
    // record; parsing it could select an unrelated prefix and must fail closed.
    if (std::strchr(line, '\n') == nullptr && std::feof(mounts) == 0) {
      (void)std::fclose(mounts);
      return false;
    }
    char *separator = std::strstr(line, " - ");
    if (separator == nullptr) {
      continue;
    }
    *separator = '\0';
    char filesystem[32]{};
    char source[256]{};
    char super_options[1024]{};
    if (std::sscanf(separator + 3, "%31s %255s %1023s", filesystem, source,
                    super_options) < 3) {
      continue;
    }
    if (unified) {
      if (std::strcmp(filesystem, "cgroup2") != 0) {
        continue;
      }
    } else if (std::strcmp(filesystem, "cgroup") != 0 ||
               !controller_list_contains(super_options, controller)) {
      continue;
    }
    char mount_root[4096]{};
    char mountpoint[4096]{};
    if (std::sscanf(line, "%*s %*s %*s %4095s %4095s", mount_root,
                    mountpoint) != 2) {
      continue;
    }
    unescape_mount_field(mount_root);
    unescape_mount_field(mountpoint);
    char candidate_directory[4096]{};
    if (!join_cgroup_directory(mountpoint, mount_root, membership,
                               candidate_directory,
                               sizeof(candidate_directory))) {
      continue;
    }
    const bool complete = std::strcmp(mount_root, "/") == 0;
    // Preserve the first usable subtree candidate, but always replace it with
    // a later complete root mount. This avoids UNKNOWN when both views exist.
    if (!found || (complete && !selected_hierarchy_complete)) {
      const auto dir_written =
          std::snprintf(selected_directory, sizeof(selected_directory), "%s",
                        candidate_directory);
      const auto mount_written = std::snprintf(
          selected_mountpoint, sizeof(selected_mountpoint), "%s", mountpoint);
      if (dir_written <= 0 || mount_written <= 0 ||
          static_cast<std::size_t>(dir_written) >= sizeof(selected_directory) ||
          static_cast<std::size_t>(mount_written) >=
              sizeof(selected_mountpoint)) {
        continue;
      }
      found = true;
      selected_hierarchy_complete = complete;
      if (complete) {
        break;
      }
    }
  }
  (void)std::fclose(mounts);
  if (!found) {
    return false;
  }
  const auto output_written =
      std::snprintf(output, capacity, "%s", selected_directory);
  const auto mount_written =
      std::snprintf(mount_output, mount_capacity, "%s", selected_mountpoint);
  if (output_written <= 0 || mount_written <= 0 ||
      static_cast<std::size_t>(output_written) >= capacity ||
      static_cast<std::size_t>(mount_written) >= mount_capacity) {
    return false;
  }
  char membership_after[4096]{};
  bool unified_after = false;
  if (!current_membership(controller, membership_after,
                          sizeof(membership_after), unified_after) ||
      unified_after != unified ||
      std::strcmp(membership_after, membership) != 0) {
    return false;
  }
  if (membership_output != nullptr) {
    if (membership_capacity < 2U) {
      return false;
    }
    const auto membership_written =
        std::snprintf(membership_output, membership_capacity, "%s", membership);
    if (membership_written <= 0 ||
        static_cast<std::size_t>(membership_written) >= membership_capacity) {
      return false;
    }
  }
  if (unified_output != nullptr) {
    *unified_output = unified;
  }
  if (hierarchy_complete != nullptr) {
    *hierarchy_complete = selected_hierarchy_complete;
  }
  return true;
}

[[nodiscard]] inline bool resolve_file(std::string_view controller,
                                       std::string_view filename, char *output,
                                       std::size_t capacity) noexcept {
  char directory[4096]{};
  char mountpoint[4096]{};
  if (!resolve_directory(controller, directory, sizeof(directory), mountpoint,
                         sizeof(mountpoint))) {
    return false;
  }
  const auto written =
      std::snprintf(output, capacity, "%s/%.*s", directory,
                    static_cast<int>(filename.size()), filename.data());
  return written > 0 && static_cast<std::size_t>(written) < capacity;
}

[[nodiscard]] inline UnsignedSample
read_unsigned_file(const char *path) noexcept {
  if (path == nullptr) {
    return {};
  }
  errno = 0;
  std::FILE *file = std::fopen(path, "r");
  if (file == nullptr) {
    return {ValueState::kUnknown, 0U, errno == ENOENT};
  }
  char raw[128]{};
  const bool read = std::fgets(raw, sizeof(raw), file) != nullptr;
  const bool complete =
      read && (std::strchr(raw, '\n') != nullptr ||
               (std::strlen(raw) + 1U < sizeof(raw) && std::feof(file) != 0));
  const bool input_error = std::ferror(file) != 0;
  const int close_status = std::fclose(file);
  if (!read || !complete || input_error || close_status != 0) {
    return {};
  }
  raw[std::strcspn(raw, "\r\n\t ")] = '\0';
  if (std::strcmp(raw, "max") == 0 || raw[0] == '-') {
    return {ValueState::kUnbounded, 0U};
  }
  if (raw[0] == '\0') {
    return {};
  }
  char *end = nullptr;
  errno = 0;
  const auto parsed = std::strtoull(raw, &end, 10);
  if (errno == ERANGE || end == raw || (end && *end != '\0')) {
    return {};
  }
  if (parsed >= (1ULL << 62U)) {
    return {ValueState::kUnbounded, 0U};
  }
  return {ValueState::kValue, static_cast<std::uint64_t>(parsed)};
}

inline bool parent_directory_in_place(char *current,
                                      const char *mountpoint) noexcept {
  if (!current || !mountpoint || std::strcmp(current, mountpoint) == 0) {
    return false;
  }
  const auto mount_length = std::strlen(mountpoint);
  const auto current_length = std::strlen(current);
  if (mount_length == 0U || current_length <= mount_length ||
      std::strncmp(current, mountpoint, mount_length) != 0) {
    return false;
  }
  char *slash = std::strrchr(current, '/');
  if (slash == nullptr) {
    return false;
  }
  // A cgroup2 mount may legally be rooted at '/'.  In that case the parent
  // of '/child' is '/' rather than the empty string.
  if (mount_length == 1U && mountpoint[0] == '/') {
    if (slash == current) {
      current[1] = '\0';
      return true;
    }
    *slash = '\0';
    return true;
  }
  if (slash < current + static_cast<std::ptrdiff_t>(mount_length)) {
    return false;
  }
  if (slash == current + static_cast<std::ptrdiff_t>(mount_length) &&
      mountpoint[mount_length - 1U] != '/') {
    *slash = '\0';
    return true;
  }
  *slash = '\0';
  return std::strlen(current) >= mount_length;
}

[[nodiscard]] inline UnsignedSample
effective_unsigned(std::string_view controller,
                   std::string_view filename) noexcept {
  for (int attempt = 0; attempt < 2; ++attempt) {
    char current[4096]{};
    char mountpoint[4096]{};
    char membership[4096]{};
    bool unified = false;
    bool hierarchy_complete = false;
    if (!resolve_directory(controller, current, sizeof(current), mountpoint,
                           sizeof(mountpoint), membership, sizeof(membership),
                           &unified, &hierarchy_complete)) {
      continue;
    }
    if (!hierarchy_complete) {
      return {};
    }
    bool saw_value = false;
    bool read_failed = false;
    std::uint64_t minimum = std::numeric_limits<std::uint64_t>::max();
    for (;;) {
      char path[4096]{};
      const auto written =
          std::snprintf(path, sizeof(path), "%s/%.*s", current,
                        static_cast<int>(filename.size()), filename.data());
      if (written <= 0 || static_cast<std::size_t>(written) >= sizeof(path)) {
        read_failed = true;
        break;
      }
      const auto sample = read_unsigned_file(path);
      if (sample.state == ValueState::kUnknown) {
        const bool at_mount_root = std::strcmp(current, mountpoint) == 0;
        if (unified && at_mount_root && sample.missing) {
          // The cgroup2 root is exempt from resource control and normally has
          // no controller interface files. Only its exact ENOENT is a known
          // unbounded level; all non-root and non-ENOENT failures stay closed.
          break;
        }
        read_failed = true;
        break;
      }
      if (sample.state == ValueState::kValue) {
        saw_value = true;
        minimum = std::min(minimum, sample.value);
      }
      if (std::strcmp(current, mountpoint) == 0) {
        break;
      }
      if (!parent_directory_in_place(current, mountpoint)) {
        read_failed = true;
        break;
      }
    }
    char membership_after[4096]{};
    bool unified_after = false;
    const bool stable =
        current_membership(controller, membership_after,
                           sizeof(membership_after), unified_after) &&
        unified_after == unified &&
        std::strcmp(membership_after, membership) == 0;
    if (!stable) {
      continue;
    }
    if (read_failed) {
      return {};
    }
    return saw_value ? UnsignedSample{ValueState::kValue, minimum}
                     : UnsignedSample{ValueState::kUnbounded, 0U};
  }
  return {};
}

[[nodiscard]] inline UnsignedSample
effective_headroom(std::string_view controller, std::string_view limit_filename,
                   std::string_view usage_filename) noexcept {
  for (int attempt = 0; attempt < 2; ++attempt) {
    char current[4096]{};
    char mountpoint[4096]{};
    char membership[4096]{};
    bool unified = false;
    bool hierarchy_complete = false;
    if (!resolve_directory(controller, current, sizeof(current), mountpoint,
                           sizeof(mountpoint), membership, sizeof(membership),
                           &unified, &hierarchy_complete)) {
      continue;
    }
    if (!hierarchy_complete) {
      return {};
    }
    bool saw_bounded = false;
    bool read_failed = false;
    std::uint64_t minimum = std::numeric_limits<std::uint64_t>::max();
    for (;;) {
      char limit_path[4096]{};
      char usage_path[4096]{};
      const auto limit_written = std::snprintf(
          limit_path, sizeof(limit_path), "%s/%.*s", current,
          static_cast<int>(limit_filename.size()), limit_filename.data());
      const auto usage_written = std::snprintf(
          usage_path, sizeof(usage_path), "%s/%.*s", current,
          static_cast<int>(usage_filename.size()), usage_filename.data());
      if (limit_written <= 0 || usage_written <= 0 ||
          static_cast<std::size_t>(limit_written) >= sizeof(limit_path) ||
          static_cast<std::size_t>(usage_written) >= sizeof(usage_path)) {
        read_failed = true;
        break;
      }
      const auto limit = read_unsigned_file(limit_path);
      if (limit.state == ValueState::kUnknown) {
        const bool at_mount_root = std::strcmp(current, mountpoint) == 0;
        if (unified && at_mount_root && limit.missing) {
          // See effective_unsigned(): a missing controller file is expected
          // only at the exempt cgroup2 root.
          break;
        }
        read_failed = true;
        break;
      }
      if (limit.state == ValueState::kValue) {
        const auto usage = read_unsigned_file(usage_path);
        if (usage.state != ValueState::kValue) {
          read_failed = true;
          break;
        }
        saw_bounded = true;
        const auto headroom =
            limit.value > usage.value ? limit.value - usage.value : 0U;
        minimum = std::min(minimum, headroom);
      }
      if (std::strcmp(current, mountpoint) == 0) {
        break;
      }
      if (!parent_directory_in_place(current, mountpoint)) {
        read_failed = true;
        break;
      }
    }
    char membership_after[4096]{};
    bool unified_after = false;
    const bool stable =
        current_membership(controller, membership_after,
                           sizeof(membership_after), unified_after) &&
        unified_after == unified &&
        std::strcmp(membership_after, membership) == 0;
    if (!stable) {
      continue;
    }
    if (read_failed) {
      return {};
    }
    return saw_bounded ? UnsignedSample{ValueState::kValue, minimum}
                       : UnsignedSample{ValueState::kUnbounded, 0U};
  }
  return {};
}

#else

[[nodiscard]] inline int current_version(std::string_view) noexcept {
  return 0;
}
[[nodiscard]] inline bool resolve_file(std::string_view, std::string_view,
                                       char *, std::size_t) noexcept {
  return false;
}
[[nodiscard]] inline UnsignedSample
effective_unsigned(std::string_view, std::string_view) noexcept {
  return {ValueState::kUnbounded, 0U};
}
[[nodiscard]] inline UnsignedSample
effective_headroom(std::string_view, std::string_view,
                   std::string_view) noexcept {
  return {ValueState::kUnbounded, 0U};
}

#endif

} // namespace sanitize::internal::cgroup_view_detail

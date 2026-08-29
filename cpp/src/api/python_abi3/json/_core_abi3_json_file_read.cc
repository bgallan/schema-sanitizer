// Implements the local-file byte reader used by Python ABI3 JSON utilities. The
// routines preserve JSON value semantics while enforcing bounded native
// ownership and Python errors.

#include "api/python_abi3/json/_core_abi3_json_tools.hh"
#include "internal/runtime/process_fd_governor.hh"

#include <array>
#include <fstream>
#include <memory>
#include <string>

namespace core_abi3_internal {

bool read_local_file_bytes(PyObject *path_obj, long long memory_limit_bytes,
                           std::string *raw) {
  if (!raw) {
    PyErr_SetString(PyExc_RuntimeError, "internal json compactor error");
    return false;
  }
  PyObject *encoded = fsencode_path(path_obj);
  if (!encoded) {
    return false;
  }
  std::unique_ptr<PyObject, decltype(&Py_DECREF)> encoded_owner(encoded,
                                                                Py_DECREF);
  char *path_data = nullptr;
  Py_ssize_t path_size = 0;
  if (PyBytes_AsStringAndSize(encoded, &path_data, &path_size) != 0) {
    return false;
  }

  sanitize::internal::ProcessFdPermitLease fd_lease(1U);
  if (!fd_lease) {
    PyErr_SetString(PyExc_OSError, "native file-descriptor capacity exhausted");
    return false;
  }
  std::ifstream file(
      std::string(path_data, static_cast<std::size_t>(path_size)),
      std::ios::binary);
  if (!file) {
    PyErr_SetFromErrnoWithFilenameObject(PyExc_OSError, path_obj);
    return false;
  }
  fd_lease.mark_opened();
  sanitize::internal::ProcessFdStreamCloseGuard<std::ifstream> close_guard(
      file, fd_lease);

  raw->clear();
  constexpr std::size_t kChunkBytes = 1024 * 1024;
  std::array<char, kChunkBytes> buffer{};
  while (file) {
    if (!check_python_signals()) {
      return false;
    }
    file.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const std::streamsize got = file.gcount();
    if (got <= 0) {
      break;
    }
    raw->append(buffer.data(), static_cast<std::size_t>(got));
    if (memory_limit_bytes > 0 &&
        raw->size() > static_cast<std::size_t>(memory_limit_bytes)) {
      PyErr_SetString(PyExc_MemoryError,
                      "memory_limit_bytes limit exceeded during json_parse");
      return false;
    }
  }
  if (file.bad()) {
    PyErr_SetFromErrnoWithFilenameObject(PyExc_OSError, path_obj);
    return false;
  }
  return true;
}

} // namespace core_abi3_internal

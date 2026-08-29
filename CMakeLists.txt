cmake_minimum_required(VERSION 4.3)

# The native concurrency runtime uses C++20 atomic wait/notify and over-aligned
# allocation. libc++ exposes the required runtime entry points from macOS 11, so
# binaries must not advertise an older deployment baseline.
if(CMAKE_HOST_APPLE AND (NOT CMAKE_OSX_DEPLOYMENT_TARGET
                         OR CMAKE_OSX_DEPLOYMENT_TARGET VERSION_LESS "11.0"))
  set(CMAKE_OSX_DEPLOYMENT_TARGET
      "11.0"
      CACHE STRING "Minimum supported macOS version" FORCE)
endif()

# Determine version from VERSION.
#
# NOTE: meta/VERSION is the single source of truth for Python packaging and
# supports manual CMake builds without parsing dynamic project metadata.
file(READ "${CMAKE_CURRENT_LIST_DIR}/meta/VERSION"
     _schema_sanitizer_version_file)
string(STRIP "${_schema_sanitizer_version_file}"
             SCHEMA_SANITIZER_PROJECT_VERSION)
if(NOT SCHEMA_SANITIZER_PROJECT_VERSION)
  message(
    FATAL_ERROR "Could not determine schema-sanitizer version from meta/VERSION"
  )
endif()

# CMake's project(VERSION ...) requires a numeric dotted version. Our Python
# packaging version may be PEP 440 (e.g. 0.1.post1), so derive a compatible
# value for CMake while keeping the Python version intact elsewhere.
set(SCHEMA_SANITIZER_CMAKE_VERSION "${SCHEMA_SANITIZER_PROJECT_VERSION}")
if(SCHEMA_SANITIZER_CMAKE_VERSION MATCHES
   "^([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.post([0-9]+)$")
  string(
    REGEX
    REPLACE "^([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.post([0-9]+)$" "\\1.\\2.\\3.\\4"
            SCHEMA_SANITIZER_CMAKE_VERSION "${SCHEMA_SANITIZER_CMAKE_VERSION}")
elseif(SCHEMA_SANITIZER_CMAKE_VERSION MATCHES
       "^([0-9]+)\\.([0-9]+)\\.post([0-9]+)$")
  string(
    REGEX
    REPLACE "^([0-9]+)\\.([0-9]+)\\.post([0-9]+)$" "\\1.\\2.0.\\3"
            SCHEMA_SANITIZER_CMAKE_VERSION "${SCHEMA_SANITIZER_CMAKE_VERSION}")
elseif(NOT SCHEMA_SANITIZER_CMAKE_VERSION MATCHES
       "^([0-9]+)\\.([0-9]+)(\\.([0-9]+))?$")
  message(
    FATAL_ERROR
      "Could not derive a CMake-compatible version from meta/VERSION: ${SCHEMA_SANITIZER_PROJECT_VERSION}"
  )
endif()

project(
  schema_sanitizer
  VERSION ${SCHEMA_SANITIZER_CMAKE_VERSION}
  DESCRIPTION
    "Spec-driven path and URI data sanitization for CSV, JSON, JSON arrays, JSON Lines, XML, and Parquet"
  HOMEPAGE_URL "https://github.com/bgallan/schema-sanitizer"
  LANGUAGES C CXX)

find_package(Threads REQUIRED)

option(SCHEMA_SANITIZER_ENABLE_LTO
       "Enable IPO/LTO in Release builds when supported" ON)
option(SCHEMA_SANITIZER_ENABLE_WARNINGS
       "Enable compiler warnings for schema-sanitizer targets" ON)
option(SCHEMA_SANITIZER_REPRODUCIBLE
       "Enable reproducible build flags (path mapping where supported)" ON)
set(SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES
    "4"
    CACHE STRING
          "Maximum concurrent MSVC compiler processes for project targets")
if(NOT SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES MATCHES "^[1-9][0-9]*$")
  message(
    FATAL_ERROR
      "SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES must be a positive integer (got '${SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES}')"
  )
endif()

# Build hygiene: warnings are errors by default in every build. Callers may
# still opt out explicitly through the CMake cache option.
option(SCHEMA_SANITIZER_ENABLE_WERROR
       "Treat compiler warnings as errors for schema-sanitizer targets" ON)

# Developer sanitizer mode: none|asan|asan-ubsan|tsan.
set(SCHEMA_SANITIZER_SANITIZER
    "none"
    CACHE STRING "Sanitizer mode: none|asan|asan-ubsan|tsan")
set_property(CACHE SCHEMA_SANITIZER_SANITIZER PROPERTY STRINGS none asan
                                                       asan-ubsan tsan)

option(SCHEMA_SANITIZER_ENABLE_CLANG_TIDY
       "Enable clang-tidy for schema-sanitizer targets" OFF)
set(SCHEMA_SANITIZER_CLANG_TIDY_CHECKS
    "clang-analyzer-*"
    CACHE STRING "clang-tidy checks used when clang-tidy is enabled")

# Optional (Linux): link libstdc++/libgcc statically for our shared libraries.
# This is OFF by default because manylinux images/toolchains may not ship static
# runtimes.
option(
  SCHEMA_SANITIZER_LINUX_STATIC_STDLIB
  "On Linux, link libstdc++/libgcc statically for schema-sanitizer shared libs"
  OFF)
option(SCHEMA_SANITIZER_REQUIRE_ZLIB
       "Require zlib for native Parquet GZIP support" OFF)
option(SCHEMA_SANITIZER_BUILD_FUZZERS "Build native parser fuzz entry points"
       OFF)
set(SCHEMA_SANITIZER_FUZZ_ENGINE
    "auto"
    CACHE STRING "Native fuzz engine: auto|libfuzzer|standalone")
set_property(CACHE SCHEMA_SANITIZER_FUZZ_ENGINE PROPERTY STRINGS auto libfuzzer
                                                         standalone)
option(SCHEMA_SANITIZER_ENABLE_COVERAGE
       "Build native targets with LLVM source coverage instrumentation" OFF)
set(SCHEMA_SANITIZER_COVERAGE_PROFILE_PATTERN
    "schema-sanitizer-%p.profraw"
    CACHE STRING
          "LLVM raw profile path pattern embedded in instrumented targets")

include("${CMAKE_CURRENT_LIST_DIR}/cmake/SchemaSanitizerTargetOptions.cmake")

# Developer ergonomics (clangd, IDEs).
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(SCHEMA_SANITIZER_ENABLE_LTO AND SCHEMA_SANITIZER_SANITIZER STREQUAL "none")
  include(CheckIPOSupported)
  check_ipo_supported(RESULT _ipo_ok OUTPUT _ipo_err)
  if(_ipo_ok)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO TRUE)
  else()
    message(STATUS "IPO/LTO not supported: ${_ipo_err}")
  endif()
elseif(SCHEMA_SANITIZER_ENABLE_LTO)
  message(STATUS "IPO/LTO disabled for sanitizer builds")
endif()

# ---- Arrow integration ------------------------------------------------------
# This project intentionally avoids Apache Arrow C++ headers/libs. We rely only
# on the Arrow C Data / C Stream ABIs through the vendored NanoArrow C header.

# ---- Optional compression ---------------------------------------------------
# GZIP is available through the same zlib API on every supported platform.
# Windows defaults to the pinned static provider; release wheels force that
# provider on every OS for bit-for-bit dependency parity.
include("${CMAKE_CURRENT_LIST_DIR}/cmake/SchemaSanitizerCompression.cmake")
schema_sanitizer_resolve_zlib(SCHEMA_SANITIZER_ZLIB_TARGET
                              SCHEMA_SANITIZER_ZLIB_PROVIDER_RESOLVED)

# Bundled compression is part of the native Parquet implementation. Sanitizer
# builds must instrument it too; otherwise GZIP paths cross into an opaque
# static library and the full-extension gate has an avoidable blind spot.
if(SCHEMA_SANITIZER_ZLIB_PROVIDER_RESOLVED MATCHES "^bundled-" AND TARGET
                                                                   zlibstatic)
  schema_sanitizer_add_sanitizer(zlibstatic)
endif()

# ---- Python discovery -------------------------------------------------------
# Python is required to build the ABI3 extension module.
find_package(Python3 REQUIRED COMPONENTS Interpreter Development.SABIModule)

add_custom_target(
  schema_sanitizer_check_cpp_documentation
  COMMAND
    ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_LIST_DIR}/meta/ci/native/check_cpp_documentation.py
    --build-root ${CMAKE_BINARY_DIR} --compile-commands
    ${CMAKE_BINARY_DIR}/compile_commands.json
  WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
  COMMENT "Checking C/C++ file and callable documentation with Clang ASTs"
  VERBATIM)

message(STATUS "Python executable: ${Python3_EXECUTABLE}")

# Minimal stable-ABI module (limited API).
#
# IMPORTANT: This module is intentionally named `_core_abi3`; the Python package
# loads it through the internal native loader.
set(_schema_sanitizer_pymod_target "_core_abi3")
python3_add_library(${_schema_sanitizer_pymod_target} MODULE USE_SABI 3.11
                    WITH_SOABI)

set_target_properties(
  ${_schema_sanitizer_pymod_target} PROPERTIES CXX_VISIBILITY_PRESET hidden
                                               VISIBILITY_INLINES_HIDDEN YES)

add_library(sanitize_core STATIC)
include("${CMAKE_CURRENT_LIST_DIR}/cmake/SchemaSanitizerSources.cmake")

# Source files intentionally use file-local anonymous namespaces with common
# helper names. Keep these targets out of unity builds even when a parent build
# enables CMAKE_UNITY_BUILD globally.
set_target_properties(sanitize_core ${_schema_sanitizer_pymod_target}
                      PROPERTIES UNITY_BUILD OFF)

foreach(_schema_sanitizer_target sanitize_core
                                 ${_schema_sanitizer_pymod_target})
  target_compile_features(${_schema_sanitizer_target} PRIVATE cxx_std_23)
  set_target_properties(
    ${_schema_sanitizer_target}
    PROPERTIES CXX_EXTENSIONS OFF
               CXX_SCAN_FOR_MODULES OFF
               POSITION_INDEPENDENT_CODE ON)
  schema_sanitizer_enable_warnings(${_schema_sanitizer_target})
  schema_sanitizer_enable_msvc_parallel_compile(${_schema_sanitizer_target})
  schema_sanitizer_enable_repro(${_schema_sanitizer_target})
  schema_sanitizer_add_sanitizer(${_schema_sanitizer_target})
  schema_sanitizer_add_coverage(${_schema_sanitizer_target})
  schema_sanitizer_enable_clang_tidy(${_schema_sanitizer_target})
  target_include_directories(${_schema_sanitizer_target} PRIVATE cpp/src
                                                                 cpp/thirdparty)
endforeach()

# Link
target_link_libraries(sanitize_core PUBLIC Threads::Threads)
target_link_libraries(${_schema_sanitizer_pymod_target} PRIVATE sanitize_core)

if(NOT SCHEMA_SANITIZER_SANITIZER STREQUAL "none")
  include(CTest)
  add_executable(
    schema_sanitizer_sanitized_ordered_executor
    cpp/tests/ordered_executor_tsan.cc
    cpp/src/internal/memory/memory_budget.cc
    cpp/src/internal/memory/memory_pool.cc
    cpp/src/internal/memory/pool_resource.cc
    cpp/src/internal/runtime/operation_task_arena.cc
    cpp/src/internal/runtime/performance_telemetry.cc
    cpp/src/internal/json_encoding/token_writer.cc)
  target_compile_features(schema_sanitizer_sanitized_ordered_executor
                          PRIVATE cxx_std_23)
  set_target_properties(
    schema_sanitizer_sanitized_ordered_executor
    PROPERTIES CXX_EXTENSIONS OFF
               CXX_SCAN_FOR_MODULES OFF
               INTERPROCEDURAL_OPTIMIZATION OFF)
  target_include_directories(schema_sanitizer_sanitized_ordered_executor
                             PRIVATE cpp/src)
  target_link_libraries(schema_sanitizer_sanitized_ordered_executor
                        PRIVATE Threads::Threads)
  schema_sanitizer_enable_warnings(schema_sanitizer_sanitized_ordered_executor)
  schema_sanitizer_enable_msvc_parallel_compile(
    schema_sanitizer_sanitized_ordered_executor)
  schema_sanitizer_enable_repro(schema_sanitizer_sanitized_ordered_executor)
  schema_sanitizer_add_sanitizer(schema_sanitizer_sanitized_ordered_executor)
  set(_schema_sanitizer_sanitized_executor_rounds 100)
  # Keep compiling the probe with MSVC ASan, but do not register it for
  # execution there. The Windows ASan runtime can stall indefinitely while
  # starting or tearing down this deeply threaded process, outside the probe's
  # own watchdog. Windows still executes every ASan fuzzer; this repeated
  # concurrency gate runs under macOS ASan/UBSan and Linux TSan.
  if(NOT (MSVC AND SCHEMA_SANITIZER_SANITIZER STREQUAL "asan"))
    add_test(NAME schema_sanitizer_sanitized_ordered_executor
             COMMAND schema_sanitizer_sanitized_ordered_executor --rounds
                     ${_schema_sanitizer_sanitized_executor_rounds})
  endif()
  if(SCHEMA_SANITIZER_SANITIZER STREQUAL "tsan")
    add_test(NAME schema_sanitizer_tsan_ordered_executor
             COMMAND schema_sanitizer_sanitized_ordered_executor --rounds
                     ${_schema_sanitizer_sanitized_executor_rounds})
  endif()
endif()

if(SCHEMA_SANITIZER_ZLIB_TARGET)
  target_compile_definitions(sanitize_core PRIVATE SCHEMA_SANITIZER_HAS_ZLIB=1)
  target_link_libraries(sanitize_core PUBLIC ${SCHEMA_SANITIZER_ZLIB_TARGET})
endif()

if(SCHEMA_SANITIZER_BUILD_FUZZERS)
  set(_schema_sanitizer_fuzz_engine "${SCHEMA_SANITIZER_FUZZ_ENGINE}")
  if(_schema_sanitizer_fuzz_engine STREQUAL "auto")
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
      set(_schema_sanitizer_fuzz_engine "libfuzzer")
    else()
      set(_schema_sanitizer_fuzz_engine "standalone")
    endif()
  endif()
  if(_schema_sanitizer_fuzz_engine STREQUAL "libfuzzer"
     AND NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "SCHEMA_SANITIZER_FUZZ_ENGINE=libfuzzer requires Clang")
  endif()
  if(NOT _schema_sanitizer_fuzz_engine MATCHES "^(libfuzzer|standalone)$")
    message(
      FATAL_ERROR
        "Invalid SCHEMA_SANITIZER_FUZZ_ENGINE value: '${SCHEMA_SANITIZER_FUZZ_ENGINE}'. Expected: auto, libfuzzer, or standalone"
    )
  endif()
  message(STATUS "Native fuzz engine: ${_schema_sanitizer_fuzz_engine}")
  schema_sanitizer_stage_msvc_asan_runtime("${CMAKE_BINARY_DIR}/fuzz")

  function(schema_sanitizer_add_fuzzer target source)
    set(_fuzz_sources ${source} cpp/fuzz/sanitizer_runtime_defaults.cc)
    if(_schema_sanitizer_fuzz_engine STREQUAL "standalone")
      list(APPEND _fuzz_sources cpp/fuzz/standalone_main.cc)
    endif()
    add_executable(${target} ${_fuzz_sources})
    target_compile_features(${target} PRIVATE cxx_std_23)
    set_target_properties(
      ${target}
      PROPERTIES CXX_EXTENSIONS OFF
                 CXX_SCAN_FOR_MODULES OFF
                 INTERPROCEDURAL_OPTIMIZATION OFF
                 RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/fuzz")
    foreach(_config DEBUG RELEASE RELWITHDEBINFO MINSIZEREL)
      set_target_properties(
        ${target} PROPERTIES "RUNTIME_OUTPUT_DIRECTORY_${_config}"
                             "${CMAKE_BINARY_DIR}/fuzz")
    endforeach()
    target_include_directories(${target} PRIVATE cpp/src cpp/thirdparty)
    target_link_libraries(${target} PRIVATE sanitize_core)
    if(WIN32 AND _schema_sanitizer_fuzz_engine STREQUAL "standalone")
      target_link_libraries(${target} PRIVATE Psapi)
    endif()
    schema_sanitizer_enable_warnings(${target})
    schema_sanitizer_enable_msvc_parallel_compile(${target})
    schema_sanitizer_enable_repro(${target})
    schema_sanitizer_add_sanitizer(${target})
    if(_schema_sanitizer_fuzz_engine STREQUAL "libfuzzer")
      target_compile_options(${target} PRIVATE -fsanitize=fuzzer)
      target_link_options(${target} PRIVATE -fsanitize=fuzzer)
    endif()
  endfunction()

  schema_sanitizer_add_fuzzer(schema_sanitizer_fuzz_json cpp/fuzz/fuzz_json.cc)
  schema_sanitizer_add_fuzzer(schema_sanitizer_fuzz_csv cpp/fuzz/fuzz_csv.cc)
  schema_sanitizer_add_fuzzer(schema_sanitizer_fuzz_xml cpp/fuzz/fuzz_xml.cc)
  schema_sanitizer_add_fuzzer(schema_sanitizer_fuzz_parquet
                              cpp/fuzz/fuzz_parquet.cc)
endif()

# Install extension into the schema_sanitizer package.
install(
  TARGETS ${_schema_sanitizer_pymod_target}
  LIBRARY DESTINATION schema_sanitizer
  RUNTIME DESTINATION schema_sanitizer
  ARCHIVE DESTINATION schema_sanitizer)

if(SCHEMA_SANITIZER_LINUX_STATIC_STDLIB
   AND UNIX
   AND NOT APPLE
   AND NOT MSVC)
  target_link_options(${_schema_sanitizer_pymod_target} PRIVATE
                      -static-libstdc++ -static-libgcc)
endif()

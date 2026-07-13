cmake_minimum_required(VERSION 4.3)

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
  LANGUAGES CXX)

option(SCHEMA_SANITIZER_ENABLE_LTO
       "Enable IPO/LTO in Release builds when supported" ON)
option(SCHEMA_SANITIZER_ENABLE_WARNINGS
       "Enable compiler warnings for schema-sanitizer targets" ON)
option(SCHEMA_SANITIZER_REPRODUCIBLE
       "Enable reproducible build flags (path mapping where supported)" ON)

# Build hygiene: treat warnings as errors for schema-sanitizer targets. Default
# is strict in CI, tolerant locally.
set(_SCHEMA_SANITIZER_WERROR_DEFAULT OFF)
if(DEFINED ENV{CI} OR DEFINED ENV{GITHUB_ACTIONS})
  set(_SCHEMA_SANITIZER_WERROR_DEFAULT ON)
endif()
option(SCHEMA_SANITIZER_ENABLE_WERROR
       "Treat compiler warnings as errors for schema-sanitizer targets"
       ${_SCHEMA_SANITIZER_WERROR_DEFAULT})

# Developer sanitizer mode: none|asan-ubsan.
set(SCHEMA_SANITIZER_SANITIZER
    "none"
    CACHE STRING "Sanitizer mode: none|asan-ubsan")
set_property(CACHE SCHEMA_SANITIZER_SANITIZER PROPERTY STRINGS none asan-ubsan)

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

include("${CMAKE_CURRENT_LIST_DIR}/cmake/SchemaSanitizerTargetOptions.cmake")

# Developer ergonomics (clangd, IDEs).
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(SCHEMA_SANITIZER_ENABLE_LTO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT _ipo_ok OUTPUT _ipo_err)
  if(_ipo_ok)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO TRUE)
  else()
    message(STATUS "IPO/LTO not supported: ${_ipo_err}")
  endif()
endif()

# ---- Arrow integration ------------------------------------------------------
# This project intentionally avoids Apache Arrow C++ headers/libs. We rely only
# on the Arrow C Data / C Stream ABIs through the vendored NanoArrow C header.

# ---- Optional compression ---------------------------------------------------
# Native Parquet output can use zlib for GZIP page compression when available.
# Keep this optional so source builds remain portable on minimal systems.
find_package(ZLIB QUIET)
if(SCHEMA_SANITIZER_REQUIRE_ZLIB AND NOT ZLIB_FOUND)
  message(
    FATAL_ERROR
      "SCHEMA_SANITIZER_REQUIRE_ZLIB=ON but zlib was not found; published wheels require native Parquet GZIP support"
  )
endif()

# ---- Python discovery -------------------------------------------------------
# Python is required to build the ABI3 extension module.
find_package(Python3 REQUIRED COMPONENTS Interpreter Development.SABIModule)

add_custom_target(
  schema_sanitizer_check_cpp_documentation
  COMMAND
    ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_LIST_DIR}/meta/ci/check_cpp_documentation.py
    --compile-commands ${CMAKE_BINARY_DIR}/compile_commands.json
  WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
  COMMENT "Checking C/C++ callable documentation with clang-doc"
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
  schema_sanitizer_enable_repro(${_schema_sanitizer_target})
  schema_sanitizer_add_sanitizer(${_schema_sanitizer_target})
  schema_sanitizer_enable_clang_tidy(${_schema_sanitizer_target})
  target_include_directories(${_schema_sanitizer_target} PRIVATE cpp/src
                                                                 cpp/thirdparty)
endforeach()

# Link
target_link_libraries(${_schema_sanitizer_pymod_target} PRIVATE sanitize_core)

if(ZLIB_FOUND)
  target_compile_definitions(sanitize_core PRIVATE SCHEMA_SANITIZER_HAS_ZLIB=1)
  target_link_libraries(sanitize_core PUBLIC ZLIB::ZLIB)
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

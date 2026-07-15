# Defines reusable target option helpers for schema-sanitizer.
#
# These functions keep warning, sanitizer, clang-tidy, and reproducibility
# settings consistent across native targets.

# Enables compiler warnings and optional Werror for a target.
function(schema_sanitizer_enable_warnings target)
  if(NOT SCHEMA_SANITIZER_ENABLE_WARNINGS)
    return()
  endif()

  # Conservative warning set; keep the build clean across toolchains.
  target_compile_options(
    ${target}
    PRIVATE
      $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic;-Wshadow>
      $<$<CXX_COMPILER_ID:MSVC>:/W4>)

  if(SCHEMA_SANITIZER_ENABLE_WERROR)
    target_compile_options(
      ${target} PRIVATE $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Werror>
                        $<$<CXX_COMPILER_ID:MSVC>:/WX>)
  endif()
endfunction()

# Adds the requested compiler/runtime sanitizer mode to a target.
function(schema_sanitizer_add_sanitizer target)
  if(SCHEMA_SANITIZER_SANITIZER STREQUAL "none")
    return()
  endif()

  if(NOT (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU|AppleClang"))
    message(
      FATAL_ERROR
        "SCHEMA_SANITIZER_SANITIZER is only supported for Clang/GCC/AppleClang (got ${CMAKE_CXX_COMPILER_ID})"
    )
  endif()

  if(SCHEMA_SANITIZER_SANITIZER STREQUAL "asan-ubsan")
    target_compile_options(${target} PRIVATE -fsanitize=address,undefined
                                             -fno-omit-frame-pointer
                                             -fno-sanitize-recover=all)
    target_link_options(${target} PRIVATE -fsanitize=address,undefined
                        -fno-omit-frame-pointer -fno-sanitize-recover=all)
  else()
    message(
      FATAL_ERROR
        "Invalid SCHEMA_SANITIZER_SANITIZER value: '${SCHEMA_SANITIZER_SANITIZER}'. Expected: none or asan-ubsan"
    )
  endif()
endfunction()

# Adds LLVM source coverage instrumentation to a target.
function(schema_sanitizer_add_coverage target)
  if(NOT SCHEMA_SANITIZER_ENABLE_COVERAGE)
    return()
  endif()

  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
    message(
      FATAL_ERROR
        "SCHEMA_SANITIZER_ENABLE_COVERAGE requires Clang/AppleClang (got ${CMAKE_CXX_COMPILER_ID})"
    )
  endif()

  target_compile_options(
    ${target}
    PRIVATE
      "-fprofile-instr-generate=${SCHEMA_SANITIZER_COVERAGE_PROFILE_PATTERN}"
      -fcoverage-mapping)
  get_target_property(_coverage_target_type ${target} TYPE)
  if(_coverage_target_type STREQUAL "SHARED_LIBRARY"
     OR _coverage_target_type STREQUAL "MODULE_LIBRARY"
     OR _coverage_target_type STREQUAL "EXECUTABLE")
    target_link_options(
      ${target}
      PRIVATE
        "-fprofile-instr-generate=${SCHEMA_SANITIZER_COVERAGE_PROFILE_PATTERN}")
  endif()
endfunction()

# Enables clang-tidy with the configured check set for a target.
function(schema_sanitizer_enable_clang_tidy target)
  if(NOT SCHEMA_SANITIZER_ENABLE_CLANG_TIDY)
    return()
  endif()
  find_program(SCHEMA_SANITIZER_CLANG_TIDY_EXE NAMES clang-tidy)
  if(NOT SCHEMA_SANITIZER_CLANG_TIDY_EXE)
    message(
      FATAL_ERROR
        "SCHEMA_SANITIZER_ENABLE_CLANG_TIDY requested but clang-tidy was not found"
    )
  endif()
  # Keep tidy strict for our code; headers from dependencies are not treated as
  # system.
  set_property(
    TARGET ${target}
    PROPERTY
      CXX_CLANG_TIDY
      "${SCHEMA_SANITIZER_CLANG_TIDY_EXE};--checks=${SCHEMA_SANITIZER_CLANG_TIDY_CHECKS};--warnings-as-errors=*"
  )
endfunction()

# Adds reproducible path mapping flags for supported toolchains.
function(schema_sanitizer_enable_repro target)
  if(NOT SCHEMA_SANITIZER_REPRODUCIBLE)
    return()
  endif()

  get_target_property(_tgt_type ${target} TYPE)
  set(_can_link_opts 0)
  if(_tgt_type STREQUAL "SHARED_LIBRARY"
     OR _tgt_type STREQUAL "MODULE_LIBRARY"
     OR _tgt_type STREQUAL "EXECUTABLE")
    set(_can_link_opts 1)
  endif()

  # Map absolute source paths to '.' in debug info and __FILE__ expansions where
  # supported.
  set(_root "${PROJECT_SOURCE_DIR}")
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU|AppleClang")
    target_compile_options(${target} PRIVATE "-ffile-prefix-map=${_root}=."
                                             "-fmacro-prefix-map=${_root}=.")
    # Do not pass -Wl,-no_uuid on Apple: modern macOS loaders may reject
    # extension modules missing LC_UUID (observed in CI on macOS x86_64).
  elseif(MSVC)
    # Best-effort: supported on modern MSVC toolsets.
    target_compile_options(${target} PRIVATE "/experimental:deterministic"
                                             "/pathmap:${_root}=." "/Brepro")
    if(_can_link_opts)
      target_link_options(${target} PRIVATE "/Brepro")
    endif()
  endif()
endfunction()

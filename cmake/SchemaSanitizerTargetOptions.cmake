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
      # C4324 reports the intentional tail padding created by cache-line
      # alignment. Keep /W4 and /WX for every actionable diagnostic.
      $<$<CXX_COMPILER_ID:MSVC>:/W4>
      $<$<CXX_COMPILER_ID:MSVC>:/wd4324>)

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

  if(SCHEMA_SANITIZER_SANITIZER STREQUAL "asan")
    if(MSVC)
      target_compile_options(${target} PRIVATE /fsanitize=address)
      get_target_property(_sanitizer_target_type ${target} TYPE)
      if(_sanitizer_target_type STREQUAL "SHARED_LIBRARY"
         OR _sanitizer_target_type STREQUAL "MODULE_LIBRARY"
         OR _sanitizer_target_type STREQUAL "EXECUTABLE")
        target_link_options(${target} PRIVATE /INCREMENTAL:NO)
      endif()
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU|AppleClang")
      target_compile_options(${target} PRIVATE -fsanitize=address
                                               -fno-omit-frame-pointer)
      target_link_options(${target} PRIVATE -fsanitize=address
                          -fno-omit-frame-pointer)
    else()
      message(
        FATAL_ERROR
          "SCHEMA_SANITIZER_SANITIZER=asan is unsupported for ${CMAKE_CXX_COMPILER_ID}"
      )
    endif()
  elseif(SCHEMA_SANITIZER_SANITIZER STREQUAL "asan-ubsan")
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU|AppleClang")
      message(
        FATAL_ERROR
          "SCHEMA_SANITIZER_SANITIZER=asan-ubsan requires Clang/GCC/AppleClang (got ${CMAKE_CXX_COMPILER_ID})"
      )
    endif()
    target_compile_options(
      ${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer
                        -fno-sanitize-recover=all)
    target_link_options(${target} PRIVATE -fsanitize=address,undefined
                        -fno-omit-frame-pointer -fno-sanitize-recover=all)
  elseif(SCHEMA_SANITIZER_SANITIZER STREQUAL "tsan")
    if(APPLE
       OR MSVC
       OR NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
      message(
        FATAL_ERROR
          "SCHEMA_SANITIZER_SANITIZER=tsan is currently supported only on Linux with Clang or GCC"
      )
    endif()
    target_compile_options(${target} PRIVATE -fsanitize=thread
                                             -fno-omit-frame-pointer)
    target_link_options(${target} PRIVATE -fsanitize=thread
                        -fno-omit-frame-pointer)
  else()
    message(
      FATAL_ERROR
        "Invalid SCHEMA_SANITIZER_SANITIZER value: '${SCHEMA_SANITIZER_SANITIZER}'. Expected: none, asan, asan-ubsan, or tsan"
    )
  endif()
endfunction()

# Copies the MSVC AddressSanitizer runtime beside standalone executables.
#
# Current MSVC toolsets use clang_rt.asan_dynamic-{arch}.dll for every CRT
# linkage mode. The DLL is installed next to cl.exe, not in a system search
# directory, so command-line executables otherwise fail at startup with
# STATUS_DLL_NOT_FOUND (0xc0000135).
function(schema_sanitizer_stage_msvc_asan_runtime destination)
  if(NOT (MSVC AND SCHEMA_SANITIZER_SANITIZER STREQUAL "asan"))
    return()
  endif()

  if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(_schema_sanitizer_asan_arch "x86_64")
  elseif(CMAKE_SIZEOF_VOID_P EQUAL 4)
    set(_schema_sanitizer_asan_arch "i386")
  else()
    message(
      FATAL_ERROR
        "MSVC AddressSanitizer requires a 32-bit or 64-bit target architecture")
  endif()

  get_filename_component(_schema_sanitizer_compiler_dir "${CMAKE_CXX_COMPILER}"
                         DIRECTORY)
  set(_schema_sanitizer_asan_runtime_name
      "clang_rt.asan_dynamic-${_schema_sanitizer_asan_arch}.dll")
  set(_schema_sanitizer_asan_runtime
      "${_schema_sanitizer_compiler_dir}/${_schema_sanitizer_asan_runtime_name}"
  )
  if(NOT EXISTS "${_schema_sanitizer_asan_runtime}")
    message(
      FATAL_ERROR
        "MSVC AddressSanitizer runtime not found beside the selected compiler: ${_schema_sanitizer_asan_runtime}"
    )
  endif()

  file(MAKE_DIRECTORY "${destination}")
  configure_file(
    "${_schema_sanitizer_asan_runtime}"
    "${destination}/${_schema_sanitizer_asan_runtime_name}" COPYONLY)
  message(
    STATUS
      "Staged MSVC AddressSanitizer runtime: ${destination}/${_schema_sanitizer_asan_runtime_name}"
  )
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
      ${target} PRIVATE
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

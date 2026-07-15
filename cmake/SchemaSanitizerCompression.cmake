# Resolves the zlib backend used by native Parquet GZIP support.
#
# Windows wheels default to a pinned static zlib build because Visual Studio
# runners do not provide a system zlib. Linux/macOS source builds prefer the
# system package and use the same pinned fallback when it is unavailable.

set(_SCHEMA_SANITIZER_ZLIB_PROVIDER_DEFAULT "auto")
if(WIN32)
  set(_SCHEMA_SANITIZER_ZLIB_PROVIDER_DEFAULT "bundled")
endif()
set(SCHEMA_SANITIZER_ZLIB_PROVIDER
    "${_SCHEMA_SANITIZER_ZLIB_PROVIDER_DEFAULT}"
    CACHE STRING "zlib provider for Parquet GZIP: auto|system|bundled|disabled")
set_property(CACHE SCHEMA_SANITIZER_ZLIB_PROVIDER PROPERTY STRINGS auto system
                                                           bundled disabled)

function(schema_sanitizer_resolve_zlib out_target out_provider)
  string(TOLOWER "${SCHEMA_SANITIZER_ZLIB_PROVIDER}" _provider)
  if(NOT _provider MATCHES "^(auto|system|bundled|disabled)$")
    message(
      FATAL_ERROR
        "Invalid SCHEMA_SANITIZER_ZLIB_PROVIDER='${SCHEMA_SANITIZER_ZLIB_PROVIDER}'. Expected auto, system, bundled, or disabled"
    )
  endif()

  set(_target "")
  set(_resolved_provider "disabled")

  if(_provider STREQUAL "auto" OR _provider STREQUAL "system")
    find_package(ZLIB QUIET)
    if(TARGET ZLIB::ZLIB)
      set(_target ZLIB::ZLIB)
      set(_resolved_provider "system")
    elseif(TARGET ZLIB::ZLIBSTATIC)
      set(_target ZLIB::ZLIBSTATIC)
      set(_resolved_provider "system-static")
    elseif(_provider STREQUAL "system")
      message(
        FATAL_ERROR "System zlib requested but no CMake zlib target was found")
    endif()
  endif()

  if(NOT _target AND (_provider STREQUAL "auto" OR _provider STREQUAL "bundled"
                     ))
    include(FetchContent)

    # zlib 1.3.2 renamed its CMake options. Disable every auxiliary target and
    # installation rule so wheel builds only compile the static compression
    # library that is linked into the Python extension.
    set(ZLIB_BUILD_TESTING
        OFF
        CACHE BOOL "" FORCE)
    set(ZLIB_BUILD_SHARED
        OFF
        CACHE BOOL "" FORCE)
    set(ZLIB_BUILD_STATIC
        ON
        CACHE BOOL "" FORCE)
    set(ZLIB_INSTALL
        OFF
        CACHE BOOL "" FORCE)

    FetchContent_Declare(
      schema_sanitizer_zlib
      URL https://github.com/madler/zlib/releases/download/v1.3.2/zlib132.zip
      URL_HASH
        SHA256=e8bf55f3017aa181690990cb58a994e77885da140609fc8f94abe9b65d2cae28
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE EXCLUDE_FROM_ALL)
    FetchContent_MakeAvailable(schema_sanitizer_zlib)

    if(TARGET zlibstatic)
      set_property(TARGET zlibstatic PROPERTY POSITION_INDEPENDENT_CODE ON)
    endif()
    if(TARGET ZLIB::ZLIBSTATIC)
      set(_target ZLIB::ZLIBSTATIC)
    elseif(TARGET zlibstatic)
      set(_target zlibstatic)
    else()
      message(FATAL_ERROR "Bundled zlib did not define its static CMake target")
    endif()
    set(_resolved_provider "bundled-1.3.2-static")
  endif()

  if(SCHEMA_SANITIZER_REQUIRE_ZLIB AND NOT _target)
    message(
      FATAL_ERROR
        "SCHEMA_SANITIZER_REQUIRE_ZLIB=ON but the selected zlib provider did not produce a usable target"
    )
  endif()

  if(_target)
    message(
      STATUS "Native Parquet GZIP backend: ${_resolved_provider} (${_target})")
  else()
    message(STATUS "Native Parquet GZIP backend: disabled")
  endif()

  set(${out_target}
      "${_target}"
      PARENT_SCOPE)
  set(${out_provider}
      "${_resolved_provider}"
      PARENT_SCOPE)
endfunction()

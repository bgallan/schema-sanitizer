# Derives native target ownership from the source-tree boundary.
#
# Python extension translation units live below cpp/src/api and belong to the
# ABI3 module. Every other production translation unit belongs to the core
# library. CONFIGURE_DEPENDS makes adding or removing a source trigger a CMake
# reconfigure instead of requiring a second, manually synchronized manifest.

set(_schema_sanitizer_source_root "${CMAKE_CURRENT_LIST_DIR}/../cpp/src")
file(
  GLOB_RECURSE _schema_sanitizer_native_sources CONFIGURE_DEPENDS
  LIST_DIRECTORIES false
  "${_schema_sanitizer_source_root}/*.c"
  "${_schema_sanitizer_source_root}/*.cc"
  "${_schema_sanitizer_source_root}/*.cpp"
  "${_schema_sanitizer_source_root}/*.cxx")

if(NOT _schema_sanitizer_native_sources)
  message(FATAL_ERROR "No native production sources found below cpp/src")
endif()

set(_schema_sanitizer_module_sources)
set(_schema_sanitizer_core_sources)
foreach(_schema_sanitizer_source IN LISTS _schema_sanitizer_native_sources)
  file(RELATIVE_PATH _schema_sanitizer_relative_source
       "${_schema_sanitizer_source_root}" "${_schema_sanitizer_source}")
  if(_schema_sanitizer_relative_source MATCHES "^api/")
    list(APPEND _schema_sanitizer_module_sources "${_schema_sanitizer_source}")
  else()
    list(APPEND _schema_sanitizer_core_sources "${_schema_sanitizer_source}")
  endif()
endforeach()

# Prove that the directory rule is an exact partition. This catches future
# changes to the derivation before an unowned or multiply-owned source reaches a
# platform build.
set(_schema_sanitizer_owned_sources ${_schema_sanitizer_module_sources}
                                    ${_schema_sanitizer_core_sources})
list(LENGTH _schema_sanitizer_native_sources _schema_sanitizer_source_count)
list(LENGTH _schema_sanitizer_owned_sources _schema_sanitizer_owned_count)
list(REMOVE_DUPLICATES _schema_sanitizer_owned_sources)
list(LENGTH _schema_sanitizer_owned_sources
     _schema_sanitizer_unique_owned_count)
if(NOT _schema_sanitizer_owned_count EQUAL _schema_sanitizer_source_count
   OR NOT _schema_sanitizer_unique_owned_count EQUAL
      _schema_sanitizer_source_count)
  message(
    FATAL_ERROR
      "Native source ownership is not an exact cpp/src partition: discovered=${_schema_sanitizer_source_count}, owned=${_schema_sanitizer_owned_count}, unique=${_schema_sanitizer_unique_owned_count}"
  )
endif()

list(SORT _schema_sanitizer_module_sources)
list(SORT _schema_sanitizer_core_sources)
target_sources(${_schema_sanitizer_pymod_target}
               PRIVATE ${_schema_sanitizer_module_sources})
target_sources(sanitize_core PRIVATE ${_schema_sanitizer_core_sources})

# Keep the non-translation-unit projection contract visible in IDE targets.
target_sources(
  sanitize_core
  PRIVATE "${_schema_sanitizer_source_root}/frontends/csv/source_projection.hh")

unset(_schema_sanitizer_source_root)
unset(_schema_sanitizer_native_sources)
unset(_schema_sanitizer_module_sources)
unset(_schema_sanitizer_core_sources)
unset(_schema_sanitizer_owned_sources)
unset(_schema_sanitizer_source)
unset(_schema_sanitizer_relative_source)
unset(_schema_sanitizer_source_count)
unset(_schema_sanitizer_owned_count)
unset(_schema_sanitizer_unique_owned_count)

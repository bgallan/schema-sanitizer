// Native registry plan ownership, construction, and Python capsule API.

#pragma once

#include <memory>
#include <string>
#include <string_view>

#ifndef Py_LIMITED_API
#define Py_LIMITED_API 0x030A0000
#endif
#include <Python.h>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"
#include "sanitize/schema_registry/schema_registry.hh"

namespace core_abi3_internal {

struct NativeRegistryPlan {
  sanitize::LogicalSchema schema;
  std::shared_ptr<const sanitize::CompiledPlan> plan;
  std::string registry_json;
  std::string drifts_json;
  std::string conversion_timestamp;
};

sanitize::Result<std::shared_ptr<NativeRegistryPlan>>
make_native_registry_plan(sanitize::SchemaRegistryMergeResult merged);

sanitize::Result<std::shared_ptr<NativeRegistryPlan>>
make_native_registry_plan_with_generated_source_file(
    const NativeRegistryPlan &base);

sanitize::Result<std::shared_ptr<NativeRegistryPlan>>
make_native_registry_plan_from_json(
    const sanitize::PreparedOptionsPtr &prepared,
    std::string_view registry_json, std::string_view drifts_json,
    std::string_view conversion_timestamp);

PyObject *
wrap_native_registry_state(std::shared_ptr<const NativeRegistryPlan> plan);

std::shared_ptr<const NativeRegistryPlan>
native_registry_state_from_py(PyObject *obj);

} // namespace core_abi3_internal

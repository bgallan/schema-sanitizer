/*
 * C bridge context metadata/stats APIs.
 *
 * This file implements context memory and operation performance stats.
 */
#include "internal/abi/schema_sanitizer_c_internal.hh"

#include <cstdint>
#include <exception>
#include <new>
#include <string>

#include "internal/abi/schema_sanitizer_c_bridge.hh"
#include "internal/json_encoding/token_writer.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/runtime/performance_telemetry.hh"
#include "sanitize/runtime/execution_context.hh"

int schema_sanitizer_context_memory_stats_json(schema_sanitizer_context *ctx,
                                               char **out_json,
                                               char **out_error) {
  static constexpr const char *kWhere =
      "schema_sanitizer_context_memory_stats_json";
  clear_out(out_error);
  if (out_json)
    *out_json = nullptr;
  if (!out_json) {
    return set_error(out_error, std::string(kWhere) + ": out_json is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  int rc = ctx_check(ctx, kWhere, out_error);
  if (rc != SCHEMA_SANITIZER_STATUS_OK)
    return rc;
  try {
    auto *pool = static_cast<sanitize::internal::MemoryPool *>(
        ctx->ctx->memory_pool_handle());
    std::string json;
    json.reserve(256);
    json.push_back('{');
    bool first = true;
    sanitize::internal::json_encoding::append_string_field(
        json, first, "backend_name", pool ? pool->backend_name() : "");
    sanitize::internal::json_encoding::append_int_field(
        json, first, "bytes_allocated",
        pool ? pool->bytes_allocated() : int64_t{0});
    sanitize::internal::json_encoding::append_int_field(
        json, first, "max_memory", pool ? pool->max_memory() : int64_t{0});
    sanitize::internal::json_encoding::append_int_field(
        json, first, "allocation_count",
        pool ? pool->allocation_count() : int64_t{0});
    sanitize::internal::json_encoding::append_int_field(
        json, first, "invalid_free_count",
        pool ? pool->invalid_free_count() : int64_t{0});
    sanitize::internal::json_encoding::append_int_field(
        json, first, "size_mismatch_count",
        pool ? pool->size_mismatch_count() : int64_t{0});
    sanitize::internal::json_encoding::append_int_field(
        json, first, "corruption_count",
        pool ? pool->corruption_count() : int64_t{0});
    sanitize::internal::json_encoding::append_int_field(
        json, first, "limit_bytes", pool ? pool->limit_bytes() : int64_t{-1});
    json.push_back('}');

    *out_json = dup_cstr(json);
    if (!*out_json) {
      return set_oom_error(out_error, kWhere);
    }
    return SCHEMA_SANITIZER_STATUS_OK;
  } catch (const std::bad_alloc &) {
    return set_oom_error(out_error, kWhere);
  } catch (const std::exception &e) {
    return set_exception_error(out_error, kWhere, e);
  } catch (...) {
    return set_unknown_exception_error(out_error, kWhere);
  }
}

int schema_sanitizer_context_performance_stats_json(
    schema_sanitizer_context *ctx, char **out_json, char **out_error) {
  static constexpr const char *kWhere =
      "schema_sanitizer_context_performance_stats_json";
  clear_out(out_error);
  if (out_json)
    *out_json = nullptr;
  if (!out_json) {
    return set_error(out_error, std::string(kWhere) + ": out_json is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  int rc = ctx_check(ctx, kWhere, out_error);
  if (rc != SCHEMA_SANITIZER_STATUS_OK)
    return rc;
  try {
    const auto telemetry = ctx->ctx->performance_telemetry();
    const std::string json = telemetry ? telemetry->ToJson() : "{}";
    *out_json = dup_cstr(json);
    if (!*out_json) {
      return set_oom_error(out_error, kWhere);
    }
    return SCHEMA_SANITIZER_STATUS_OK;
  } catch (const std::bad_alloc &) {
    return set_oom_error(out_error, kWhere);
  } catch (const std::exception &e) {
    return set_exception_error(out_error, kWhere, e);
  } catch (...) {
    return set_unknown_exception_error(out_error, kWhere);
  }
}

/*
 * C bridge context core APIs.
 *
 * This file implements context creation, affinity checks, and context
 * destruction.
 */
#include "internal/abi/schema_sanitizer_c_internal.hh"

#include <exception>
#include <memory>
#include <new>
#include <string>

#include "internal/abi/schema_sanitizer_c_bridge.hh"
#include "sanitize/runtime/execution_context.hh"

int schema_sanitizer_context_new(schema_sanitizer_context **out_ctx,
                                 char **out_error) {
  static constexpr const char *kWhere = "schema_sanitizer_context_new";
  clear_out(out_error);
  if (!out_ctx) {
    return set_error(out_error, std::string(kWhere) + ": out_ctx is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  *out_ctx = nullptr;
  try {
    auto h = std::make_unique<schema_sanitizer_context>();
    h->ctx = std::make_shared<sanitize::ExecutionContext>();
    *out_ctx = h.release();
    return SCHEMA_SANITIZER_STATUS_OK;
  } catch (const std::bad_alloc &) {
    return set_oom_error(out_error, kWhere);
  } catch (const std::exception &e) {
    return set_exception_error(out_error, kWhere, e);
  } catch (...) {
    return set_unknown_exception_error(out_error, kWhere);
  }
}
int ctx_check(schema_sanitizer_context *ctx, const char *where,
              char **out_error) {
  if (!ctx || !ctx->ctx) {
    return set_error(out_error, std::string(where) + ": ctx is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  return SCHEMA_SANITIZER_STATUS_OK;
}
void schema_sanitizer_context_free(schema_sanitizer_context *ctx) {
  delete ctx;
}

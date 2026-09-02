// Declares on-demand JSON document and nested value views.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#pragma once

#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string_view>

#include "internal/parsing/json/ondemand/scan.hh"
#include "sanitize/core/value_view.hh"

namespace sanitize::internal {

/// Validates and skips one JSON value, returning the first following byte
/// index.
sanitize::Result<std::size_t> json_skip_value(std::string_view text,
                                              std::size_t start,
                                              std::size_t base_offset = 0);

// DOM-less JSON parser for building ValueView without allocating a full tree.
//
// Design goals:
// - Memory safety: validate syntax, bounds-check every parse step.
// - Low allocations: avoid per-scalar nodes; allocate only decoded strings and
//   lightweight wrappers for nested objects/arrays.
// - Zero-copy when possible: unescaped strings are string_views into the input
// slice.
//
// Lifetime: JsonOnDemandDoc should be owned per-batch, and must outlive any
// ValueView produced from it (since nested wrappers and decoded strings live in
// its arena).
class JsonOnDemandDoc {
public:
  struct FlatValue {
    enum class Kind : std::uint8_t {
      kNull = 0,
      kBool,
      kInt,
      kFloat,
      kString,
      kEmptyObject,
      kEmptyArray,
      kNestedObject,
      kNestedArray,
    };

    Kind kind = Kind::kNull;
    std::string_view string_value;
  };

  using FlatObjectEachFn = sanitize::Status (*)(void *, std::string_view,
                                                FlatValue);

  /// Initializes arena-backed JSON parsing with a required upstream memory
  /// resource.
  explicit JsonOnDemandDoc(std::pmr::memory_resource *upstream);

  /// Releases decoded strings and wrapper allocations between row parses.
  void Reset() noexcept { arena_.release(); }

  // Lightweight wrappers allocated in this doc's arena.
  // These must be visible to the vtable callbacks implemented in the .cc.
  struct OdObject;
  struct OdArray;

  /// Parses one JSON value into inline scalars or arena-backed container
  /// wrappers.
  sanitize::Result<ValueView> ParseValue(std::string_view text,
                                         std::size_t base_offset = 0);

  /// Parses a JSON object and emits each field key, hash, and value to a
  /// callback.
  sanitize::Status ForEachObjectFieldC(std::string_view text, void *ctx,
                                       ValueView::ObjectEachFn fn,
                                       std::size_t base_offset = 0);

  /// Validates a flat root object and emits lexically classified fields without
  /// hashing keys.
  sanitize::Status ForEachFlatObjectFieldC(std::string_view text, void *ctx,
                                           FlatObjectEachFn fn,
                                           std::size_t base_offset = 0);

  /// Parses a JSON array and emits each element to a callback.
  sanitize::Status ForEachArrayElementC(std::string_view text, void *ctx,
                                        ValueView::ArrayEachFn fn,
                                        std::size_t base_offset = 0);

  /// Iterates an arena-backed object wrapper through the public object visitor.
  sanitize::Status ForEachObjectFieldImpl(const OdObject *obj, void *ctx,
                                          ValueView::ObjectEachFn fn) const;
  /// Iterates an arena-backed array wrapper through the public array visitor.
  sanitize::Status ForEachArrayElementImpl(const OdArray *arr, void *ctx,
                                           ValueView::ArrayEachFn fn) const;

private:
  /// Allocates aligned storage from the document arena.
  void *ArenaAlloc(std::size_t n, std::size_t align);
  /// Allocates storage for decoded characters in the document arena.
  char *ArenaAllocChars(std::size_t n);

  /// Creates a parse error at the supplied absolute JSON offset.
  static sanitize::Status ParseError(std::string_view msg, std::size_t offset);
  /// Creates a JSON cursor over a source slice.
  static json_scan::Cursor MakeCursor(std::string_view text,
                                      std::size_t base_offset) noexcept;
  /// Verifies that a top-level JSON value consumed the whole slice.
  static sanitize::Status ExpectEnd(json_scan::Cursor &cursor);
  /// Parses one JSON literal and returns the supplied typed value.
  static sanitize::Result<ValueView>
  ParseLiteralValue(json_scan::Cursor &cursor, const char *literal,
                    std::size_t literal_size, ValueView value);
  /// Parses one JSON string token and decodes escapes when needed.
  sanitize::Result<ValueView> ParseStringValue(json_scan::Cursor &cursor,
                                               std::string_view text,
                                               std::size_t base_offset);
  /// Parses one JSON object token into an on-demand object wrapper.
  sanitize::Result<ValueView> ParseObjectValue(json_scan::Cursor &cursor,
                                               std::string_view text,
                                               std::size_t base_offset);
  /// Parses one JSON array token into an on-demand array wrapper.
  sanitize::Result<ValueView> ParseArrayValue(json_scan::Cursor &cursor,
                                              std::string_view text,
                                              std::size_t base_offset);
  /// Parses one JSON number token into an integer or floating scalar.
  sanitize::Result<ValueView> ParseNumberValue(json_scan::Cursor &cursor,
                                               std::string_view text,
                                               std::size_t base_offset);
  /// Parses one JSON object key token.
  sanitize::Result<std::string_view> ParseObjectKey(json_scan::Cursor &cursor,
                                                    std::string_view text,
                                                    std::size_t base_offset);
  /// Parses the current object field or array element value slice.
  sanitize::Result<ValueView> ParseChildValue(json_scan::Cursor &cursor,
                                              std::string_view text,
                                              std::size_t base_offset);
  /// Parses and emits the current object field to a caller callback.
  sanitize::Status EmitObjectField(json_scan::Cursor &cursor,
                                   std::string_view text, void *ctx,
                                   ValueView::ObjectEachFn fn,
                                   std::size_t base_offset);
  /// Parses one scalar or validated nested value from a flat-object cursor.
  sanitize::Result<FlatValue> ParseFlatChildValue(json_scan::Cursor &cursor,
                                                  std::string_view text,
                                                  std::size_t base_offset);
  /// Parses one flat object field and emits its key and classified value.
  sanitize::Status EmitFlatObjectField(json_scan::Cursor &cursor,
                                       std::string_view text, void *ctx,
                                       FlatObjectEachFn fn,
                                       std::size_t base_offset);
  /// Enters a JSON object iterator and reports whether the object is empty.
  static sanitize::Status EnterObjectIterator(json_scan::Cursor &cursor,
                                              bool *done);
  /// Enters a JSON array iterator and reports whether the array is empty.
  static sanitize::Status EnterArrayIterator(json_scan::Cursor &cursor,
                                             bool *done);
  /// Advances an object iterator past ',' or '}'.
  static sanitize::Status AdvanceObjectIterator(json_scan::Cursor &cursor,
                                                bool *done);
  /// Advances an array iterator past ',' or ']'.
  static sanitize::Status AdvanceArrayIterator(json_scan::Cursor &cursor,
                                               bool *done);

  std::pmr::monotonic_buffer_resource arena_;
};

} // namespace sanitize::internal

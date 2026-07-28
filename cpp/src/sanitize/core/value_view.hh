// Defines non-owning scalar, object, and array value views.

#pragma once

#include <cstdint>
#include <string_view>

#include "sanitize/core/status.hh"

namespace sanitize {

class ValueView {
public:
  enum class Tag : uint8_t {
    kNull = 0,
    kBool,
    kInt,
    kFloat,
    kString,
    kObject,
    kArray,
  };

  using ArrayEachFn = Status (*)(void *ctx, ValueView el);
  using ObjectEachFn = Status (*)(void *ctx, std::string_view key,
                                  uint64_t key_hash, ValueView value);

  struct ArrayVTable {
    Status (*for_each)(const void *self, void *ctx, ArrayEachFn fn);
  };

  struct ObjectVTable {
    Status (*for_each)(const void *self, void *ctx, ObjectEachFn fn);
  };

  // Creates a null value view.
  static ValueView Null() { return {}; }
  // Creates a boolean value view.
  static ValueView Bool(bool v) { return ValueView(v); }
  // Creates an integer value view.
  static ValueView Int(int64_t v) { return ValueView(v); }
  // Creates a floating-point value view.
  static ValueView Float(double v) { return ValueView(v); }
  // Creates a string value view.
  static ValueView String(std::string_view v) { return ValueView(v); }

  // Creates an object value view backed by a custom vtable.
  static ValueView ObjectView(const void *self, const ObjectVTable *vt) {
    return {Tag::kObject, self, vt};
  }
  // Creates an array value view backed by a custom vtable.
  static ValueView ArrayView(const void *self, const ArrayVTable *vt) {
    return {Tag::kArray, self, vt};
  }

  // Returns the compact value category without exposing backing storage.
  [[nodiscard]] Tag tag() const noexcept { return tag_; }
  // Returns whether the value is null.
  [[nodiscard]] bool is_null() const { return tag_ == Tag::kNull; }
  // Returns whether the value is boolean.
  [[nodiscard]] bool is_bool() const { return tag_ == Tag::kBool; }
  // Returns whether the value is an integer.
  [[nodiscard]] bool is_int() const { return tag_ == Tag::kInt; }
  // Returns whether the value is floating point.
  [[nodiscard]] bool is_float() const { return tag_ == Tag::kFloat; }
  // Returns whether the value is a string.
  [[nodiscard]] bool is_string() const { return tag_ == Tag::kString; }
  // Returns whether the value is an object.
  [[nodiscard]] bool is_object() const { return tag_ == Tag::kObject; }
  // Returns whether the value is an array.
  [[nodiscard]] bool is_array() const { return tag_ == Tag::kArray; }

  // Returns the value as a boolean.
  [[nodiscard]] bool as_bool() const;
  // Returns the value as an integer.
  [[nodiscard]] int64_t as_int() const;
  // Returns the value as floating point.
  [[nodiscard]] double as_float() const;
  // Returns the value as a string view.
  [[nodiscard]] std::string_view as_string_view() const;
  // Reports whether an object or array has no children.
  Status container_is_empty(bool *out) const;

  // Visits each object field.
  template <class Fn> Status for_each_object_field(Fn &&fn) const {
    if (tag_ != Tag::kObject || !ptr_ || !obj_vt_) {
      return Status::Invalid("ValueView is not an object");
    }
    struct Ctx {
      Fn *fn;
    } ctx{&fn};
    auto cb = [](void *raw, std::string_view k, uint64_t h,
                 ValueView v) -> Status {
      return (*static_cast<Ctx *>(raw)->fn)(k, h, v);
    };
    return obj_vt_->for_each(ptr_, &ctx, cb);
  }

  // Visits each array element.
  template <class Fn> Status for_each_array_element(Fn &&fn) const {
    if (tag_ != Tag::kArray || !ptr_ || !arr_vt_) {
      return Status::Invalid("ValueView is not an array");
    }
    struct Ctx {
      Fn *fn;
    } ctx{&fn};
    auto cb = [](void *raw, ValueView el) -> Status {
      return (*static_cast<Ctx *>(raw)->fn)(el);
    };
    return arr_vt_->for_each(ptr_, &ctx, cb);
  }

private:
  Tag tag_ = Tag::kNull;

  union {
    bool b_;
    int64_t i_;
    double d_;
  };

  std::string_view s_;

  const void *ptr_ = nullptr;
  const ObjectVTable *obj_vt_ = nullptr;
  const ArrayVTable *arr_vt_ = nullptr;

  // Creates a null value view.
  ValueView() : i_(0) {}
  // Creates a boolean value view.
  explicit ValueView(bool v) : tag_(Tag::kBool), b_(v) {}
  // Creates an integer value view.
  explicit ValueView(int64_t v) : tag_(Tag::kInt), i_(v) {}
  // Creates a floating-point value view.
  explicit ValueView(double v) : tag_(Tag::kFloat), d_(v) {}
  // Creates a string value view.
  explicit ValueView(std::string_view v) : tag_(Tag::kString), i_(0), s_(v) {}

  // Creates an object or array value view backed by an erased vtable.
  ValueView(Tag tag, const void *self, const void *vt)
      : tag_(tag), i_(0), ptr_(self) {
    if (tag == Tag::kObject) {
      obj_vt_ = static_cast<const ObjectVTable *>(vt);
    } else if (tag == Tag::kArray) {
      arr_vt_ = static_cast<const ArrayVTable *>(vt);
    }
  }
};

} // namespace sanitize

// Declares nested Arrow direct ValueView vtables.

#pragma once

#include "sanitize/core/value_view.hh"

namespace core_abi3_internal {

// Returns the struct/object iterator vtable for Arrow direct values.
const sanitize::ValueView::ObjectVTable &arrow_direct_object_vtable();

// Returns the list/map iterator vtable for Arrow direct values.
const sanitize::ValueView::ArrayVTable &arrow_direct_array_vtable();

} // namespace core_abi3_internal

// Writes a primitive schema element into the compact footer.
void write_primitive_schema_element(CompactWriter &writer,
                                    const ParquetNode &node) {
  writer.FieldI32(
      1, static_cast<std::int32_t>(std::to_underlying(node.physical_type)));
  if (node.physical_type == PhysicalType::kFixedLenByteArray) {
    writer.FieldI32(2, node.fixed_binary_byte_width);
  }
  writer.FieldI32(3, node.required ? 0 : 1);
  writer.FieldString(4, node.name);
  if (node.has_converted_type) {
    writer.FieldI32(
        6, static_cast<std::int32_t>(std::to_underlying(node.converted_type)));
  }
  if (node.has_decimal_metadata) {
    writer.FieldI32(7, node.decimal_scale);
    writer.FieldI32(8, node.decimal_precision);
  }
  if (node.has_null_logical_type) {
    writer.FieldStruct(10, [&](CompactWriter &logical) {
      logical.FieldStruct(11, [](CompactWriter &) {});
    });
  } else if (node.has_timestamp_logical_type) {
    writer.FieldStruct(10, [&](CompactWriter &logical) {
      logical.FieldStruct(8, [&](CompactWriter &timestamp) {
        timestamp.FieldBool(1, false);
        timestamp.FieldStruct(2, [&](CompactWriter &unit) {
          const std::int16_t unit_field =
              node.arrow_kind == jsonl::JsonlKind::kTimestampMillis   ? 1
              : node.arrow_kind == jsonl::JsonlKind::kTimestampMicros ? 2
                                                                      : 3;
          unit.FieldStruct(unit_field, [](CompactWriter &) {});
        });
      });
    });
  } else if (node.has_time_millis_logical_type) {
    writer.FieldStruct(10, [&](CompactWriter &logical) {
      logical.FieldStruct(7, [&](CompactWriter &time) {
        time.FieldBool(1, false);
        time.FieldStruct(2, [&](CompactWriter &unit) {
          unit.FieldStruct(1, [](CompactWriter &) {});
        });
      });
    });
  } else if (node.has_int_logical_type) {
    writer.FieldStruct(10, [&](CompactWriter &logical) {
      logical.FieldStruct(10, [&](CompactWriter &integer) {
        integer.FieldByte(1, node.int_bit_width);
        integer.FieldBool(2, node.int_is_signed);
      });
    });
  }
}
// Writes one schema element and all descendants in preorder.
void write_schema_elements(CompactWriter &, std::string &out,
                           const ParquetNode &node) {
  CompactWriter item(out);
  if (node.node_kind == NodeKind::kPrimitive) {
    write_primitive_schema_element(item, node);
    item.Stop();
    return;
  }
  item.FieldString(4, node.name);
  if (node.node_kind != NodeKind::kRoot) {
    item.FieldI32(3, node.required ? 0 : 1);
  }
  if (node.node_kind == NodeKind::kList) {
    item.FieldI32(5, 1);
    item.FieldI32(
        6, static_cast<std::int32_t>(std::to_underlying(ConvertedType::kList)));
  } else if (node.node_kind == NodeKind::kMap) {
    item.FieldI32(5, 1);
    item.FieldI32(
        6, static_cast<std::int32_t>(std::to_underlying(ConvertedType::kMap)));
  } else if (node.node_kind == NodeKind::kStruct ||
             node.node_kind == NodeKind::kRoot) {
    item.FieldI32(5, static_cast<std::int32_t>(node.children.size()));
  }
  item.Stop();
  if (node.node_kind == NodeKind::kList) {
    CompactWriter repeated(out);
    repeated.FieldI32(3, 2); // REPEATED
    repeated.FieldString(4, "list");
    repeated.FieldI32(5, 1);
    repeated.Stop();
    write_schema_elements(repeated, out, *node.element);
    return;
  }
  if (node.node_kind == NodeKind::kMap) {
    CompactWriter repeated(out);
    repeated.FieldI32(3, 2); // REPEATED
    repeated.FieldString(4, "key_value");
    repeated.FieldI32(5,
                      static_cast<std::int32_t>(node.element->children.size()));
    repeated.Stop();
    for (const auto &child : node.element->children) {
      write_schema_elements(repeated, out, child);
    }
    return;
  }
  for (const auto &child : node.children) {
    write_schema_elements(item, out, child);
  }
}

// Encodes the Parquet file footer for all written row groups.
std::string encode_footer(const ParquetNode &root,
                          const std::vector<LeafColumn> &columns,
                          const std::vector<RowGroupInfo> &row_groups,
                          std::int64_t total_rows, CompressionCodec codec) {
  std::string out;
  CompactWriter writer(out);
  writer.FieldI32(1, 1);
  writer.FieldListStruct(
      2, root.subtree_schema_element_count,
      [&](CompactWriter &w) { write_schema_elements(w, out, root); });
  writer.FieldI64(3, total_rows);
  writer.FieldListStruct(4, row_groups.size(), [&](CompactWriter &) {
    for (const auto &row_group : row_groups) {
      CompactWriter rg(out);
      rg.FieldListStruct(1, row_group.columns.size(), [&](CompactWriter &) {
        for (std::size_t i = 0; i < row_group.columns.size(); ++i) {
          const auto &chunk_info = row_group.columns[i];
          const auto &page = chunk_info.aggregate;
          const auto total_uncompressed_size =
              page.dictionary_header_size +
              page.dictionary_uncompressed_payload_size + page.header_size +
              page.uncompressed_payload_size;
          const auto total_compressed_size =
              page.dictionary_header_size +
              page.dictionary_compressed_payload_size + page.header_size +
              page.compressed_payload_size;
          CompactWriter chunk(out);
          chunk.FieldI64(2, page.first_page_offset);
          chunk.FieldStruct(3, [&](CompactWriter &meta) {
            meta.FieldI32(
                1, static_cast<std::int32_t>(columns[i].node->physical_type));
            std::vector<std::int32_t> encodings;
            if (page.has_plain_encoding || page.dictionary_encoded) {
              encodings.push_back(kEncodingPlain);
            }
            encodings.push_back(3);
            if (page.dictionary_encoded) {
              encodings.push_back(kEncodingRleDictionary);
            }
            if (page.has_delta_binary_packed_encoding) {
              encodings.push_back(kEncodingDeltaBinaryPacked);
            }
            if (page.has_delta_length_byte_array_encoding) {
              encodings.push_back(kEncodingDeltaLengthByteArray);
            }
            if (page.has_byte_stream_split_encoding) {
              encodings.push_back(kEncodingByteStreamSplit);
            }
            meta.FieldListI32(2, encodings);
            meta.FieldListString(3, columns[i].path);
            meta.FieldI32(4, static_cast<std::int32_t>(codec));
            meta.FieldI64(5, page.num_values);
            meta.FieldI64(6, total_uncompressed_size);
            meta.FieldI64(7, total_compressed_size);
            meta.FieldI64(9, page.offset);
            if (page.dictionary_page_offset >= 0) {
              meta.FieldI64(11, page.dictionary_page_offset);
            }
            meta.FieldStruct(12, [&](CompactWriter &statistics) {
              if (page.max_value && page.min_value) {
                statistics.FieldString(1, *page.max_value);
                statistics.FieldString(2, *page.min_value);
              }
              statistics.FieldI64(3, page.null_count);
              if (page.max_value && page.min_value) {
                statistics.FieldString(5, *page.max_value);
                statistics.FieldString(6, *page.min_value);
                statistics.FieldBool(7, true);
                statistics.FieldBool(8, true);
              }
            });
          });
          if (chunk_info.offset_index_offset >= 0) {
            chunk.FieldI64(4, chunk_info.offset_index_offset);
            chunk.FieldI32(5, chunk_info.offset_index_length);
          }
          if (chunk_info.column_index_offset >= 0) {
            chunk.FieldI64(6, chunk_info.column_index_offset);
            chunk.FieldI32(7, chunk_info.column_index_length);
          }
          chunk.Stop();
        }
      });
      std::int64_t row_group_size = 0;
      for (const auto &chunk_info : row_group.columns) {
        const auto &page = chunk_info.aggregate;
        row_group_size += page.dictionary_header_size +
                          page.dictionary_uncompressed_payload_size +
                          page.header_size + page.uncompressed_payload_size;
      }
      rg.FieldI64(2, row_group_size);
      rg.FieldI64(3, row_group.num_rows);
      rg.Stop();
    }
  });
  writer.FieldString(6, "schema-sanitizer native parquet writer");
  writer.FieldListStruct(7, columns.size(), [&](CompactWriter &) {
    for (std::size_t i = 0; i < columns.size(); ++i) {
      CompactWriter order(out);
      order.FieldStruct(1, [](CompactWriter &) {});
      order.Stop();
    }
  });
  writer.Stop();
  return out;
}

#include <gtest/gtest.h>

#include <config.h>

#if USE_ARROW

#include <Columns/ColumnObject.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeObject.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromOwnString.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetVariantToJSON.h>

#include <arrow/api.h>
#include <arrow/util/key_value_metadata.h>

#include <string>
#include <vector>

using namespace DB;

namespace
{

/// Build Variant metadata for dictionary keys (1-byte offsets, version 1).
std::string makeMetadata(const std::vector<std::string> & keys)
{
    std::string bytes;
    for (const auto & k : keys)
        bytes += k;

    std::string meta;
    meta.push_back(0x01); // version=1, sorted=0, offset_size=1
    meta.push_back(static_cast<char>(keys.size()));
    UInt8 offset = 0;
    meta.push_back(static_cast<char>(offset));
    for (const auto & k : keys)
    {
        offset = static_cast<UInt8>(offset + k.size());
        meta.push_back(static_cast<char>(offset));
    }
    meta += bytes;
    return meta;
}

/// Primitive int8 value.
std::string makeInt8(Int8 v)
{
    std::string out;
    out.push_back(static_cast<char>((3 << 2) | 0)); // primitive int8
    out.push_back(static_cast<char>(v));
    return out;
}

/// Short string value (length < 64).
std::string makeShortString(std::string_view s)
{
    EXPECT_LT(s.size(), 64u);
    std::string out;
    out.push_back(static_cast<char>((static_cast<UInt8>(s.size()) << 2) | 1));
    out.append(s);
    return out;
}

/// Object with 1-byte field ids/offsets: keys referenced by field_ids in order.
std::string makeObject(const std::vector<UInt8> & field_ids, const std::vector<std::string> & values)
{
    EXPECT_EQ(field_ids.size(), values.size());
    std::string fields;
    std::vector<UInt8> offsets;
    offsets.push_back(0);
    for (const auto & v : values)
    {
        fields += v;
        offsets.push_back(static_cast<UInt8>(fields.size()));
    }

    std::string out;
    out.push_back(static_cast<char>((0 << 4 | 0 << 2 | 0) << 2 | 2)); // object, sizes=1
    out.push_back(static_cast<char>(field_ids.size()));
    for (UInt8 id : field_ids)
        out.push_back(static_cast<char>(id));
    for (UInt8 o : offsets)
        out.push_back(static_cast<char>(o));
    out += fields;
    return out;
}

std::string makeArray(const std::vector<std::string> & values)
{
    std::string fields;
    std::vector<UInt8> offsets;
    offsets.push_back(0);
    for (const auto & v : values)
    {
        fields += v;
        offsets.push_back(static_cast<UInt8>(fields.size()));
    }

    std::string out;
    out.push_back(static_cast<char>((0 << 2 | 0) << 2 | 3)); // array, sizes=1
    out.push_back(static_cast<char>(values.size()));
    for (UInt8 o : offsets)
        out.push_back(static_cast<char>(o));
    out += fields;
    return out;
}

}

TEST(ParquetVariantToJSON, PrimitiveAndObject)
{
    EXPECT_EQ(decodeParquetVariantToJSON(makeMetadata({}), makeInt8(42)), "42");
    EXPECT_EQ(decodeParquetVariantToJSON(makeMetadata({}), makeShortString("hi")), "\"hi\"");

    /// {"a":1,"b":"x"} — field ids sorted by key name: a=0, b=1
    const auto meta = makeMetadata({"a", "b"});
    const auto value = makeObject({0, 1}, {makeInt8(1), makeShortString("x")});
    EXPECT_EQ(decodeParquetVariantToJSON(meta, value), R"({"a":1,"b":"x"})");

    const auto arr = makeArray({makeInt8(1), makeInt8(2), makeInt8(3)});
    EXPECT_EQ(decodeParquetVariantToJSON(makeMetadata({}), arr), "[1,2,3]");
}

TEST(ParquetVariantToJSON, NestedObject)
{
    /// {"user":{"name":"alice"},"age":30}
    const auto meta = makeMetadata({"age", "name", "user"});
    const auto inner = makeObject({1}, {makeShortString("alice")}); // name
    const auto outer = makeObject({0, 2}, {makeInt8(30), inner}); // age, user (lex order)
    EXPECT_EQ(decodeParquetVariantToJSON(meta, outer), R"({"age":30,"user":{"name":"alice"}})");
}

TEST(ParquetVariantToJSON, ArrowColumnImport)
{
    const auto meta = makeMetadata({"a", "b"});
    const auto value = makeObject({0, 1}, {makeInt8(1), makeShortString("x")});

    arrow::BinaryBuilder meta_builder;
    arrow::BinaryBuilder value_builder;
    ASSERT_TRUE(meta_builder.Append(meta).ok());
    ASSERT_TRUE(value_builder.Append(value).ok());
    std::shared_ptr<arrow::Array> meta_array;
    std::shared_ptr<arrow::Array> value_array;
    ASSERT_TRUE(meta_builder.Finish(&meta_array).ok());
    ASSERT_TRUE(value_builder.Finish(&value_array).ok());

    auto metadata_field = arrow::field("metadata", arrow::binary(), /*nullable=*/false);
    auto value_field = arrow::field("value", arrow::binary(), /*nullable=*/false);
    auto storage_type = arrow::struct_({metadata_field, value_field});
    auto ext_metadata = arrow::key_value_metadata({"ARROW:extension:name"}, {"arrow.parquet.variant"});
    auto variant_field = arrow::field("v", storage_type, /*nullable=*/true, ext_metadata);

    auto struct_array = std::make_shared<arrow::StructArray>(
        storage_type, /*length=*/1, arrow::ArrayVector{meta_array, value_array});
    auto chunked = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{struct_array});
    auto schema = arrow::schema({variant_field});
    auto table = arrow::Table::Make(schema, {chunked});

    FormatSettings format_settings;
    format_settings.arrow.enable_json_parsing = true;

    Block header;
    header.insert({std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON), "v"});

    ArrowColumnToCHColumn converter(
        header,
        "Arrow",
        format_settings,
        /*parquet_columns_to_clickhouse*/ std::nullopt,
        /*clickhouse_columns_to_parquet*/ std::nullopt,
        /*allow_missing_columns*/ false,
        /*null_as_default*/ false,
        FormatSettings::DateTimeOverflowBehavior::Ignore,
        /*allow_geoparquet_parser*/ false,
        /*case_insensitive_matching*/ false,
        /*is_stream*/ false,
        /*enable_json_parsing*/ true);

    auto chunk = converter.arrowTableToCHChunk(table, table->num_rows(), schema->metadata());
    ASSERT_EQ(chunk.getNumRows(), 1u);
    ASSERT_EQ(chunk.getNumColumns(), 1u);

    const auto & col = chunk.getColumns()[0];
    const auto * object = typeid_cast<const ColumnObject *>(col.get());
    ASSERT_NE(object, nullptr);

    WriteBufferFromOwnString buf;
    auto type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
    type->getDefaultSerialization()->serializeTextJSON(*object, 0, buf, format_settings);
    EXPECT_EQ(buf.str(), R"({"a":1,"b":"x"})");
}

TEST(ParquetVariantToJSON, SchemaInference)
{
    auto metadata_field = arrow::field("metadata", arrow::binary(), /*nullable=*/false);
    auto value_field = arrow::field("value", arrow::binary(), /*nullable=*/false);
    auto storage_type = arrow::struct_({metadata_field, value_field});
    auto ext_metadata = arrow::key_value_metadata({"ARROW:extension:name"}, {"parquet.variant"});
    auto variant_field = arrow::field("v", storage_type, /*nullable=*/false, ext_metadata);
    auto schema = arrow::schema({variant_field});

    FormatSettings format_settings;
    format_settings.arrow.enable_json_parsing = true;

    Block header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *schema,
        schema->metadata(),
        "Arrow",
        format_settings,
        /*skip_columns_with_unsupported_types*/ false,
        /*allow_inferring_nullable_columns*/ true,
        /*case_insensitive_matching*/ false,
        /*allow_geoparquet_parser*/ false,
        /*enable_json_parsing*/ true);

    ASSERT_EQ(header.columns(), 1u);
    EXPECT_EQ(header.getByPosition(0).type->getName(), "JSON");
}

#endif

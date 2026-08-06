#pragma once

#include <base/types.h>
#include <string>
#include <string_view>

namespace DB
{

/// Decode an unshredded Apache Parquet/Spark Variant value (metadata + value binaries)
/// into a JSON text string suitable for ClickHouse JSON/`Object` columns.
///
/// Spec: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
/// Arrow extension name: `arrow.parquet.variant`
///
/// Throws INCORRECT_DATA on malformed input. Unsupported primitive types are emitted
/// as JSON null so the rest of the document can still be consumed.
String decodeParquetVariantToJSON(std::string_view metadata, std::string_view value);

/// True when Arrow field metadata (or ExtensionType name) identifies a Spark/Parquet Variant.
bool isArrowParquetVariantExtensionName(std::string_view extension_name);

/// Accepts both the canonical Arrow name (`arrow.parquet.variant`) and Arrow C++ Parquet's
/// registered name (`parquet.variant`).
inline bool isArrowParquetVariantExtensionName(const String & extension_name)
{
    return isArrowParquetVariantExtensionName(std::string_view(extension_name));
}

}
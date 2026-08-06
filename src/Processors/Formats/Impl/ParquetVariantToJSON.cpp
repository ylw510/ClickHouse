#include <Processors/Formats/Impl/ParquetVariantToJSON.h>

#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromOwnString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

bool isArrowParquetVariantExtensionName(std::string_view extension_name)
{
    /// Canonical Arrow extension name, plus the name used by Arrow C++ Parquet bindings.
    return extension_name == "arrow.parquet.variant" || extension_name == "parquet.variant";
}

namespace
{

[[noreturn]] void throwCorrupt(const char * what)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupt Parquet Variant binary: {}", what);
}

UInt32 readLE32(const UInt8 * data, size_t nbytes)
{
    UInt32 v = 0;
    for (size_t i = 0; i < nbytes; ++i)
        v |= static_cast<UInt32>(data[i]) << (8 * i);
    return v;
}

UInt64 readLE64(const UInt8 * data, size_t nbytes)
{
    UInt64 v = 0;
    for (size_t i = 0; i < nbytes; ++i)
        v |= static_cast<UInt64>(data[i]) << (8 * i);
    return v;
}

Int128 readLE128(const UInt8 * data, size_t nbytes)
{
    Int128 v = 0;
    for (size_t i = 0; i < nbytes; ++i)
        v |= Int128(data[i]) << (8 * i);
    /// Sign-extend when fewer than 16 bytes were provided.
    if (nbytes < 16 && nbytes > 0 && (data[nbytes - 1] & 0x80))
    {
        for (size_t i = nbytes; i < 16; ++i)
            v |= Int128(0xFF) << (8 * i);
    }
    return v;
}

struct VariantMetadata
{
    std::vector<std::string_view> dictionary;
};

VariantMetadata parseMetadata(std::string_view metadata)
{
    if (metadata.empty())
        throwCorrupt("empty metadata");

    const auto * data = reinterpret_cast<const UInt8 *>(metadata.data());
    const size_t size = metadata.size();

    const UInt8 header = data[0];
    const UInt8 version = header & 0x0F;
    if (version != 1)
        throwCorrupt("unsupported metadata version");

    const size_t offset_size = ((header >> 6) & 0x03) + 1;
    size_t pos = 1;
    if (pos + offset_size > size)
        throwCorrupt("truncated dictionary_size");

    const UInt32 dictionary_size = readLE32(data + pos, offset_size);
    pos += offset_size;

    const size_t offsets_bytes = static_cast<size_t>(dictionary_size + 1) * offset_size;
    if (pos + offsets_bytes > size)
        throwCorrupt("truncated dictionary offsets");

    std::vector<UInt32> offsets(dictionary_size + 1);
    for (UInt32 i = 0; i <= dictionary_size; ++i)
    {
        offsets[i] = readLE32(data + pos, offset_size);
        pos += offset_size;
    }

    if (offsets.front() != 0)
        throwCorrupt("invalid first dictionary offset");

    const size_t bytes_len = offsets.back();
    if (pos + bytes_len > size)
        throwCorrupt("truncated dictionary bytes");

    const char * bytes = metadata.data() + pos;
    VariantMetadata result;
    result.dictionary.reserve(dictionary_size);
    for (UInt32 i = 0; i < dictionary_size; ++i)
    {
        if (offsets[i] > offsets[i + 1] || offsets[i + 1] > bytes_len)
            throwCorrupt("non-monotonic dictionary offsets");
        result.dictionary.emplace_back(bytes + offsets[i], offsets[i + 1] - offsets[i]);
    }
    return result;
}

void writeJSONStringEscaped(WriteBuffer & out, std::string_view s)
{
    writeJSONString(s, out, FormatSettings{});
}

/// Length of the nested value that starts at `start` inside a fields region whose offset list
/// (including the trailing end-offset) is `field_offsets`. Offsets need not be sorted.
size_t nestedValueLength(const std::vector<UInt32> & field_offsets, UInt32 start)
{
    UInt32 end = std::numeric_limits<UInt32>::max();
    for (UInt32 o : field_offsets)
    {
        if (o > start && o < end)
            end = o;
    }
    if (end == std::numeric_limits<UInt32>::max())
        throwCorrupt("missing end offset for nested value");
    return end - start;
}

void decodeValue(WriteBuffer & out, const VariantMetadata & meta, std::string_view value);

void decodePrimitive(WriteBuffer & out, UInt8 primitive_header, std::string_view value_data)
{
    const auto * data = reinterpret_cast<const UInt8 *>(value_data.data());
    const size_t size = value_data.size();

    switch (primitive_header)
    {
        case 0:
            writeCString("null", out);
            return;
        case 1:
            writeCString("true", out);
            return;
        case 2:
            writeCString("false", out);
            return;
        case 3:
            if (size < 1)
                throwCorrupt("truncated int8");
            writeIntText(static_cast<Int8>(data[0]), out);
            return;
        case 4:
            if (size < 2)
                throwCorrupt("truncated int16");
            writeIntText(static_cast<Int16>(static_cast<UInt16>(readLE32(data, 2))), out);
            return;
        case 5:
            if (size < 4)
                throwCorrupt("truncated int32");
            writeIntText(static_cast<Int32>(readLE32(data, 4)), out);
            return;
        case 6:
            if (size < 8)
                throwCorrupt("truncated int64");
            writeIntText(static_cast<Int64>(readLE64(data, 8)), out);
            return;
        case 7:
        {
            if (size < 8)
                throwCorrupt("truncated double");
            double v = 0;
            memcpy(&v, data, 8);
            writeFloatText(v, out);
            return;
        }
        case 8: // decimal4 (int32 unscaled)
        case 9: // decimal8 (int64 unscaled)
        case 10: // decimal16 (int128 unscaled)
        {
            if (size < 1)
                throwCorrupt("truncated decimal");
            const UInt8 scale = data[0];
            const size_t unscaled_bytes = (primitive_header == 8) ? 4 : (primitive_header == 9) ? 8 : 16;
            if (size < 1 + unscaled_bytes)
                throwCorrupt("truncated decimal payload");

            Int128 unscaled = readLE128(data + 1, unscaled_bytes);
            /// Emit as a JSON number string: optional '-', digits, optional fractional part.
            bool negative = unscaled < 0;
            UInt128 abs_val = negative ? static_cast<UInt128>(-unscaled) : static_cast<UInt128>(unscaled);

            char digits[64];
            size_t n = 0;
            if (abs_val == 0)
            {
                digits[n++] = '0';
            }
            else
            {
                while (abs_val > 0 && n < sizeof(digits))
                {
                    digits[n++] = char('0' + static_cast<char>(abs_val % 10));
                    abs_val /= 10;
                }
            }
            std::reverse(digits, digits + n);

            if (negative)
                writeChar('-', out);

            if (scale == 0)
            {
                out.write(digits, n);
            }
            else if (scale >= n)
            {
                writeChar('0', out);
                writeChar('.', out);
                for (size_t i = 0; i < scale - n; ++i)
                    writeChar('0', out);
                out.write(digits, n);
            }
            else
            {
                out.write(digits, n - scale);
                writeChar('.', out);
                out.write(digits + (n - scale), scale);
            }
            return;
        }
        case 11: // date
            if (size < 4)
                throwCorrupt("truncated date");
            writeIntText(static_cast<Int32>(readLE32(data, 4)), out);
            return;
        case 12: // timestamp micros utc
        case 13: // timestamp micros ntz
        case 17: // time micros ntz
        case 18: // timestamp nanos utc
        case 19: // timestamp nanos ntz
            if (size < 8)
                throwCorrupt("truncated timestamp/time");
            writeIntText(static_cast<Int64>(readLE64(data, 8)), out);
            return;
        case 14: // float
        {
            if (size < 4)
                throwCorrupt("truncated float");
            float v = 0;
            memcpy(&v, data, 4);
            writeFloatText(v, out);
            return;
        }
        case 15: // binary
        {
            if (size < 4)
                throwCorrupt("truncated binary length");
            const UInt32 len = readLE32(data, 4);
            if (size < 4ull + len)
                throwCorrupt("truncated binary");
            writeJSONStringEscaped(out, std::string_view(reinterpret_cast<const char *>(data + 4), len));
            return;
        }
        case 16: // string
        {
            if (size < 4)
                throwCorrupt("truncated string length");
            const UInt32 len = readLE32(data, 4);
            if (size < 4ull + len)
                throwCorrupt("truncated string");
            writeJSONStringEscaped(out, std::string_view(reinterpret_cast<const char *>(data + 4), len));
            return;
        }
        case 20: // uuid (16-byte big-endian)
        {
            if (size < 16)
                throwCorrupt("truncated uuid");
            char buf[36];
            static constexpr char hex[] = "0123456789abcdef";
            size_t p = 0;
            auto emit = [&](size_t from, size_t count)
            {
                for (size_t i = 0; i < count; ++i)
                {
                    buf[p++] = hex[data[from + i] >> 4];
                    buf[p++] = hex[data[from + i] & 0x0F];
                }
            };
            emit(0, 4);
            buf[p++] = '-';
            emit(4, 2);
            buf[p++] = '-';
            emit(6, 2);
            buf[p++] = '-';
            emit(8, 2);
            buf[p++] = '-';
            emit(10, 6);
            writeJSONStringEscaped(out, std::string_view(buf, 36));
            return;
        }
        default:
            /// Spec: new primitive IDs may appear without bumping metadata version.
            writeCString("null", out);
            return;
    }
}

void decodeObjectOrArray(WriteBuffer & out, const VariantMetadata & meta, bool is_object, UInt8 value_header, std::string_view value_data)
{
    const bool is_large = is_object ? ((value_header >> 4) & 0x01) != 0 : ((value_header >> 2) & 0x01) != 0;
    const size_t field_id_size = is_object ? ((value_header >> 2) & 0x03) + 1 : 0;
    const size_t field_offset_size = (value_header & 0x03) + 1;
    const size_t num_elements_size = is_large ? 4 : 1;

    if (value_data.size() < num_elements_size)
        throwCorrupt("truncated num_elements");

    const auto * vd = reinterpret_cast<const UInt8 *>(value_data.data());
    size_t pos = 0;
    const UInt32 num_elements = readLE32(vd + pos, num_elements_size);
    pos += num_elements_size;

    std::vector<UInt32> field_ids;
    if (is_object)
    {
        const size_t ids_bytes = static_cast<size_t>(num_elements) * field_id_size;
        if (pos + ids_bytes > value_data.size())
            throwCorrupt("truncated field_ids");
        field_ids.resize(num_elements);
        for (UInt32 i = 0; i < num_elements; ++i)
        {
            field_ids[i] = readLE32(vd + pos, field_id_size);
            pos += field_id_size;
        }
    }

    const size_t offsets_bytes = static_cast<size_t>(num_elements + 1) * field_offset_size;
    if (pos + offsets_bytes > value_data.size())
        throwCorrupt("truncated field_offsets");

    std::vector<UInt32> field_offsets(num_elements + 1);
    for (UInt32 i = 0; i <= num_elements; ++i)
    {
        field_offsets[i] = readLE32(vd + pos, field_offset_size);
        pos += field_offset_size;
    }

    const char * fields_base = value_data.data() + pos;
    const size_t fields_size = value_data.size() - pos;
    if (field_offsets.back() > fields_size)
        throwCorrupt("field offsets exceed fields region");

    writeChar(is_object ? '{' : '[', out);

    for (UInt32 i = 0; i < num_elements; ++i)
    {
        if (i)
            writeChar(',', out);

        if (is_object)
        {
            if (field_ids[i] >= meta.dictionary.size())
                throwCorrupt("field_id out of dictionary range");
            writeJSONStringEscaped(out, meta.dictionary[field_ids[i]]);
            writeChar(':', out);
        }

        const UInt32 start = field_offsets[i];
        if (start > field_offsets.back())
            throwCorrupt("field offset out of range");
        const size_t len = nestedValueLength(field_offsets, start);
        if (static_cast<size_t>(start) + len > fields_size)
            throwCorrupt("nested value exceeds fields region");
        decodeValue(out, meta, std::string_view(fields_base + start, len));
    }

    writeChar(is_object ? '}' : ']', out);
}

void decodeValue(WriteBuffer & out, const VariantMetadata & meta, std::string_view value)
{
    if (value.empty())
        throwCorrupt("empty value");

    const auto * data = reinterpret_cast<const UInt8 *>(value.data());
    const UInt8 value_metadata = data[0];
    const UInt8 basic_type = value_metadata & 0x03;
    const UInt8 value_header = value_metadata >> 2;
    const std::string_view value_data(value.data() + 1, value.size() - 1);

    switch (basic_type)
    {
        case 0:
            decodePrimitive(out, value_header, value_data);
            return;
        case 1:
        {
            if (value_data.size() < value_header)
                throwCorrupt("truncated short string");
            writeJSONStringEscaped(out, value_data.substr(0, value_header));
            return;
        }
        case 2:
            decodeObjectOrArray(out, meta, /*is_object=*/true, value_header, value_data);
            return;
        case 3:
            decodeObjectOrArray(out, meta, /*is_object=*/false, value_header, value_data);
            return;
        default:
            throwCorrupt("unknown basic_type");
    }
}

}

String decodeParquetVariantToJSON(std::string_view metadata, std::string_view value)
{
    const VariantMetadata meta = parseMetadata(metadata);
    WriteBufferFromOwnString out;
    decodeValue(out, meta, value);
    return out.str();
}

}

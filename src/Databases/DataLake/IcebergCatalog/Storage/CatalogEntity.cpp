#include <Databases/DataLake/IcebergCatalog/Storage/CatalogEntity.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <sstream>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DataLake::IcebergCatalogStorage
{

namespace
{

std::map<std::string, std::string> readStringMap(const Poco::JSON::Object & object, const char * field_name)
{
    std::map<std::string, std::string> result;
    if (!object.has(field_name) || object.isNull(field_name))
        return result;

    const auto & properties_object = object.get(field_name).extract<Poco::JSON::Object::Ptr>();
    for (const auto & key : properties_object->getNames())
        result[key] = properties_object->get(key).convert<std::string>();

    return result;
}

void writeStringMap(Poco::JSON::Object & object, const char * field_name, const std::map<std::string, std::string> & values)
{
    if (values.empty())
        return;

    Poco::JSON::Object::Ptr properties_object = new Poco::JSON::Object;
    for (const auto & [key, value] : values)
        properties_object->set(key, value);

    object.set(field_name, properties_object);
}

}

CatalogEntity parseCatalogEntityFromObject(const Poco::JSON::Object & object)
{
    CatalogEntity entity;
    if (object.has("default-base-location") && !object.isNull("default-base-location"))
        entity.default_base_location = object.get("default-base-location").convert<std::string>();

    entity.properties = readStringMap(object, "properties");
    return entity;
}

void writeCatalogEntityToObject(const CatalogEntity & entity, Poco::JSON::Object & object)
{
    if (!entity.default_base_location.empty())
        object.set("default-base-location", entity.default_base_location);

    writeStringMap(object, "properties", entity.properties);
}

CatalogEntity parseCatalogEntity(const std::string & json)
{
    if (json.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot parse empty catalog entity");

    Poco::JSON::Parser parser;
    const auto parsed = parser.parse(json);
    const auto object = parsed.extract<Poco::JSON::Object::Ptr>();
    return parseCatalogEntityFromObject(*object);
}

std::string serializeCatalogEntity(const CatalogEntity & entity)
{
    Poco::JSON::Object object;
    writeCatalogEntityToObject(entity, object);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(object, oss);
    return oss.str();
}

}

#endif

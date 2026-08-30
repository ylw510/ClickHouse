#pragma once
#include "config.h"

#if USE_AVRO

#include <map>
#include <string>

namespace Poco
{
namespace JSON
{
    class Object;
}
}

namespace DataLake::IcebergCatalogStorage
{

struct CatalogEntity
{
    std::string default_base_location;
    std::map<std::string, std::string> properties;
};

CatalogEntity parseCatalogEntity(const std::string & json);
std::string serializeCatalogEntity(const CatalogEntity & entity);

CatalogEntity parseCatalogEntityFromObject(const Poco::JSON::Object & object);
void writeCatalogEntityToObject(const CatalogEntity & entity, Poco::JSON::Object & object);

}

#endif

#pragma once
#include "config.h"

#if USE_AVRO

#include <map>
#include <string>
#include <vector>

namespace Poco
{
namespace JSON
{
    class Object;
}
}

namespace DataLake::IcebergCatalogStorage
{

struct NamespaceEntity
{
    std::map<std::string, std::string> properties;
};

NamespaceEntity parseNamespaceEntity(const std::string & json);
std::string serializeNamespaceEntity(const NamespaceEntity & entity);

NamespaceEntity parseNamespaceEntityFromObject(const Poco::JSON::Object & object);
void writeNamespaceEntityToObject(const NamespaceEntity & entity, Poco::JSON::Object & object);

void mergeNamespaceProperties(
    NamespaceEntity & entity,
    const std::map<std::string, std::string> & updates,
    const std::vector<std::string> & removals);

}

#endif

#pragma once
#include "config.h"

#if USE_AVRO

#include <string>
#include <vector>

namespace DataLake::IcebergCatalogStorage
{

inline constexpr const char * ICEBERG_ROOT = "/iceberg";
inline constexpr const char * CATALOGS_CONTAINER = "catalogs";
inline constexpr const char * NAMESPACES_CONTAINER = "namespaces";
inline constexpr const char * TABLES_CONTAINER = "tables";

struct WarehousePath
{
    std::string warehouse;
};

/// `levels` empty means the warehouse root namespace container.
struct NamespacePath
{
    std::string warehouse;
    std::vector<std::string> levels;
};

std::string encodePathSegment(const std::string & segment);
std::string decodePathSegment(const std::string & encoded_segment);

std::string catalogNodePath(const std::string & root_path, const WarehousePath & path);
std::string catalogNamespacesContainerPath(const std::string & root_path, const WarehousePath & path);

std::string namespaceNodePath(const std::string & root_path, const NamespacePath & path);
std::string namespaceChildrenContainerPath(const std::string & root_path, const NamespacePath & path);
std::string tablesContainerPath(const std::string & root_path, const NamespacePath & path);

std::string listNamespacesContainerPath(const std::string & root_path, const NamespacePath & parent);

}

#endif

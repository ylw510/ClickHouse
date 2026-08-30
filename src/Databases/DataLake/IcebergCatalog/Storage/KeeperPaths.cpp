#include <Databases/DataLake/IcebergCatalog/Storage/KeeperPaths.h>

#if USE_AVRO

#include <Common/escapeForFileName.h>
#include <Common/Exception.h>
#include <boost/algorithm/string/join.hpp>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DataLake::IcebergCatalogStorage
{

namespace
{

std::string normalizeRootPath(std::string root_path)
{
    if (root_path.empty())
        root_path = ICEBERG_ROOT;

    while (root_path.size() > 1 && root_path.back() == '/')
        root_path.pop_back();

    if (root_path.front() != '/')
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Iceberg catalog root path must be absolute");

    return root_path;
}

std::string joinPath(const std::vector<std::string> & parts)
{
    if (parts.empty())
        return "";

    return boost::algorithm::join(parts, "/");
}

}

std::string encodePathSegment(const std::string & segment)
{
    return DB::escapeForFileName(segment);
}

std::string decodePathSegment(const std::string & encoded_segment)
{
    return DB::unescapeForFileName(encoded_segment);
}

std::string catalogNodePath(const std::string & root_path, const WarehousePath & path)
{
    const auto normalized_root = normalizeRootPath(root_path);
    return normalized_root + "/" + CATALOGS_CONTAINER + "/" + encodePathSegment(path.warehouse);
}

std::string catalogNamespacesContainerPath(const std::string & root_path, const WarehousePath & path)
{
    return catalogNodePath(root_path, path) + "/" + NAMESPACES_CONTAINER;
}

std::string namespaceNodePath(const std::string & root_path, const NamespacePath & path)
{
    if (path.levels.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Namespace path must contain at least one level");

    std::vector<std::string> parts;
    parts.push_back(normalizeRootPath(root_path));
    parts.push_back(CATALOGS_CONTAINER);
    parts.push_back(encodePathSegment(path.warehouse));
    parts.push_back(NAMESPACES_CONTAINER);

    for (size_t i = 0; i < path.levels.size(); ++i)
    {
        parts.push_back(encodePathSegment(path.levels[i]));
        if (i + 1 < path.levels.size())
            parts.push_back(NAMESPACES_CONTAINER);
    }

    return joinPath(parts);
}

std::string namespaceChildrenContainerPath(const std::string & root_path, const NamespacePath & path)
{
    return namespaceNodePath(root_path, path) + "/" + NAMESPACES_CONTAINER;
}

std::string tablesContainerPath(const std::string & root_path, const NamespacePath & path)
{
    return namespaceNodePath(root_path, path) + "/" + TABLES_CONTAINER;
}

std::string listNamespacesContainerPath(const std::string & root_path, const NamespacePath & parent)
{
    if (parent.levels.empty())
        return catalogNamespacesContainerPath(root_path, WarehousePath{parent.warehouse});

    return namespaceChildrenContainerPath(root_path, parent);
}

}

#endif

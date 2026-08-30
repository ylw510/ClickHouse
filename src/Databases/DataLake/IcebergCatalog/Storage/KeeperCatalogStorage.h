#pragma once
#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/IcebergCatalog/Storage/CatalogEntity.h>
#include <Databases/DataLake/IcebergCatalog/Storage/KeeperPaths.h>
#include <Databases/DataLake/IcebergCatalog/Storage/NamespaceEntity.h>

#include <Common/ZooKeeper/ZooKeeper.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace DataLake::IcebergCatalogStorage
{

/// Keeper-backed storage for Iceberg REST catalog warehouses and namespaces.
class KeeperCatalogStorage
{
public:
    explicit KeeperCatalogStorage(zkutil::ZooKeeperPtr zookeeper_, std::string root_path_ = ICEBERG_ROOT);

    void ensureRootNodes();

    void createCatalog(const WarehousePath & path, const CatalogEntity & entity);
    CatalogEntity getCatalog(const WarehousePath & path);
    bool catalogExists(const WarehousePath & path);
    void deleteCatalog(const WarehousePath & path);

    void createNamespace(const NamespacePath & path, const NamespaceEntity & entity);
    NamespaceEntity getNamespace(const NamespacePath & path);
    bool namespaceExists(const NamespacePath & path);
    void deleteNamespace(const NamespacePath & path);
    std::vector<std::string> listNamespaces(const NamespacePath & parent);
    void updateNamespaceProperties(
        const NamespacePath & path,
        const std::map<std::string, std::string> & updates,
        const std::vector<std::string> & removals);

private:
    zkutil::ZooKeeperPtr zookeeper;
    std::string root_path;

    void ensureCatalogExists(const WarehousePath & path);
    void ensureNamespaceStructure(const NamespacePath & path, const NamespaceEntity & entity, bool create_if_missing);
    void createNamespaceContainers(const NamespacePath & path);

    std::string getNodeDataOrThrow(const std::string & path) const;
    bool nodeExists(const std::string & path) const;
    bool containerIsEmpty(const std::string & path) const;

    void createPersistentNode(const std::string & path, const std::string & data);
    void createPersistentNodeIfNotExists(const std::string & path, const std::string & data);
    void setNodeData(const std::string & path, const std::string & data);
    void removeNodeIfExists(const std::string & path);
};

}

#endif

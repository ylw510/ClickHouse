#include <Databases/DataLake/IcebergCatalog/Storage/KeeperCatalogStorage.h>

#if USE_AVRO

#include <Databases/DataLake/IcebergCatalog/Storage/KeeperCatalogErrors.h>

#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Exception.h>
#include <boost/algorithm/string/join.hpp>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int DATABASE_NOT_EMPTY;
    extern const int NOT_FOUND_NODE;
}

namespace DataLake::IcebergCatalogStorage
{

namespace
{

bool isContainerName(const std::string & name)
{
    return name == NAMESPACES_CONTAINER || name == TABLES_CONTAINER;
}

[[noreturn]] void rethrowKeeperError(const Coordination::Error code, const std::string & path)
{
    if (code == Coordination::Error::ZNONODE)
        throw NotFoundException(DB::ErrorCodes::NOT_FOUND_NODE, "Keeper node {} does not exist", path);

    if (code == Coordination::Error::ZNODEEXISTS)
        throw AlreadyExistsException(DB::ErrorCodes::DATABASE_ALREADY_EXISTS, "Keeper node {} already exists", path);

    throw zkutil::KeeperException::fromPath(code, path);
}

}

KeeperCatalogStorage::KeeperCatalogStorage(zkutil::ZooKeeperPtr zookeeper_, std::string root_path_)
    : zookeeper(std::move(zookeeper_))
    , root_path(std::move(root_path_))
{
    if (!zookeeper)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "ZooKeeper client must not be null");
}

void KeeperCatalogStorage::ensureRootNodes()
{
    zookeeper->createAncestors(root_path);
    createPersistentNodeIfNotExists(root_path, "");
    createPersistentNodeIfNotExists(root_path + "/" + CATALOGS_CONTAINER, "");
}

void KeeperCatalogStorage::createPersistentNode(const std::string & path, const std::string & data)
{
    try
    {
        zookeeper->create(path, data, zkutil::CreateMode::Persistent);
    }
    catch (const zkutil::KeeperException & exception)
    {
        rethrowKeeperError(exception.code, path);
    }
}

void KeeperCatalogStorage::createPersistentNodeIfNotExists(const std::string & path, const std::string & data)
{
    if (!nodeExists(path))
        createPersistentNode(path, data);
}

void KeeperCatalogStorage::setNodeData(const std::string & path, const std::string & data)
{
    try
    {
        zookeeper->set(path, data);
    }
    catch (const zkutil::KeeperException & exception)
    {
        rethrowKeeperError(exception.code, path);
    }
}

void KeeperCatalogStorage::removeNodeIfExists(const std::string & path)
{
    if (!nodeExists(path))
        return;

    try
    {
        zookeeper->remove(path);
    }
    catch (const zkutil::KeeperException & exception)
    {
        rethrowKeeperError(exception.code, path);
    }
}

bool KeeperCatalogStorage::nodeExists(const std::string & path) const
{
    return zookeeper->exists(path);
}

std::string KeeperCatalogStorage::getNodeDataOrThrow(const std::string & path) const
{
    try
    {
        return zookeeper->get(path);
    }
    catch (const zkutil::KeeperException & exception)
    {
        rethrowKeeperError(exception.code, path);
    }
}

bool KeeperCatalogStorage::containerIsEmpty(const std::string & path) const
{
    if (!nodeExists(path))
        return true;

    const auto children = zookeeper->getChildren(path);
    return children.empty();
}

void KeeperCatalogStorage::ensureCatalogExists(const WarehousePath & path)
{
    if (!catalogExists(path))
        throw NotFoundException(DB::ErrorCodes::NOT_FOUND_NODE, "Catalog {} does not exist", path.warehouse);
}

void KeeperCatalogStorage::createNamespaceContainers(const NamespacePath & path)
{
    createPersistentNodeIfNotExists(namespaceChildrenContainerPath(root_path, path), "");
    createPersistentNodeIfNotExists(tablesContainerPath(root_path, path), "");
}

void KeeperCatalogStorage::ensureNamespaceStructure(
    const NamespacePath & path,
    const NamespaceEntity & entity,
    bool create_if_missing)
{
    if (path.levels.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Namespace path must contain at least one level");

    NamespacePath partial_path{path.warehouse, {}};
    for (size_t i = 0; i < path.levels.size(); ++i)
    {
        partial_path.levels.push_back(path.levels[i]);
        const auto node_path = namespaceNodePath(root_path, partial_path);
        const bool is_leaf = i + 1 == path.levels.size();

        if (is_leaf)
        {
            if (nodeExists(node_path))
                throw AlreadyExistsException(
                    DB::ErrorCodes::DATABASE_ALREADY_EXISTS,
                    "Namespace {} already exists",
                    boost::algorithm::join(path.levels, "."));

            createPersistentNode(node_path, serializeNamespaceEntity(entity));
        }
        else if (!nodeExists(node_path))
        {
            if (!create_if_missing)
                throw NotFoundException(
                    DB::ErrorCodes::NOT_FOUND_NODE,
                    "Parent namespace {} does not exist",
                    boost::algorithm::join(partial_path.levels, "."));

            createPersistentNode(node_path, serializeNamespaceEntity({}));
        }

        createNamespaceContainers(partial_path);
    }
}

void KeeperCatalogStorage::createCatalog(const WarehousePath & path, const CatalogEntity & entity)
{
    if (path.warehouse.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Warehouse name must not be empty");

    ensureRootNodes();

    Coordination::Requests requests;
    requests.emplace_back(zkutil::makeCreateRequest(catalogNodePath(root_path, path), serializeCatalogEntity(entity), zkutil::CreateMode::Persistent));
    requests.emplace_back(zkutil::makeCreateRequest(catalogNamespacesContainerPath(root_path, path), "", zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(requests, responses);
    if (code == Coordination::Error::ZNODEEXISTS)
        throw AlreadyExistsException(DB::ErrorCodes::DATABASE_ALREADY_EXISTS, "Catalog {} already exists", path.warehouse);

    if (code != Coordination::Error::ZOK)
        rethrowKeeperError(code, catalogNodePath(root_path, path));
}

CatalogEntity KeeperCatalogStorage::getCatalog(const WarehousePath & path)
{
    return parseCatalogEntity(getNodeDataOrThrow(catalogNodePath(root_path, path)));
}

bool KeeperCatalogStorage::catalogExists(const WarehousePath & path)
{
    return nodeExists(catalogNodePath(root_path, path));
}

void KeeperCatalogStorage::deleteCatalog(const WarehousePath & path)
{
    ensureCatalogExists(path);

    if (!containerIsEmpty(catalogNamespacesContainerPath(root_path, path)))
        throw NamespaceNotEmptyException(DB::ErrorCodes::DATABASE_NOT_EMPTY, "Catalog {} is not empty", path.warehouse);

    removeNodeIfExists(catalogNamespacesContainerPath(root_path, path));
    removeNodeIfExists(catalogNodePath(root_path, path));
}

void KeeperCatalogStorage::createNamespace(const NamespacePath & path, const NamespaceEntity & entity)
{
    if (path.warehouse.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Warehouse name must not be empty");

    if (path.levels.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Namespace path must contain at least one level");

    ensureCatalogExists(WarehousePath{path.warehouse});
    ensureNamespaceStructure(path, entity, /* create_if_missing */ true);
}

NamespaceEntity KeeperCatalogStorage::getNamespace(const NamespacePath & path)
{
    return parseNamespaceEntity(getNodeDataOrThrow(namespaceNodePath(root_path, path)));
}

bool KeeperCatalogStorage::namespaceExists(const NamespacePath & path)
{
    if (path.levels.empty())
        return false;

    return nodeExists(namespaceNodePath(root_path, path));
}

void KeeperCatalogStorage::deleteNamespace(const NamespacePath & path)
{
    if (path.levels.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Namespace path must contain at least one level");

    if (!namespaceExists(path))
        throw NotFoundException(DB::ErrorCodes::NOT_FOUND_NODE, "Namespace {} does not exist", boost::algorithm::join(path.levels, "."));

    if (!containerIsEmpty(namespaceChildrenContainerPath(root_path, path)))
    {
        throw NamespaceNotEmptyException(
            DB::ErrorCodes::DATABASE_NOT_EMPTY,
            "Namespace {} is not empty: child namespaces exist",
            boost::algorithm::join(path.levels, "."));
    }

    if (!containerIsEmpty(tablesContainerPath(root_path, path)))
    {
        throw NamespaceNotEmptyException(
            DB::ErrorCodes::DATABASE_NOT_EMPTY,
            "Namespace {} is not empty: tables exist",
            boost::algorithm::join(path.levels, "."));
    }

    removeNodeIfExists(tablesContainerPath(root_path, path));
    removeNodeIfExists(namespaceChildrenContainerPath(root_path, path));
    removeNodeIfExists(namespaceNodePath(root_path, path));
}

std::vector<std::string> KeeperCatalogStorage::listNamespaces(const NamespacePath & parent)
{
    if (parent.warehouse.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Warehouse name must not be empty");

    ensureCatalogExists(WarehousePath{parent.warehouse});

    const auto container_path = listNamespacesContainerPath(root_path, parent);
    if (!parent.levels.empty() && !nodeExists(namespaceNodePath(root_path, parent)))
    {
        throw NotFoundException(
            DB::ErrorCodes::NOT_FOUND_NODE,
            "Parent namespace {} does not exist",
            boost::algorithm::join(parent.levels, "."));
    }

    if (!nodeExists(container_path))
        return {};

    std::vector<std::string> result;
    for (const auto & child : zookeeper->getChildren(container_path))
    {
        if (isContainerName(child))
            continue;

        result.push_back(decodePathSegment(child));
    }

    return result;
}

void KeeperCatalogStorage::updateNamespaceProperties(
    const NamespacePath & path,
    const std::map<std::string, std::string> & updates,
    const std::vector<std::string> & removals)
{
    auto entity = getNamespace(path);
    mergeNamespaceProperties(entity, updates, removals);
    setNodeData(namespaceNodePath(root_path, path), serializeNamespaceEntity(entity));
}

}

#endif

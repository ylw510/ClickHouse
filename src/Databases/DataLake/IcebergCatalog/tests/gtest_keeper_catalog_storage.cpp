#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/IcebergCatalog/Storage/CatalogEntity.h>
#include <Databases/DataLake/IcebergCatalog/Storage/KeeperCatalogErrors.h>
#include <Databases/DataLake/IcebergCatalog/Storage/KeeperCatalogStorage.h>
#include <Databases/DataLake/IcebergCatalog/Storage/KeeperPaths.h>
#include <Databases/DataLake/IcebergCatalog/Storage/NamespaceEntity.h>

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <gtest/gtest.h>

#include <algorithm>

using namespace DataLake::IcebergCatalogStorage;

namespace
{

zkutil::ZooKeeperPtr makeTestZooKeeper()
{
    zkutil::ZooKeeperArgs args;
    args.implementation = "testkeeper";
    return zkutil::ZooKeeper::create(std::move(args), /* zk_log */ nullptr, /* aggregated_zookeeper_log */ nullptr);
}

KeeperCatalogStorage makeStorage(const std::string & root_path = "/iceberg_test")
{
    return KeeperCatalogStorage(makeTestZooKeeper(), root_path);
}

TEST(IcebergKeeperPaths, NamespaceNodePathForNestedNamespace)
{
    NamespacePath path{"main", {"accounting", "tax"}};
    EXPECT_EQ(
        namespaceNodePath("/iceberg", path),
        "/iceberg/catalogs/main/namespaces/accounting/namespaces/tax");
    EXPECT_EQ(
        listNamespacesContainerPath("/iceberg", NamespacePath{"main", {"accounting"}}),
        "/iceberg/catalogs/main/namespaces/accounting/namespaces");
    EXPECT_EQ(
        listNamespacesContainerPath("/iceberg", NamespacePath{"main", {}}),
        "/iceberg/catalogs/main/namespaces");
}

TEST(IcebergKeeperPaths, EncodeDecodeRoundtrip)
{
    EXPECT_EQ(decodePathSegment(encodePathSegment("accounting")), "accounting");
    EXPECT_EQ(decodePathSegment(encodePathSegment("a.b")), "a.b");
}

TEST(IcebergKeeperCatalogStorage, CreateAndGetCatalog)
{
    auto storage = makeStorage("/iceberg_pr2_catalog");

    CatalogEntity entity;
    entity.default_base_location = "s3://bucket/warehouse/";
    entity.properties = {{"owner", "team-a"}};

    storage.createCatalog(WarehousePath{"main"}, entity);

    const auto loaded = storage.getCatalog(WarehousePath{"main"});
    EXPECT_EQ(loaded.default_base_location, entity.default_base_location);
    ASSERT_EQ(loaded.properties.size(), 1);
    EXPECT_EQ(loaded.properties.at("owner"), "team-a");
    EXPECT_TRUE(storage.catalogExists(WarehousePath{"main"}));
}

TEST(IcebergKeeperCatalogStorage, DuplicateCatalogThrows)
{
    auto storage = makeStorage("/iceberg_pr2_duplicate_catalog");

    storage.createCatalog(WarehousePath{"main"}, CatalogEntity{});

    EXPECT_THROW(storage.createCatalog(WarehousePath{"main"}, CatalogEntity{}), AlreadyExistsException);
}

TEST(IcebergKeeperCatalogStorage, GetMissingCatalogThrows)
{
    auto storage = makeStorage("/iceberg_pr2_missing_catalog");
    storage.ensureRootNodes();

    EXPECT_THROW(storage.getCatalog(WarehousePath{"missing"}), NotFoundException);
}

TEST(IcebergKeeperCatalogStorage, CreateNestedNamespace)
{
    auto storage = makeStorage("/iceberg_pr2_nested_ns");
    storage.createCatalog(WarehousePath{"main"}, CatalogEntity{});

    NamespaceEntity entity;
    entity.properties = {{"location", "s3://bucket/warehouse/accounting/tax/"}};

    storage.createNamespace(NamespacePath{"main", {"accounting", "tax"}}, entity);

    EXPECT_TRUE(storage.namespaceExists(NamespacePath{"main", {"accounting"}}));
    EXPECT_TRUE(storage.namespaceExists(NamespacePath{"main", {"accounting", "tax"}}));

    const auto loaded = storage.getNamespace(NamespacePath{"main", {"accounting", "tax"}});
    ASSERT_EQ(loaded.properties.size(), 1);
    EXPECT_EQ(loaded.properties.at("location"), entity.properties.at("location"));
}

TEST(IcebergKeeperCatalogStorage, ListNamespacesWithParent)
{
    auto storage = makeStorage("/iceberg_pr2_list_ns");
    storage.createCatalog(WarehousePath{"main"}, CatalogEntity{});
    storage.createNamespace(NamespacePath{"main", {"sales"}}, NamespaceEntity{});
    storage.createNamespace(NamespacePath{"main", {"accounting", "tax"}}, NamespaceEntity{});
    storage.createNamespace(NamespacePath{"main", {"accounting", "credits"}}, NamespaceEntity{});

    const auto top_level = storage.listNamespaces(NamespacePath{"main", {}});
    std::sort(top_level.begin(), top_level.end());
    EXPECT_EQ(top_level, (std::vector<std::string>{"accounting", "sales"}));

    auto accounting_children = storage.listNamespaces(NamespacePath{"main", {"accounting"}});
    std::sort(accounting_children.begin(), accounting_children.end());
    EXPECT_EQ(accounting_children, (std::vector<std::string>{"credits", "tax"}));
}

TEST(IcebergKeeperCatalogStorage, DeleteEmptyNamespace)
{
    auto storage = makeStorage("/iceberg_pr2_delete_ns");
    storage.createCatalog(WarehousePath{"main"}, CatalogEntity{});
    storage.createNamespace(NamespacePath{"main", {"accounting", "tax"}}, NamespaceEntity{});

    storage.deleteNamespace(NamespacePath{"main", {"accounting", "tax"}});

    EXPECT_FALSE(storage.namespaceExists(NamespacePath{"main", {"accounting", "tax"}}));
    EXPECT_TRUE(storage.namespaceExists(NamespacePath{"main", {"accounting"}}));
}

TEST(IcebergKeeperCatalogStorage, DeleteNonEmptyNamespaceThrows)
{
    auto storage = makeStorage("/iceberg_pr2_delete_nonempty");
    storage.createCatalog(WarehousePath{"main"}, CatalogEntity{});
    storage.createNamespace(NamespacePath{"main", {"accounting", "tax"}}, NamespaceEntity{});

    EXPECT_THROW(storage.deleteNamespace(NamespacePath{"main", {"accounting"}}), NamespaceNotEmptyException);
}

TEST(IcebergKeeperCatalogStorage, UpdateNamespaceProperties)
{
    auto storage = makeStorage("/iceberg_pr2_update_props");
    storage.createCatalog(WarehousePath{"main"}, CatalogEntity{});
    storage.createNamespace(NamespacePath{"main", {"sales"}}, NamespaceEntity{});

    storage.updateNamespaceProperties(
        NamespacePath{"main", {"sales"}},
        {{"owner", "alice"}, {"location", "s3://bucket/sales/"}},
        {});

    auto entity = storage.getNamespace(NamespacePath{"main", {"sales"}});
    EXPECT_EQ(entity.properties.at("owner"), "alice");
    EXPECT_EQ(entity.properties.at("location"), "s3://bucket/sales/");

    storage.updateNamespaceProperties(
        NamespacePath{"main", {"sales"}},
        {{"owner", "bob"}},
        {"location"});

    entity = storage.getNamespace(NamespacePath{"main", {"sales"}});
    EXPECT_EQ(entity.properties.at("owner"), "bob");
    EXPECT_EQ(entity.properties.find("location"), entity.properties.end());
}

TEST(IcebergKeeperCatalogStorage, DeleteCatalogWhenEmpty)
{
    auto storage = makeStorage("/iceberg_pr2_delete_catalog");
    storage.createCatalog(WarehousePath{"main"}, CatalogEntity{});

    storage.deleteCatalog(WarehousePath{"main"});
    EXPECT_FALSE(storage.catalogExists(WarehousePath{"main"}));
}

TEST(IcebergKeeperCatalogStorage, DeleteCatalogWhenNotEmptyThrows)
{
    auto storage = makeStorage("/iceberg_pr2_delete_catalog_nonempty");
    storage.createCatalog(WarehousePath{"main"}, CatalogEntity{});
    storage.createNamespace(NamespacePath{"main", {"sales"}}, NamespaceEntity{});

    EXPECT_THROW(storage.deleteCatalog(WarehousePath{"main"}), NamespaceNotEmptyException);
}

}

#endif

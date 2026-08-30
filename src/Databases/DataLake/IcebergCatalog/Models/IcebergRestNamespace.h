#pragma once
#include "config.h"

#if USE_AVRO

#include <Poco/URI.h>
#include <string>
#include <vector>

namespace Poco
{
namespace JSON
{
    class Object;
}
}

namespace DataLake::IcebergRestModels
{

std::string encodeNamespaceForURI(const std::string & namespace_name);
Poco::URI::QueryParameters createParentNamespaceQueryParams(const std::string & base_namespace);

struct NamespaceListParseOptions
{
    /// When true, skip all namespace entries if `base_namespace` is non-empty.
    bool skip_subnamespaces_when_parent_non_empty = false;
    /// When true and every entry was skipped, do not propagate `next-page-token`.
    bool suppress_pagination_when_all_entries_skipped = false;
};

struct NamespaceListPage
{
    std::vector<std::string> namespaces;
    std::string next_page_token;
};

NamespaceListPage parseNamespaceListPage(
    const std::string & json,
    const std::string & base_namespace,
    const NamespaceListParseOptions & options);

Poco::JSON::Object::Ptr buildCreateNamespaceRequest(const std::string & namespace_name, const std::string & location);
std::string serializeCreateNamespaceRequest(const std::string & namespace_name, const std::string & location);
std::string serializeNamespaceListPage(const NamespaceListPage & page);

}

#endif

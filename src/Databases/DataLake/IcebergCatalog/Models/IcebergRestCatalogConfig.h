#pragma once
#include "config.h"

#if USE_AVRO

#include <filesystem>
#include <string>

namespace Poco
{
namespace JSON
{
    class Object;
}
}

namespace DataLake::IcebergRestModels
{

/// Parse and serialize `GET /v1/config` (`CatalogConfigResponse`).

/// Catalog defaults or per-warehouse overrides from config response.
struct CatalogConfigSettings
{
    std::filesystem::path prefix;
    std::string default_base_location;

    void mergeFrom(const CatalogConfigSettings & overrides);
};

/// Full config response with server defaults and warehouse-specific overrides.
struct CatalogConfigResponse
{
    CatalogConfigSettings defaults;
    CatalogConfigSettings overrides;

    /// Applies overrides on top of defaults.
    CatalogConfigSettings merged() const;
};

CatalogConfigResponse parseCatalogConfigResponse(const std::string & json);
void applyCatalogConfigSettings(const Poco::JSON::Object::Ptr & object, CatalogConfigSettings & result);

std::string serializeCatalogConfigResponse(const CatalogConfigResponse & response);

}

#endif

#pragma once
#include "config.h"

#if USE_AVRO

#include <Common/Exception.h>

namespace DataLake::IcebergCatalogStorage
{

/// Thrown when a catalog or namespace node does not exist in Keeper.
class NotFoundException : public DB::Exception
{
public:
    using DB::Exception::Exception;
};

/// Thrown when creating a catalog or namespace that already exists.
class AlreadyExistsException : public DB::Exception
{
public:
    using DB::Exception::Exception;
};

/// Thrown when deleting a namespace that still has child namespaces or tables.
class NamespaceNotEmptyException : public DB::Exception
{
public:
    using DB::Exception::Exception;
};

}

#endif

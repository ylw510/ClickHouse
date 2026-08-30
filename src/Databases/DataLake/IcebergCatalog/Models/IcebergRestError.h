#pragma once
#include "config.h"

#if USE_AVRO

#include <optional>
#include <string>

namespace DataLake::IcebergRestModels
{

/// JSON parse/serialize for Iceberg REST `ErrorResponse` (non-2xx responses).

/// Fields inside the top-level `error` object.
struct ErrorResponse
{
    std::string message;
    std::string type;
    int code = 0;
};

/// Returns nullopt if JSON is invalid or lacks top-level `error`.
std::optional<ErrorResponse> tryParseErrorResponse(const std::string & json);
/// Produces `{"error": {...}}`.
std::string serializeErrorResponse(const ErrorResponse & error);

}

#endif

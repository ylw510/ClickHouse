# Script to print build summary after successful build.
# Invoked by the build-summary target. Reads build start time from
# .build_start_epoch (written by record-build-start target) and prints
# total time and finished timestamp.
#
# ClickHouse supports Linux, macOS, FreeBSD, SunOS - all use Unix date command.
#
# Usage:
#   - Normally invoked from the build system, which passes CLICKHOUSE_BUILD_DIR.
#   - If not set, try to infer it from CMake variables or the current directory.

if(NOT DEFINED CLICKHOUSE_BUILD_DIR OR "${CLICKHOUSE_BUILD_DIR}" STREQUAL "")
    if(DEFINED CMAKE_BINARY_DIR AND NOT "${CMAKE_BINARY_DIR}" STREQUAL "")
        set(CLICKHOUSE_BUILD_DIR "${CMAKE_BINARY_DIR}")
    else()
        execute_process(
            COMMAND "${CMAKE_COMMAND}" -E pwd
            OUTPUT_VARIABLE CLICKHOUSE_BUILD_DIR
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
    endif()
endif()

set(BUILD_DIR "${CLICKHOUSE_BUILD_DIR}")

set(START_FILE "${BUILD_DIR}/.build_start_epoch")
set(ELAPSED_STR "")
set(FINISHED_STR "")
set(BUILD_USER "")
set(BUILD_TYPE "")

# Detect build user (best-effort, Unix platforms)
execute_process(
    COMMAND whoami
    OUTPUT_VARIABLE BUILD_USER
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT BUILD_USER AND DEFINED ENV{USER})
    set(BUILD_USER "$ENV{USER}")
endif()
if(NOT BUILD_USER AND DEFINED ENV{LOGNAME})
    set(BUILD_USER "$ENV{LOGNAME}")
endif()

# Build type (optional, passed from CMake)
if(DEFINED CLICKHOUSE_BUILD_TYPE AND NOT "${CLICKHOUSE_BUILD_TYPE}" STREQUAL "")
    set(BUILD_TYPE "${CLICKHOUSE_BUILD_TYPE}")
endif()

# Get finished timestamp (UTC, ISO 8601)
execute_process(
    COMMAND date -u "+%Y-%m-%dT%H:%M:%SZ"
    OUTPUT_VARIABLE FINISHED_STR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT FINISHED_STR)
    string(TIMESTAMP FINISHED_STR "%Y-%m-%dT%H:%M:%SZ" UTC)
endif()

# Calculate elapsed time if start file exists
if(EXISTS "${START_FILE}")
    file(READ "${START_FILE}" START_EPOCH)
    string(STRIP "${START_EPOCH}" START_EPOCH)
    execute_process(
        COMMAND date "+%s"
        OUTPUT_VARIABLE END_EPOCH
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    string(STRIP "${END_EPOCH}" END_EPOCH)
    if(START_EPOCH AND END_EPOCH)
        math(EXPR ELAPSED_SEC "${END_EPOCH} - ${START_EPOCH}")
        math(EXPR ELAPSED_MIN "${ELAPSED_SEC} / 60")
        math(EXPR ELAPSED_SEC_REM "${ELAPSED_SEC} % 60")
        if(ELAPSED_MIN GREATER 0)
            set(ELAPSED_STR "${ELAPSED_MIN}m ${ELAPSED_SEC_REM}s")
        else()
            set(ELAPSED_STR "${ELAPSED_SEC_REM}s")
        endif()
    endif()
endif()

# Print build summary (modern style)
string(REPLACE "T" " " FINISHED_DISPLAY "${FINISHED_STR}")
string(REPLACE "Z" " UTC" FINISHED_DISPLAY "${FINISHED_DISPLAY}")
message("")
if(ELAPSED_STR)
    if(BUILD_TYPE)
        message("  ✓ ClickHouse (${BUILD_TYPE}) build completed in ${ELAPSED_STR}")
    else()
        message("  ✓ ClickHouse build completed in ${ELAPSED_STR}")
    endif()
else()
    if(BUILD_TYPE)
        message("  ✓ ClickHouse (${BUILD_TYPE}) build completed successfully")
    else()
        message("  ✓ ClickHouse build completed successfully")
    endif()
endif()
if(BUILD_USER AND NOT CLICKHOUSE_BUILD_HIDE_USER)
    message("    by ${BUILD_USER} in ${BUILD_DIR}")
else()
    message("    in ${BUILD_DIR}")
endif()
message("    ${FINISHED_DISPLAY}")
message("")

#include <Storages/System/StorageSystemVectorSimilarityIndexCache.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnsDescription StorageSystemVectorSimilarityIndexCache::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"disk_name", std::make_shared<DataTypeString>(), "Disk of the cached data part."},
        {"data_path", std::make_shared<DataTypeString>(), "Relative path of the cached data part."},
        {"index_name", std::make_shared<DataTypeString>(), "Skip-index file name of the cached granule (`skp_idx_<name>`)."},
        {"index_mark", std::make_shared<DataTypeUInt64>(), "Mark of the cached vector similarity index granule."},
        {"size", std::make_shared<DataTypeUInt64>(), "Accounted size of the cache entry in bytes."},
    };
}

StorageSystemVectorSimilarityIndexCache::StorageSystemVectorSimilarityIndexCache(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemVectorSimilarityIndexCache::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto cache = context->getVectorSimilarityIndexCache();
    if (!cache)
        return;

    for (const auto & [key, cell] : cache->dump())
    {
        String disk_name;
        String data_path = key.path_to_data_part;
        if (const auto pos = key.path_to_data_part.find(':'); pos != String::npos)
        {
            disk_name = key.path_to_data_part.substr(0, pos);
            data_path = key.path_to_data_part.substr(pos + 1);
        }

        size_t i = 0;
        res_columns[i++]->insert(disk_name);
        res_columns[i++]->insert(data_path);
        res_columns[i++]->insert(key.index_name);
        res_columns[i++]->insert(key.index_mark);
        res_columns[i++]->insert(cell ? cell->memory_bytes : 0);
    }
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemVectorSimilarityIndexCache) }

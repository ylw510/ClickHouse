-- Tags: no-fasttest, no-ordinary-database
-- Vector similarity indexes need usearch. After OPTIMIZE, source parts stay
-- Outdated until old_parts_lifetime; DROP FOR TABLE must still evict them.

SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS vs_cache_outdated;

CREATE TABLE vs_cache_outdated
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, old_parts_lifetime = 10000000;

SYSTEM STOP MERGES vs_cache_outdated;

INSERT INTO vs_cache_outdated VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]);
INSERT INTO vs_cache_outdated VALUES (2, [0.0, 2.0]), (3, [0.0, 2.1]);

SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_outdated;
SELECT 'before optimize', count() FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_outdated';

SYSTEM START MERGES vs_cache_outdated;
OPTIMIZE TABLE vs_cache_outdated FINAL;
SELECT 'after optimize', count() FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_outdated';

SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_outdated;
SELECT 'after drop', count() FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_outdated';

DROP TABLE vs_cache_outdated SYNC;

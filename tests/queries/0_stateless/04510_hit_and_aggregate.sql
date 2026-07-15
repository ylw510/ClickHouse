DROP TABLE IF EXISTS t_ha;
CREATE TABLE t_ha (id UInt32, category String, score UInt32) ENGINE=MergeTree ORDER BY id;
INSERT INTO t_ha VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',1);

SELECT id, score FROM t_ha ORDER BY score DESC LIMIT 2
WITH AGGREGATES (SELECT category, count() AS c GROUP BY category ORDER BY c DESC)
FORMAT JSON;

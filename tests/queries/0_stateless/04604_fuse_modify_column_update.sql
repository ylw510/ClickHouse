-- Fuse MODIFY COLUMN + UPDATE in one ALTER: rewrite non-trivially convertible values
-- with an expression of the previous column type (issue #108720).

DROP TABLE IF EXISTS t_fuse_modify_update;
DROP TABLE IF EXISTS t_fuse_modify_update_bare;
DROP TABLE IF EXISTS t_fuse_modify_update_order;
DROP TABLE IF EXISTS t_fuse_modify_update_partial;

SET mutations_sync = 2;

-- Bare MODIFY starts a mutation that fails on non-convertible values.
CREATE TABLE t_fuse_modify_update_bare
(
    id UInt32,
    amount String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_fuse_modify_update_bare VALUES (1, '100'), (2, 'bad');

ALTER TABLE t_fuse_modify_update_bare MODIFY COLUMN amount UInt64; -- { serverError UNFINISHED }

DROP TABLE t_fuse_modify_update_bare;

CREATE TABLE t_fuse_modify_update
(
    id UInt32,
    amount String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_fuse_modify_update VALUES (1, '100'), (2, '250'), (3, 'bad');

ALTER TABLE t_fuse_modify_update
    MODIFY COLUMN amount UInt64,
    UPDATE amount = toUInt64OrZero(amount) WHERE 1;

SELECT id, amount, toTypeName(amount) FROM t_fuse_modify_update ORDER BY id;

-- Reverse clause order should fuse the same way.
CREATE TABLE t_fuse_modify_update_order
(
    id UInt32,
    amount String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_fuse_modify_update_order VALUES (1, '7'), (2, 'x');

ALTER TABLE t_fuse_modify_update_order
    UPDATE amount = toUInt64OrZero(amount) WHERE 1,
    MODIFY COLUMN amount UInt64;

SELECT id, amount FROM t_fuse_modify_update_order ORDER BY id;

-- Expression may use other columns.
CREATE TABLE t_fuse_modify_update_partial
(
    id UInt32,
    price String,
    qty UInt32
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_fuse_modify_update_partial VALUES (1, '10', 3), (2, 'nope', 5);

ALTER TABLE t_fuse_modify_update_partial
    MODIFY COLUMN price UInt64,
    UPDATE price = toUInt64OrZero(price) * qty WHERE 1;

SELECT id, price FROM t_fuse_modify_update_partial ORDER BY id;

DROP TABLE t_fuse_modify_update;
DROP TABLE t_fuse_modify_update_order;
DROP TABLE t_fuse_modify_update_partial;

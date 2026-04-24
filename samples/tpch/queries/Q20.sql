-- Q20: Potential Part Promotion
-- STATUS: PASS (IN subquery implemented)
SELECT s_name, s_address
FROM supplier, nation
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp
    WHERE ps_partkey IN (
        SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
    )
)
AND s_nationkey = n_nationkey
AND n_name = 'CANADA'
ORDER BY s_name

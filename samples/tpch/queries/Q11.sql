-- Q11: Important Stock Identification
-- STATUS: PARTIAL - HAVING with threshold
SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > 100000
ORDER BY value DESC

-- Q07: Volume Shipping
-- STATUS: PARTIAL - EXTRACT supported
SELECT n1n2_pair, l_year, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem, orders, customer, supplier, nation n1, nation n2
WHERE s_suppkey = l_suppkey
  AND o_orderkey = l_orderkey
  AND c_custkey = o_custkey
  AND s_nationkey = n1.n_nationkey
  AND c_nationkey = n2.n_nationkey
  AND l_shipdate >= '1995-01-01'
  AND l_shipdate <= '1996-12-31'
GROUP BY n1n2_pair, l_year
ORDER BY n1n2_pair, l_year

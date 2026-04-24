-- Q22: Global Sales Opportunity
-- STATUS: PARTIAL - subquery with SUBSTRING
SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
FROM (
    SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, c_acctbal
    FROM customer
    WHERE c_acctbal > 0.00
      AND NOT EXISTS (
          SELECT * FROM orders WHERE o_custkey = c_custkey
      )
) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode

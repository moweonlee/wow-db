-- 02: Top Pages by Pageview Count
-- No behavioral routing triggered (no session_id reference)
SELECT
    page_url,
    COUNT(*) AS views,
    COUNT(DISTINCT visitor_id) AS unique_visitors
FROM events
WHERE event_type = 'pageview'
GROUP BY page_url
ORDER BY views DESC
LIMIT 10

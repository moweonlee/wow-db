-- 01: Session Summary
-- Session count, avg duration, total events
SELECT
    COUNT(*) AS session_count,
    AVG(duration_ms) / 1000.0 AS avg_duration_s,
    SUM(event_count) AS total_events,
    SUM(pageview_count) AS total_pageviews,
    country
FROM sessions
GROUP BY country
ORDER BY session_count DESC

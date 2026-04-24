-- 04: Returning Users by Country
-- Users with more than 1 session (returning visitors)
SELECT
    country,
    COUNT(*) AS returning_sessions,
    AVG(event_count) AS avg_events_per_session
FROM sessions
WHERE visitor_id IN (
    SELECT visitor_id FROM sessions
    GROUP BY visitor_id
    HAVING COUNT(*) > 1
)
GROUP BY country
ORDER BY returning_sessions DESC

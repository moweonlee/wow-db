-- 05: Behavioral Routing Demo
-- ROUTING: This query references session_id on 'events' table.
-- WOW-DB's behavioral router detects the session_id column and
-- transparently rewrites this query to read from the 'sessions' BT.
-- The user sees session-level aggregates even though they wrote "FROM events".

-- ROUTING: behavioral router redirects FROM events → sessions BT
SELECT
    session_id,
    COUNT(*) AS event_count
FROM events
GROUP BY session_id
ORDER BY event_count DESC
LIMIT 10

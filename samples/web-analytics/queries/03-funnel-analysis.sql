-- 03: Entry-to-Exit Funnel
-- How many sessions start on /home and end on /checkout
SELECT
    entry_page,
    exit_page,
    COUNT(*) AS sessions,
    AVG(duration_ms) / 1000.0 AS avg_duration_s
FROM sessions
WHERE entry_page IN ('/home', '/product', '/pricing')
GROUP BY entry_page, exit_page
ORDER BY sessions DESC
LIMIT 20

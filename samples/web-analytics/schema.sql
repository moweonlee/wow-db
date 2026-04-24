-- WOW-DB Web Analytics Sample Schema
-- Two-table Dual-Layout: events (raw) + sessions (pre-aggregated)

CREATE DATABASE IF NOT EXISTS web_analytics;
USE web_analytics;

-- Raw event stream (Event Cube)
CREATE CUBE IF NOT EXISTS events (
    event_id     VARCHAR(36)  NOT NULL,
    session_id   VARCHAR(36)  NOT NULL,
    visitor_id   VARCHAR(36)  NOT NULL,
    user_id      VARCHAR(64),
    event_type   VARCHAR(32)  NOT NULL,
    page_url     VARCHAR(512) NOT NULL,
    referrer_url VARCHAR(512),
    device_type  VARCHAR(16),
    browser      VARCHAR(32),
    os           VARCHAR(32),
    country      VARCHAR(2),
    ts           BIGINT       NOT NULL
);

-- Session MV declaration (behavioral routing target)
CREATE SESSION MATERIALIZED VIEW sessions ON events KEY visitor_id TIMEOUT 30;

-- Session summary table (Behavioral Table)
CREATE CUBE IF NOT EXISTS sessions (
    session_id      VARCHAR(36)  NOT NULL,
    visitor_id      VARCHAR(36)  NOT NULL,
    user_id         VARCHAR(64),
    session_start   BIGINT       NOT NULL,
    session_end     BIGINT       NOT NULL,
    duration_ms     BIGINT       NOT NULL,
    event_count     INT          NOT NULL,
    pageview_count  INT          NOT NULL,
    entry_page      VARCHAR(512) NOT NULL,
    exit_page       VARCHAR(512) NOT NULL,
    device_type     VARCHAR(16),
    browser         VARCHAR(32),
    os              VARCHAR(32),
    country         VARCHAR(2)
);

-- WOW-DB Sample Teardown
-- Drops all sample tables created by TPC-H and web-analytics loaders

DROP CUBE IF EXISTS lineitem;
DROP CUBE IF EXISTS orders;
DROP CUBE IF EXISTS partsupp;
DROP CUBE IF EXISTS part;
DROP CUBE IF EXISTS customer;
DROP CUBE IF EXISTS supplier;
DROP CUBE IF EXISTS nation;
DROP CUBE IF EXISTS region;
DROP CUBE IF EXISTS events;
DROP CUBE IF EXISTS sessions;
DROP CUBE IF EXISTS pageviews;

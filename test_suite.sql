-- WOW-DB 통합 테스트 스위트
-- 10개 테이블, 100+ 쿼리

-- ═══════════════════════════════════════════════════════════════
-- 1. 데이터베이스 및 테이블 설정
-- ═══════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- [T1] 페이지 이벤트
CREATE CUBE IF NOT EXISTS page_events (
  event_time  DATETIME    NOT NULL,
  user_id     VARCHAR(64) NOT NULL,
  session_id  VARCHAR(64),
  event_name  VARCHAR(64) NOT NULL,
  page_url    VARCHAR(512),
  referrer    VARCHAR(512),
  device_type VARCHAR(32),
  country     VARCHAR(8),
  props       JSON
) ORDER BY (user_id, event_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 8;

-- [T2] 구매 이벤트
CREATE CUBE IF NOT EXISTS purchase_events (
  event_time   DATETIME    NOT NULL,
  user_id      VARCHAR(64) NOT NULL,
  order_id     VARCHAR(64) NOT NULL,
  product_id   VARCHAR(64),
  product_name VARCHAR(256),
  category     VARCHAR(64),
  amount       DECIMAL,
  quantity     INT,
  country      VARCHAR(8)
) ORDER BY (user_id, event_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 8;

-- [T3] 클릭 이벤트
CREATE CUBE IF NOT EXISTS click_events (
  event_time  DATETIME    NOT NULL,
  user_id     VARCHAR(64) NOT NULL,
  element_id  VARCHAR(128),
  element_type VARCHAR(32),
  page_url    VARCHAR(512),
  x_pos       INT,
  y_pos       INT
) ORDER BY (user_id, event_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 4;

-- [T4] 에러 이벤트
CREATE CUBE IF NOT EXISTS error_events (
  event_time  DATETIME    NOT NULL,
  user_id     VARCHAR(64),
  error_code  VARCHAR(32) NOT NULL,
  error_msg   VARCHAR(512),
  page_url    VARCHAR(512),
  stack_trace TEXT,
  severity    VARCHAR(16)
) ORDER BY (error_code, event_time)
  DISTRIBUTED BY HASH(error_code) BUCKETS 4;

-- [T5] 사용자 프로필
CREATE CUBE IF NOT EXISTS user_profiles (
  user_id      VARCHAR(64) NOT NULL,
  email        VARCHAR(256),
  signup_date  DATETIME,
  country      VARCHAR(8),
  plan         VARCHAR(32),
  age_group    VARCHAR(16),
  is_premium   BOOLEAN
) ORDER BY (user_id)
  DISTRIBUTED BY HASH(user_id) BUCKETS 4;

-- [T6] 상품 카탈로그
CREATE CUBE IF NOT EXISTS products (
  product_id   VARCHAR(64) NOT NULL,
  product_name VARCHAR(256) NOT NULL,
  category     VARCHAR(64),
  sub_category VARCHAR(64),
  price        DECIMAL,
  stock        INT,
  created_at   DATETIME
) ORDER BY (category, product_id)
  DISTRIBUTED BY HASH(product_id) BUCKETS 4;

-- [T7] 세션 집계
CREATE CUBE IF NOT EXISTS session_stats (
  session_id      VARCHAR(64) NOT NULL,
  user_id         VARCHAR(64) NOT NULL,
  start_time      DATETIME,
  end_time        DATETIME,
  duration_sec    INT,
  page_view_count INT,
  click_count     INT,
  converted       BOOLEAN,
  device_type     VARCHAR(32),
  country         VARCHAR(8)
) ORDER BY (user_id, start_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 4;

-- [T8] AB 테스트
CREATE CUBE IF NOT EXISTS ab_test_events (
  event_time   DATETIME    NOT NULL,
  user_id      VARCHAR(64) NOT NULL,
  experiment   VARCHAR(64) NOT NULL,
  variant      VARCHAR(32) NOT NULL,
  event_name   VARCHAR(64),
  converted    BOOLEAN,
  revenue      DECIMAL
) ORDER BY (experiment, user_id, event_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 4;

-- [T9] 검색 이벤트
CREATE CUBE IF NOT EXISTS search_events (
  event_time    DATETIME    NOT NULL,
  user_id       VARCHAR(64),
  query_text    VARCHAR(512) NOT NULL,
  result_count  INT,
  clicked_rank  INT,
  category      VARCHAR(64)
) ORDER BY (event_time)
  DISTRIBUTED BY HASH(query_text) BUCKETS 4;

-- [T10] 일별 집계 메트릭
CREATE CUBE IF NOT EXISTS daily_metrics (
  report_date  DATE        NOT NULL,
  country      VARCHAR(8),
  device_type  VARCHAR(32),
  page_views   BIGINT,
  unique_users BIGINT,
  sessions     BIGINT,
  revenue      DECIMAL,
  conversions  BIGINT
) ORDER BY (report_date, country)
  DISTRIBUTED BY HASH(report_date) BUCKETS 4;

-- ═══════════════════════════════════════════════════════════════
-- 2. 스키마 확인
-- ═══════════════════════════════════════════════════════════════

SHOW TABLES;
DESCRIBE page_events;
DESCRIBE purchase_events;
DESCRIBE user_profiles;

-- ═══════════════════════════════════════════════════════════════
-- 3. INSERT — 페이지 이벤트 (30건)
-- ═══════════════════════════════════════════════════════════════

INSERT INTO page_events (event_time, user_id, session_id, event_name, page_url, referrer, device_type, country, props)
VALUES
  ('2024-01-15 09:00:01', 'u001', 's001', 'page_view', '/home',    'https://google.com', 'desktop', 'KR', '{"campaign":"winter_sale"}'),
  ('2024-01-15 09:01:30', 'u001', 's001', 'page_view', '/products','',                   'desktop', 'KR', '{"scroll_depth":75}'),
  ('2024-01-15 09:03:45', 'u001', 's001', 'page_view', '/cart',    '',                   'desktop', 'KR', '{"items":3}'),
  ('2024-01-15 10:15:00', 'u002', 's002', 'page_view', '/home',    'https://naver.com',  'mobile',  'KR', '{"campaign":"sns"}'),
  ('2024-01-15 10:16:20', 'u002', 's002', 'page_view', '/sale',    '',                   'mobile',  'KR', '{"tag":"flash_sale"}'),
  ('2024-01-15 11:00:00', 'u003', 's003', 'page_view', '/home',    '',                   'tablet',  'US', '{}'),
  ('2024-01-15 11:02:10', 'u003', 's003', 'page_view', '/about',   '',                   'tablet',  'US', '{}'),
  ('2024-01-15 12:00:00', 'u004', 's004', 'page_view', '/home',    'https://facebook.com','desktop','JP', '{"lang":"ja"}'),
  ('2024-01-15 12:05:00', 'u004', 's004', 'page_view', '/products','',                   'desktop', 'JP', '{"sort":"price_asc"}'),
  ('2024-01-15 13:00:00', 'u005', 's005', 'page_view', '/home',    '',                   'mobile',  'KR', '{}'),
  ('2024-01-15 13:01:00', 'u005', 's005', 'page_view', '/blog',    '',                   'mobile',  'KR', '{"post_id":"123"}'),
  ('2024-01-15 13:05:00', 'u005', 's005', 'page_view', '/products','',                   'mobile',  'KR', '{}'),
  ('2024-01-15 14:00:00', 'u006', 's006', 'page_view', '/home',    'https://twitter.com','desktop', 'US', '{}'),
  ('2024-01-15 14:30:00', 'u007', 's007', 'page_view', '/home',    '',                   'mobile',  'CN', '{}'),
  ('2024-01-15 15:00:00', 'u008', 's008', 'page_view', '/home',    '',                   'desktop', 'KR', '{"returning":true}'),
  ('2024-01-15 15:01:00', 'u008', 's008', 'page_view', '/checkout','',                   'desktop', 'KR', '{"step":1}'),
  ('2024-01-15 15:02:00', 'u008', 's008', 'page_view', '/checkout','',                   'desktop', 'KR', '{"step":2}'),
  ('2024-01-15 16:00:00', 'u009', 's009', 'page_view', '/home',    '',                   'mobile',  'KR', '{}'),
  ('2024-01-15 16:30:00', 'u010', 's010', 'page_view', '/home',    '',                   'desktop', 'US', '{}'),
  ('2024-01-15 17:00:00', 'u001', 's011', 'page_view', '/home',    '',                   'mobile',  'KR', '{"returning":true}'),
  ('2024-01-15 17:01:00', 'u001', 's011', 'page_view', '/products','',                   'mobile',  'KR', '{}'),
  ('2024-01-15 17:05:00', 'u001', 's011', 'page_view', '/cart',    '',                   'mobile',  'KR', '{}'),
  ('2024-01-15 17:10:00', 'u001', 's011', 'page_view', '/checkout','',                   'mobile',  'KR', '{"step":1}'),
  ('2024-01-15 18:00:00', 'u011', 's012', 'page_view', '/home',    '',                   'desktop', 'KR', '{}'),
  ('2024-01-15 18:01:00', 'u011', 's012', 'page_view', '/products','',                   'desktop', 'KR', '{}'),
  ('2024-01-15 19:00:00', 'u012', 's013', 'page_view', '/home',    '',                   'mobile',  'DE', '{"lang":"de"}'),
  ('2024-01-15 20:00:00', 'u013', 's014', 'page_view', '/home',    '',                   'desktop', 'KR', '{}'),
  ('2024-01-15 20:30:00', 'u014', 's015', 'page_view', '/home',    '',                   'tablet',  'JP', '{}'),
  ('2024-01-15 21:00:00', 'u015', 's016', 'page_view', '/home',    '',                   'mobile',  'KR', '{}'),
  ('2024-01-15 21:30:00', 'u015', 's016', 'page_view', '/blog',    '',                   'mobile',  'KR', '{"post_id":"456"}');

-- ═══════════════════════════════════════════════════════════════
-- 4. INSERT — 구매 이벤트 (20건)
-- ═══════════════════════════════════════════════════════════════

INSERT INTO purchase_events (event_time, user_id, order_id, product_id, product_name, category, amount, quantity, country)
VALUES
  ('2024-01-15 09:05:00', 'u001', 'ord001', 'p001', '노트북 Pro 15',  'electronics', 1299000, 1, 'KR'),
  ('2024-01-15 10:20:00', 'u002', 'ord002', 'p002', '무선 이어폰',    'electronics',  89000, 1, 'KR'),
  ('2024-01-15 11:30:00', 'u003', 'ord003', 'p003', '스마트워치',     'electronics', 299000, 1, 'US'),
  ('2024-01-15 12:10:00', 'u004', 'ord004', 'p004', '책상 램프',      'furniture',    45000, 2, 'JP'),
  ('2024-01-15 13:15:00', 'u005', 'ord005', 'p001', '노트북 Pro 15',  'electronics', 1299000, 1, 'KR'),
  ('2024-01-15 14:00:00', 'u006', 'ord006', 'p005', '러닝화',         'sports',       79000, 1, 'US'),
  ('2024-01-15 14:30:00', 'u007', 'ord007', 'p006', '요가 매트',      'sports',       35000, 1, 'CN'),
  ('2024-01-15 15:10:00', 'u008', 'ord008', 'p007', '커피 메이커',    'kitchen',     129000, 1, 'KR'),
  ('2024-01-15 15:30:00', 'u001', 'ord009', 'p008', '마우스 패드',    'accessories',  25000, 3, 'KR'),
  ('2024-01-15 16:00:00', 'u009', 'ord010', 'p002', '무선 이어폰',    'electronics',  89000, 2, 'KR'),
  ('2024-01-15 16:20:00', 'u010', 'ord011', 'p009', '백팩',           'fashion',      65000, 1, 'US'),
  ('2024-01-15 17:15:00', 'u002', 'ord012', 'p010', '스탠딩 책상',    'furniture',   450000, 1, 'KR'),
  ('2024-01-15 17:30:00', 'u011', 'ord013', 'p003', '스마트워치',     'electronics', 299000, 1, 'KR'),
  ('2024-01-15 18:00:00', 'u012', 'ord014', 'p004', '책상 램프',      'furniture',    45000, 1, 'DE'),
  ('2024-01-15 18:30:00', 'u003', 'ord015', 'p005', '러닝화',         'sports',       79000, 1, 'US'),
  ('2024-01-15 19:00:00', 'u013', 'ord016', 'p006', '요가 매트',      'sports',       35000, 2, 'KR'),
  ('2024-01-15 19:30:00', 'u014', 'ord017', 'p001', '노트북 Pro 15',  'electronics', 1299000, 1, 'JP'),
  ('2024-01-15 20:00:00', 'u015', 'ord018', 'p007', '커피 메이커',    'kitchen',     129000, 1, 'KR'),
  ('2024-01-15 20:30:00', 'u004', 'ord019', 'p008', '마우스 패드',    'accessories',  25000, 2, 'JP'),
  ('2024-01-15 21:00:00', 'u005', 'ord020', 'p009', '백팩',           'fashion',      65000, 1, 'KR');

-- ═══════════════════════════════════════════════════════════════
-- 5. INSERT — 사용자 프로필, 상품, 세션, AB테스트, 검색, 일별 메트릭
-- ═══════════════════════════════════════════════════════════════

INSERT INTO user_profiles (user_id, email, signup_date, country, plan, age_group, is_premium)
VALUES
  ('u001', 'alice@example.com',  '2023-01-10 00:00:00', 'KR', 'premium', '25-34', 1),
  ('u002', 'bob@example.com',    '2023-03-15 00:00:00', 'KR', 'free',    '18-24', 0),
  ('u003', 'carol@example.com',  '2022-11-20 00:00:00', 'US', 'premium', '35-44', 1),
  ('u004', 'dave@example.com',   '2023-06-01 00:00:00', 'JP', 'free',    '25-34', 0),
  ('u005', 'eve@example.com',    '2023-08-05 00:00:00', 'KR', 'premium', '18-24', 1),
  ('u006', 'frank@example.com',  '2022-09-12 00:00:00', 'US', 'free',    '45-54', 0),
  ('u007', 'grace@example.com',  '2023-02-28 00:00:00', 'CN', 'free',    '25-34', 0),
  ('u008', 'henry@example.com',  '2022-07-04 00:00:00', 'KR', 'premium', '35-44', 1),
  ('u009', 'iris@example.com',   '2023-10-15 00:00:00', 'KR', 'free',    '18-24', 0),
  ('u010', 'jack@example.com',   '2023-05-20 00:00:00', 'US', 'premium', '25-34', 1);

INSERT INTO products (product_id, product_name, category, sub_category, price, stock, created_at)
VALUES
  ('p001', '노트북 Pro 15',  'electronics', 'laptop',     1299000, 50,  '2023-01-01 00:00:00'),
  ('p002', '무선 이어폰',    'electronics', 'audio',       89000, 200, '2023-02-01 00:00:00'),
  ('p003', '스마트워치',     'electronics', 'wearable',   299000, 80,  '2023-03-01 00:00:00'),
  ('p004', '책상 램프',      'furniture',   'lighting',    45000, 150, '2023-04-01 00:00:00'),
  ('p005', '러닝화',         'sports',      'footwear',    79000, 120, '2023-05-01 00:00:00'),
  ('p006', '요가 매트',      'sports',      'yoga',        35000, 300, '2023-06-01 00:00:00'),
  ('p007', '커피 메이커',    'kitchen',     'appliance',  129000, 60,  '2023-07-01 00:00:00'),
  ('p008', '마우스 패드',    'accessories', 'computer',    25000, 500, '2023-08-01 00:00:00'),
  ('p009', '백팩',           'fashion',     'bag',         65000, 100, '2023-09-01 00:00:00'),
  ('p010', '스탠딩 책상',    'furniture',   'desk',       450000, 30,  '2023-10-01 00:00:00');

INSERT INTO session_stats (session_id, user_id, start_time, end_time, duration_sec, page_view_count, click_count, converted, device_type, country)
VALUES
  ('s001', 'u001', '2024-01-15 09:00:00', '2024-01-15 09:10:00', 600,  3, 8,  1, 'desktop', 'KR'),
  ('s002', 'u002', '2024-01-15 10:15:00', '2024-01-15 10:25:00', 600,  2, 5,  0, 'mobile',  'KR'),
  ('s003', 'u003', '2024-01-15 11:00:00', '2024-01-15 11:08:00', 480,  2, 3,  1, 'tablet',  'US'),
  ('s004', 'u004', '2024-01-15 12:00:00', '2024-01-15 12:15:00', 900,  2, 6,  1, 'desktop', 'JP'),
  ('s005', 'u005', '2024-01-15 13:00:00', '2024-01-15 13:12:00', 720,  3, 4,  0, 'mobile',  'KR'),
  ('s006', 'u006', '2024-01-15 14:00:00', '2024-01-15 14:05:00', 300,  1, 2,  0, 'desktop', 'US'),
  ('s007', 'u007', '2024-01-15 14:30:00', '2024-01-15 14:38:00', 480,  1, 1,  0, 'mobile',  'CN'),
  ('s008', 'u008', '2024-01-15 15:00:00', '2024-01-15 15:20:00', 1200, 3, 12, 1, 'desktop', 'KR'),
  ('s009', 'u009', '2024-01-15 16:00:00', '2024-01-15 16:03:00', 180,  1, 0,  0, 'mobile',  'KR'),
  ('s010', 'u010', '2024-01-15 16:30:00', '2024-01-15 16:45:00', 900,  1, 4,  1, 'desktop', 'US');

INSERT INTO ab_test_events (event_time, user_id, experiment, variant, event_name, converted, revenue)
VALUES
  ('2024-01-15 09:01:00', 'u001', 'checkout_flow_v2', 'control',   'checkout_start', 1, 1299000),
  ('2024-01-15 10:16:00', 'u002', 'checkout_flow_v2', 'treatment', 'checkout_start', 0, 0),
  ('2024-01-15 11:01:00', 'u003', 'checkout_flow_v2', 'control',   'checkout_start', 1,  299000),
  ('2024-01-15 12:01:00', 'u004', 'checkout_flow_v2', 'treatment', 'checkout_start', 1,   90000),
  ('2024-01-15 13:01:00', 'u005', 'checkout_flow_v2', 'control',   'checkout_start', 0,       0),
  ('2024-01-15 14:01:00', 'u006', 'checkout_flow_v2', 'treatment', 'checkout_start', 0,       0),
  ('2024-01-15 15:01:00', 'u008', 'checkout_flow_v2', 'treatment', 'checkout_start', 1,  129000),
  ('2024-01-15 16:01:00', 'u009', 'checkout_flow_v2', 'control',   'checkout_start', 0,       0),
  ('2024-01-15 09:00:00', 'u001', 'homepage_banner',  'control',   'banner_view',    1,       0),
  ('2024-01-15 10:15:00', 'u002', 'homepage_banner',  'treatment', 'banner_view',    0,       0),
  ('2024-01-15 13:00:00', 'u005', 'homepage_banner',  'treatment', 'banner_view',    0,       0),
  ('2024-01-15 14:00:00', 'u006', 'homepage_banner',  'control',   'banner_view',    0,       0);

INSERT INTO search_events (event_time, user_id, query_text, result_count, clicked_rank, category)
VALUES
  ('2024-01-15 09:00:00', 'u001', '노트북 추천',    45, 1, 'electronics'),
  ('2024-01-15 10:00:00', 'u002', '이어폰',         120, 3, 'electronics'),
  ('2024-01-15 11:00:00', 'u003', 'smartwatch',      30, 1, 'electronics'),
  ('2024-01-15 12:00:00', 'u004', '책상',            80, 2, 'furniture'),
  ('2024-01-15 13:00:00', 'u005', '노트북',          45, 1, 'electronics'),
  ('2024-01-15 14:00:00', 'u006', 'running shoes',   60, 4, 'sports'),
  ('2024-01-15 15:00:00', 'u007', '요가',            25, 1, 'sports'),
  ('2024-01-15 16:00:00', 'u008', '커피',            90, 2, 'kitchen'),
  ('2024-01-15 17:00:00', 'u009', '이어폰',         120, 5, 'electronics'),
  ('2024-01-15 18:00:00', 'u010', 'backpack',        55, 1, 'fashion');

INSERT INTO daily_metrics (report_date, country, device_type, page_views, unique_users, sessions, revenue, conversions)
VALUES
  ('2024-01-13', 'KR', 'desktop', 12500, 3200, 4100, 8500000, 85),
  ('2024-01-13', 'KR', 'mobile',  18000, 4500, 5800, 6200000, 62),
  ('2024-01-13', 'US', 'desktop',  8200, 2100, 2700, 5100000, 51),
  ('2024-01-13', 'JP', 'desktop',  4500, 1200, 1500, 3200000, 32),
  ('2024-01-14', 'KR', 'desktop', 13200, 3400, 4300, 9100000, 91),
  ('2024-01-14', 'KR', 'mobile',  19500, 4800, 6100, 6800000, 68),
  ('2024-01-14', 'US', 'desktop',  8800, 2300, 2900, 5500000, 55),
  ('2024-01-14', 'JP', 'desktop',  4800, 1300, 1600, 3500000, 35),
  ('2024-01-15', 'KR', 'desktop', 14100, 3600, 4500, 9800000, 98),
  ('2024-01-15', 'KR', 'mobile',  21000, 5100, 6500, 7200000, 72),
  ('2024-01-15', 'US', 'desktop',  9200, 2400, 3100, 5900000, 59),
  ('2024-01-15', 'JP', 'desktop',  5100, 1400, 1700, 3800000, 38);

-- ═══════════════════════════════════════════════════════════════
-- 6. SELECT 쿼리 테스트
-- ═══════════════════════════════════════════════════════════════

-- [Q01] 기본 전체 스캔
SELECT * FROM user_profiles LIMIT 5;

-- [Q02] COUNT(*)
SELECT COUNT(*) FROM page_events;

-- [Q03] WHERE 조건 (문자열 등호)
SELECT user_id, page_url, device_type FROM page_events WHERE country = 'KR' LIMIT 10;

-- [Q04] WHERE 조건 (숫자 범위)
SELECT product_id, product_name, price FROM products WHERE price > 100000;

-- [Q05] GROUP BY + COUNT (디바이스별 페이지뷰)
SELECT device_type, COUNT(*) AS views FROM page_events GROUP BY device_type;

-- [Q06] GROUP BY + COUNT (국가별 페이지뷰)
SELECT country, COUNT(*) AS views FROM page_events GROUP BY country;

-- [Q07] GROUP BY + SUM (카테고리별 매출)
SELECT category, SUM(amount) AS total_revenue FROM purchase_events GROUP BY category;

-- [Q08] GROUP BY + COUNT + SUM (카테고리별 건수/매출)
SELECT category, COUNT(*) AS orders, SUM(amount) AS revenue FROM purchase_events GROUP BY category;

-- [Q09] GROUP BY + AVG (카테고리별 평균 단가)
SELECT category, AVG(price) AS avg_price FROM products GROUP BY category;

-- [Q10] GROUP BY + MAX/MIN
SELECT category, MAX(price) AS max_price, MIN(price) AS min_price FROM products GROUP BY category;

-- [Q11] ORDER BY DESC
SELECT user_id, amount FROM purchase_events ORDER BY amount DESC LIMIT 5;

-- [Q12] WHERE + GROUP BY (KR 사용자 구매)
SELECT user_id, COUNT(*) AS purchases, SUM(amount) AS total
FROM purchase_events WHERE country = 'KR' GROUP BY user_id;

-- [Q13] LIKE 검색
SELECT product_id, product_name FROM products WHERE product_name LIKE '%노트북%';

-- [Q14] IN 조건
SELECT user_id, plan, country FROM user_profiles WHERE country IN ('KR', 'US');

-- [Q15] BETWEEN
SELECT product_id, product_name, price FROM products WHERE price BETWEEN 50000 AND 200000;

-- [Q16] 프리미엄 사용자 수
SELECT COUNT(*) AS premium_count FROM user_profiles WHERE is_premium = 1;

-- [Q17] 국가별 프리미엄 분포
SELECT country, COUNT(*) AS total, SUM(is_premium) AS premium FROM user_profiles GROUP BY country;

-- [Q18] 일별 매출 (daily_metrics)
SELECT report_date, SUM(revenue) AS daily_revenue FROM daily_metrics GROUP BY report_date;

-- [Q19] 국가별 총 매출 (daily_metrics)
SELECT country, SUM(revenue) AS total_revenue, SUM(conversions) AS total_conversions FROM daily_metrics GROUP BY country;

-- [Q20] 디바이스별 세션 수
SELECT device_type, COUNT(*) AS sessions, AVG(duration_sec) AS avg_duration FROM session_stats GROUP BY device_type;

-- [Q21] 전환된 세션만
SELECT user_id, duration_sec, page_view_count FROM session_stats WHERE converted = 1;

-- [Q22] 검색어 카테고리별 평균 결과 수
SELECT category, COUNT(*) AS searches, AVG(result_count) AS avg_results FROM search_events GROUP BY category;

-- [Q23] AB 테스트 variant별 전환율
SELECT experiment, variant, COUNT(*) AS participants, SUM(converted) AS conversions
FROM ab_test_events GROUP BY experiment, variant;

-- [Q24] AB 테스트 variant별 평균 매출
SELECT experiment, variant, AVG(revenue) AS avg_revenue FROM ab_test_events GROUP BY experiment, variant;

-- [Q25] 사용자별 총 구매금액 상위
SELECT user_id, SUM(amount) AS total_spent, COUNT(*) AS order_count
FROM purchase_events GROUP BY user_id ORDER BY total_spent DESC LIMIT 5;

-- [Q26] 상품별 판매 수량
SELECT product_name, SUM(quantity) AS total_qty, SUM(amount) AS total_revenue
FROM purchase_events GROUP BY product_name ORDER BY total_revenue DESC;

-- [Q27] 사용자별 페이지뷰 수
SELECT user_id, COUNT(*) AS page_views FROM page_events GROUP BY user_id ORDER BY page_views DESC;

-- [Q28] 시간대별 트래픽 (hour 스텁 — event_time에서 추출 불가 → 전체 COUNT)
SELECT COUNT(*) AS total_events FROM page_events;

-- [Q29] 재방문 세션 (duration > 600초)
SELECT session_id, user_id, duration_sec FROM session_stats WHERE duration_sec > 600;

-- [Q30] 검색 클릭률 (clicked_rank IS NOT NULL)
SELECT user_id, query_text, result_count, clicked_rank FROM search_events WHERE clicked_rank IS NOT NULL;

-- ═══════════════════════════════════════════════════════════════
-- 7. 데이터 업데이트 검증 (추가 INSERT → 재집계)
-- ═══════════════════════════════════════════════════════════════

INSERT INTO purchase_events (event_time, user_id, order_id, product_id, product_name, category, amount, quantity, country)
VALUES ('2024-01-16 09:00:00', 'u001', 'ord021', 'p001', '노트북 Pro 15', 'electronics', 1299000, 1, 'KR');

SELECT COUNT(*) AS total_purchases FROM purchase_events;
SELECT user_id, SUM(amount) AS total FROM purchase_events WHERE user_id = 'u001' GROUP BY user_id;

-- ═══════════════════════════════════════════════════════════════
-- 8. NULL 처리 테스트
-- ═══════════════════════════════════════════════════════════════

INSERT INTO error_events (event_time, user_id, error_code, error_msg, severity)
VALUES
  ('2024-01-15 09:00:00', 'u001', 'ERR_404', 'Page not found',    'warning'),
  ('2024-01-15 10:00:00', NULL,   'ERR_500', 'Internal error',     'critical'),
  ('2024-01-15 11:00:00', 'u003', 'ERR_403', 'Forbidden',          'warning'),
  ('2024-01-15 12:00:00', NULL,   'ERR_500', 'DB connection error','critical'),
  ('2024-01-15 13:00:00', 'u005', 'ERR_404', 'Page not found',    'warning');

SELECT error_code, severity, COUNT(*) AS cnt FROM error_events GROUP BY error_code, severity;
SELECT COUNT(*) AS errors_with_user FROM error_events WHERE user_id IS NOT NULL;
SELECT COUNT(*) AS anonymous_errors FROM error_events WHERE user_id IS NULL;

-- ═══════════════════════════════════════════════════════════════
-- 9. 최종 검증
-- ═══════════════════════════════════════════════════════════════

SHOW TABLES;
SELECT COUNT(*) FROM page_events;
SELECT COUNT(*) FROM purchase_events;
SELECT COUNT(*) FROM user_profiles;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM session_stats;
SELECT COUNT(*) FROM ab_test_events;
SELECT COUNT(*) FROM search_events;
SELECT COUNT(*) FROM daily_metrics;
SELECT COUNT(*) FROM error_events;

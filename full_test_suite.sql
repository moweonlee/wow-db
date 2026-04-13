-- ════════════════════════════════════════════════════════════════════
-- WOW-DB 전체 기능 테스트 스위트 v2
-- 10 Cubes, 100+ Queries, SRS 전체 커버리지
-- ════════════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────────────
-- [0] 데이터베이스 초기화
-- ────────────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- ────────────────────────────────────────────────────────────────────
-- [0b] 테이블 초기화 (멱등 실행 보장)
-- ────────────────────────────────────────────────────────────────────
DELETE FROM page_events;
DELETE FROM purchase_events;
DELETE FROM user_profiles;
DELETE FROM products;
DELETE FROM session_stats;
DELETE FROM ab_test_events;
DELETE FROM search_events;
DELETE FROM error_events;
DELETE FROM daily_metrics;
DELETE FROM click_events;

-- ────────────────────────────────────────────────────────────────────
-- [1] CREATE CUBE x10 (DDL 테스트)
-- ────────────────────────────────────────────────────────────────────

-- [T01] 페이지 이벤트 (핵심 웹 분석 테이블)
CREATE CUBE IF NOT EXISTS page_events (
  event_time   DATETIME     NOT NULL,
  user_id      VARCHAR(64)  NOT NULL,
  session_id   VARCHAR(64),
  event_name   VARCHAR(64)  NOT NULL,
  page_url     VARCHAR(512),
  referrer     VARCHAR(512),
  device_type  VARCHAR(32),
  country      VARCHAR(8),
  props        JSON
) ORDER BY (user_id, event_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 8;

-- [T02] 구매 이벤트
CREATE CUBE IF NOT EXISTS purchase_events (
  event_time   DATETIME     NOT NULL,
  user_id      VARCHAR(64)  NOT NULL,
  order_id     VARCHAR(64)  NOT NULL,
  product_id   VARCHAR(64),
  product_name VARCHAR(256),
  category     VARCHAR(64),
  amount       DECIMAL,
  quantity     INT,
  country      VARCHAR(8),
  payment_type VARCHAR(32)
) ORDER BY (user_id, event_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 8;

-- [T03] 클릭 이벤트
CREATE CUBE IF NOT EXISTS click_events (
  event_time   DATETIME     NOT NULL,
  user_id      VARCHAR(64)  NOT NULL,
  session_id   VARCHAR(64),
  element_id   VARCHAR(128),
  element_type VARCHAR(32),
  page_url     VARCHAR(512),
  x_pos        INT,
  y_pos        INT,
  country      VARCHAR(8)
) ORDER BY (user_id, event_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 4;

-- [T04] 에러 이벤트 (NULL 처리 테스트)
CREATE CUBE IF NOT EXISTS error_events (
  event_time   DATETIME     NOT NULL,
  user_id      VARCHAR(64),
  error_code   VARCHAR(32)  NOT NULL,
  error_msg    VARCHAR(512),
  page_url     VARCHAR(512),
  severity     VARCHAR(16),
  is_resolved  BOOLEAN
) ORDER BY (error_code, event_time)
  DISTRIBUTED BY HASH(error_code) BUCKETS 4;

-- [T05] 사용자 프로필
CREATE CUBE IF NOT EXISTS user_profiles (
  user_id      VARCHAR(64)  NOT NULL,
  email        VARCHAR(256),
  signup_date  DATETIME,
  country      VARCHAR(8),
  plan         VARCHAR(32),
  age_group    VARCHAR(16),
  is_premium   BOOLEAN,
  lifetime_value DECIMAL
) ORDER BY (user_id)
  DISTRIBUTED BY HASH(user_id) BUCKETS 4;

-- [T06] 상품 카탈로그
CREATE CUBE IF NOT EXISTS products (
  product_id   VARCHAR(64)  NOT NULL,
  product_name VARCHAR(256) NOT NULL,
  category     VARCHAR(64),
  sub_category VARCHAR(64),
  price        DECIMAL,
  cost         DECIMAL,
  stock        INT,
  rating       DECIMAL,
  created_at   DATETIME
) ORDER BY (category, product_id)
  DISTRIBUTED BY HASH(product_id) BUCKETS 4;

-- [T07] 세션 집계
CREATE CUBE IF NOT EXISTS session_stats (
  session_id       VARCHAR(64) NOT NULL,
  user_id          VARCHAR(64) NOT NULL,
  start_time       DATETIME,
  end_time         DATETIME,
  duration_sec     INT,
  page_view_count  INT,
  click_count      INT,
  converted        BOOLEAN,
  device_type      VARCHAR(32),
  country          VARCHAR(8),
  bounce           BOOLEAN
) ORDER BY (user_id, start_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 4;

-- [T08] A/B 테스트 이벤트
CREATE CUBE IF NOT EXISTS ab_test_events (
  event_time   DATETIME     NOT NULL,
  user_id      VARCHAR(64)  NOT NULL,
  experiment   VARCHAR(64)  NOT NULL,
  variant      VARCHAR(32)  NOT NULL,
  event_name   VARCHAR(64),
  converted    BOOLEAN,
  revenue      DECIMAL
) ORDER BY (experiment, user_id, event_time)
  DISTRIBUTED BY HASH(user_id) BUCKETS 4;

-- [T09] 검색 이벤트
CREATE CUBE IF NOT EXISTS search_events (
  event_time    DATETIME     NOT NULL,
  user_id       VARCHAR(64),
  query_text    VARCHAR(512) NOT NULL,
  result_count  INT,
  clicked_rank  INT,
  category      VARCHAR(64),
  converted     BOOLEAN
) ORDER BY (event_time)
  DISTRIBUTED BY HASH(query_text) BUCKETS 4;

-- [T10] 일별 집계 메트릭 (Pre-aggregation)
CREATE CUBE IF NOT EXISTS daily_metrics (
  report_date  DATE         NOT NULL,
  country      VARCHAR(8),
  device_type  VARCHAR(32),
  page_views   BIGINT,
  unique_users BIGINT,
  sessions     BIGINT,
  revenue      DECIMAL,
  conversions  BIGINT,
  avg_session_sec INT
) ORDER BY (report_date, country)
  DISTRIBUTED BY HASH(report_date) BUCKETS 4;

-- ────────────────────────────────────────────────────────────────────
-- [2] 스키마 확인 (DDL 검증)
-- ────────────────────────────────────────────────────────────────────
SHOW TABLES;
SHOW CUBES;
DESCRIBE page_events;
DESCRIBE products;
DESCRIBE session_stats;

-- ────────────────────────────────────────────────────────────────────
-- [3] INSERT - page_events (30건, 다양한 JSON props)
-- ────────────────────────────────────────────────────────────────────
INSERT INTO page_events (event_time, user_id, session_id, event_name, page_url, referrer, device_type, country, props)
VALUES
  ('2024-01-15 09:00:01','u001','s001','page_view','/home','https://google.com','desktop','KR','{"campaign":"winter_sale","version":2}'),
  ('2024-01-15 09:01:30','u001','s001','page_view','/products','','desktop','KR','{"scroll_depth":75,"items_seen":12}'),
  ('2024-01-15 09:03:45','u001','s001','page_view','/cart','','desktop','KR','{"items":3,"total":45000}'),
  ('2024-01-15 09:05:00','u001','s001','click','#buy-btn','','desktop','KR','{"element":"purchase_btn"}'),
  ('2024-01-15 10:15:00','u002','s002','page_view','/home','https://naver.com','mobile','KR','{"campaign":"sns","ab_variant":"B"}'),
  ('2024-01-15 10:16:20','u002','s002','page_view','/sale','','mobile','KR','{"tag":"flash_sale","discount":0.3}'),
  ('2024-01-15 11:00:00','u003','s003','page_view','/home','','tablet','US','{}'),
  ('2024-01-15 11:02:10','u003','s003','page_view','/about','','tablet','US','{}'),
  ('2024-01-15 11:05:00','u003','s003','page_view','/products','','tablet','US','{"filter":"electronics"}'),
  ('2024-01-15 12:00:00','u004','s004','page_view','/home','https://facebook.com','desktop','JP','{"lang":"ja"}'),
  ('2024-01-15 12:05:00','u004','s004','page_view','/products','','desktop','JP','{"sort":"price_asc"}'),
  ('2024-01-15 13:00:00','u005','s005','page_view','/home','','mobile','KR','{}'),
  ('2024-01-15 13:01:00','u005','s005','page_view','/blog','','mobile','KR','{"post_id":"article_123","read_time":180}'),
  ('2024-01-15 13:05:00','u005','s005','page_view','/products','','mobile','KR','{}'),
  ('2024-01-15 14:00:00','u006','s006','page_view','/home','https://twitter.com','desktop','US','{"referral_code":"TW2024"}'),
  ('2024-01-15 14:30:00','u007','s007','page_view','/home','','mobile','CN','{}'),
  ('2024-01-15 15:00:00','u008','s008','page_view','/home','','desktop','KR','{"returning":true,"visit_count":5}'),
  ('2024-01-15 15:01:00','u008','s008','page_view','/checkout','','desktop','KR','{"step":1,"cart_value":129000}'),
  ('2024-01-15 15:02:00','u008','s008','page_view','/checkout','','desktop','KR','{"step":2,"payment":"card"}'),
  ('2024-01-15 16:00:00','u009','s009','page_view','/home','','mobile','KR','{}'),
  ('2024-01-15 16:30:00','u010','s010','page_view','/home','','desktop','US','{"campaign":"email","source":"newsletter"}'),
  ('2024-01-15 17:00:00','u001','s011','page_view','/home','','mobile','KR','{"returning":true}'),
  ('2024-01-15 17:01:00','u001','s011','page_view','/products','','mobile','KR','{}'),
  ('2024-01-15 17:05:00','u001','s011','page_view','/cart','','mobile','KR','{}'),
  ('2024-01-15 17:10:00','u001','s011','page_view','/checkout','','mobile','KR','{"step":1}'),
  ('2024-01-15 18:00:00','u011','s012','page_view','/home','','desktop','KR','{}'),
  ('2024-01-15 18:01:00','u011','s012','page_view','/products','','desktop','KR','{"new_arrivals":true}'),
  ('2024-01-15 19:00:00','u012','s013','page_view','/home','','mobile','DE','{"lang":"de"}'),
  ('2024-01-15 20:00:00','u013','s014','page_view','/home','','desktop','KR','{}'),
  ('2024-01-15 21:00:00','u015','s016','page_view','/home','','mobile','KR','{}');

-- ────────────────────────────────────────────────────────────────────
-- [4] INSERT - purchase_events (25건, JSON props)
-- ────────────────────────────────────────────────────────────────────
INSERT INTO purchase_events (event_time, user_id, order_id, product_id, product_name, category, amount, quantity, country, payment_type)
VALUES
  ('2024-01-15 09:05:00','u001','ord001','p001','노트북 Pro 15','electronics',1299000,1,'KR','card'),
  ('2024-01-15 10:20:00','u002','ord002','p002','무선 이어폰','electronics',89000,1,'KR','card'),
  ('2024-01-15 11:30:00','u003','ord003','p003','스마트워치','electronics',299000,1,'US','paypal'),
  ('2024-01-15 12:10:00','u004','ord004','p004','책상 램프','furniture',45000,2,'JP','card'),
  ('2024-01-15 13:15:00','u005','ord005','p001','노트북 Pro 15','electronics',1299000,1,'KR','card'),
  ('2024-01-15 14:00:00','u006','ord006','p005','러닝화','sports',79000,1,'US','paypal'),
  ('2024-01-15 14:30:00','u007','ord007','p006','요가 매트','sports',35000,1,'CN','alipay'),
  ('2024-01-15 15:10:00','u008','ord008','p007','커피 메이커','kitchen',129000,1,'KR','card'),
  ('2024-01-15 15:30:00','u001','ord009','p008','마우스 패드','accessories',25000,3,'KR','card'),
  ('2024-01-15 16:00:00','u009','ord010','p002','무선 이어폰','electronics',89000,2,'KR','mobile_pay'),
  ('2024-01-15 16:20:00','u010','ord011','p009','백팩','fashion',65000,1,'US','card'),
  ('2024-01-15 17:15:00','u002','ord012','p010','스탠딩 책상','furniture',450000,1,'KR','card'),
  ('2024-01-15 17:30:00','u011','ord013','p003','스마트워치','electronics',299000,1,'KR','card'),
  ('2024-01-15 18:00:00','u012','ord014','p004','책상 램프','furniture',45000,1,'DE','card'),
  ('2024-01-15 18:30:00','u003','ord015','p005','러닝화','sports',79000,1,'US','paypal'),
  ('2024-01-15 19:00:00','u013','ord016','p006','요가 매트','sports',35000,2,'KR','mobile_pay'),
  ('2024-01-15 19:30:00','u014','ord017','p001','노트북 Pro 15','electronics',1299000,1,'JP','card'),
  ('2024-01-15 20:00:00','u015','ord018','p007','커피 메이커','kitchen',129000,1,'KR','card'),
  ('2024-01-15 20:30:00','u004','ord019','p008','마우스 패드','accessories',25000,2,'JP','card'),
  ('2024-01-15 21:00:00','u005','ord020','p009','백팩','fashion',65000,1,'KR','card'),
  ('2024-01-16 09:00:00','u001','ord021','p001','노트북 Pro 15','electronics',1299000,1,'KR','card'),
  ('2024-01-16 10:00:00','u003','ord022','p003','스마트워치','electronics',299000,1,'US','paypal'),
  ('2024-01-16 11:00:00','u005','ord023','p002','무선 이어폰','electronics',89000,2,'KR','card'),
  ('2024-01-16 12:00:00','u008','ord024','p010','스탠딩 책상','furniture',450000,1,'KR','card'),
  ('2024-01-16 13:00:00','u002','ord025','p005','러닝화','sports',79000,1,'KR','mobile_pay');

-- ────────────────────────────────────────────────────────────────────
-- [5] INSERT - user_profiles, products
-- ────────────────────────────────────────────────────────────────────
INSERT INTO user_profiles (user_id, email, signup_date, country, plan, age_group, is_premium, lifetime_value)
VALUES
  ('u001','alice@example.com','2023-01-10 00:00:00','KR','premium','25-34',1,5800000),
  ('u002','bob@example.com','2023-03-15 00:00:00','KR','free','18-24',0,618000),
  ('u003','carol@example.com','2022-11-20 00:00:00','US','premium','35-44',1,677000),
  ('u004','dave@example.com','2023-06-01 00:00:00','JP','free','25-34',0,115000),
  ('u005','eve@example.com','2023-08-05 00:00:00','KR','premium','18-24',1,1742000),
  ('u006','frank@example.com','2022-09-12 00:00:00','US','free','45-54',0,79000),
  ('u007','grace@example.com','2023-02-28 00:00:00','CN','free','25-34',0,35000),
  ('u008','henry@example.com','2022-07-04 00:00:00','KR','premium','35-44',1,708000),
  ('u009','iris@example.com','2023-10-15 00:00:00','KR','free','18-24',0,178000),
  ('u010','jack@example.com','2023-05-20 00:00:00','US','premium','25-34',1,65000),
  ('u011','kate@example.com','2023-07-01 00:00:00','KR','free','35-44',0,299000),
  ('u012','leo@example.com','2023-09-10 00:00:00','DE','premium','45-54',1,45000),
  ('u013','mia@example.com','2023-11-01 00:00:00','KR','free','18-24',0,70000),
  ('u014','noah@example.com','2022-12-15 00:00:00','JP','premium','25-34',1,1299000),
  ('u015','olivia@example.com','2024-01-01 00:00:00','KR','free','18-24',0,129000);

INSERT INTO products (product_id, product_name, category, sub_category, price, cost, stock, rating, created_at)
VALUES
  ('p001','노트북 Pro 15','electronics','laptop',1299000,800000,50,4.8,'2023-01-01 00:00:00'),
  ('p002','무선 이어폰','electronics','audio',89000,35000,200,4.5,'2023-02-01 00:00:00'),
  ('p003','스마트워치','electronics','wearable',299000,150000,80,4.7,'2023-03-01 00:00:00'),
  ('p004','책상 램프','furniture','lighting',45000,20000,150,4.2,'2023-04-01 00:00:00'),
  ('p005','러닝화','sports','footwear',79000,40000,120,4.3,'2023-05-01 00:00:00'),
  ('p006','요가 매트','sports','yoga',35000,15000,300,4.6,'2023-06-01 00:00:00'),
  ('p007','커피 메이커','kitchen','appliance',129000,60000,60,4.4,'2023-07-01 00:00:00'),
  ('p008','마우스 패드','accessories','computer',25000,8000,500,4.1,'2023-08-01 00:00:00'),
  ('p009','백팩','fashion','bag',65000,30000,100,4.5,'2023-09-01 00:00:00'),
  ('p010','스탠딩 책상','furniture','desk',450000,200000,30,4.9,'2023-10-01 00:00:00');

-- ────────────────────────────────────────────────────────────────────
-- [6] INSERT - session_stats, ab_test_events, search_events, error_events, daily_metrics, click_events
-- ────────────────────────────────────────────────────────────────────
INSERT INTO session_stats (session_id, user_id, start_time, end_time, duration_sec, page_view_count, click_count, converted, device_type, country, bounce)
VALUES
  ('s001','u001','2024-01-15 09:00:00','2024-01-15 09:10:00',600,3,8,1,'desktop','KR',0),
  ('s002','u002','2024-01-15 10:15:00','2024-01-15 10:25:00',600,2,5,0,'mobile','KR',0),
  ('s003','u003','2024-01-15 11:00:00','2024-01-15 11:08:00',480,3,3,1,'tablet','US',0),
  ('s004','u004','2024-01-15 12:00:00','2024-01-15 12:15:00',900,2,6,1,'desktop','JP',0),
  ('s005','u005','2024-01-15 13:00:00','2024-01-15 13:12:00',720,3,4,0,'mobile','KR',0),
  ('s006','u006','2024-01-15 14:00:00','2024-01-15 14:01:00',60,1,0,0,'desktop','US',1),
  ('s007','u007','2024-01-15 14:30:00','2024-01-15 14:38:00',480,1,1,0,'mobile','CN',0),
  ('s008','u008','2024-01-15 15:00:00','2024-01-15 15:20:00',1200,3,12,1,'desktop','KR',0),
  ('s009','u009','2024-01-15 16:00:00','2024-01-15 16:03:00',180,1,2,0,'mobile','KR',0),
  ('s010','u010','2024-01-15 16:30:00','2024-01-15 16:45:00',900,1,3,0,'desktop','US',0),
  ('s011','u001','2024-01-15 17:00:00','2024-01-15 17:15:00',900,4,9,1,'mobile','KR',0),
  ('s012','u011','2024-01-15 18:00:00','2024-01-15 18:08:00',480,2,4,0,'desktop','KR',0),
  ('s013','u012','2024-01-15 19:00:00','2024-01-15 19:02:00',120,1,0,0,'mobile','DE',1),
  ('s014','u013','2024-01-15 20:00:00','2024-01-15 20:10:00',600,1,2,0,'desktop','KR',0),
  ('s015','u015','2024-01-15 21:00:00','2024-01-15 21:08:00',480,1,1,0,'mobile','KR',0);

INSERT INTO ab_test_events (event_time, user_id, experiment, variant, event_name, converted, revenue)
VALUES
  ('2024-01-15 09:00:00','u001','checkout_v2','treatment','view',0,0),
  ('2024-01-15 09:05:00','u001','checkout_v2','treatment','purchase',1,1299000),
  ('2024-01-15 10:15:00','u002','checkout_v2','control','view',0,0),
  ('2024-01-15 11:00:00','u003','checkout_v2','treatment','view',0,0),
  ('2024-01-15 11:30:00','u003','checkout_v2','treatment','purchase',1,299000),
  ('2024-01-15 12:00:00','u004','checkout_v2','control','view',0,0),
  ('2024-01-15 13:00:00','u005','checkout_v2','treatment','view',0,0),
  ('2024-01-15 13:15:00','u005','checkout_v2','treatment','purchase',1,1299000),
  ('2024-01-15 14:00:00','u006','homepage_banner','control','view',0,0),
  ('2024-01-15 14:30:00','u007','homepage_banner','treatment','view',0,0),
  ('2024-01-15 15:00:00','u008','checkout_v2','control','view',0,0),
  ('2024-01-15 15:10:00','u008','checkout_v2','control','purchase',1,129000),
  ('2024-01-15 16:00:00','u009','homepage_banner','control','view',0,0),
  ('2024-01-15 16:00:00','u009','homepage_banner','control','purchase',1,89000),
  ('2024-01-15 16:30:00','u010','homepage_banner','treatment','view',0,0),
  ('2024-01-15 17:00:00','u001','homepage_banner','treatment','view',0,0);

INSERT INTO search_events (event_time, user_id, query_text, result_count, clicked_rank, category, converted)
VALUES
  ('2024-01-15 09:00:00','u001','노트북 추천',45,1,'electronics',1),
  ('2024-01-15 10:00:00','u002','이어폰',120,3,'electronics',1),
  ('2024-01-15 11:00:00','u003','smartwatch',30,1,'electronics',1),
  ('2024-01-15 12:00:00','u004','책상',80,2,'furniture',1),
  ('2024-01-15 13:00:00','u005','노트북',45,1,'electronics',1),
  ('2024-01-15 14:00:00','u006','running shoes',60,4,'sports',0),
  ('2024-01-15 15:00:00','u007','요가',25,1,'sports',0),
  ('2024-01-15 16:00:00','u008','커피',90,2,'kitchen',1),
  ('2024-01-15 17:00:00','u009','이어폰',120,5,'electronics',1),
  ('2024-01-15 18:00:00','u010','backpack',55,1,'fashion',0),
  ('2024-01-15 19:00:00','u011','노트북',45,2,'electronics',0),
  ('2024-01-15 20:00:00','u012','laptop',38,3,'electronics',0);

INSERT INTO error_events (event_time, user_id, error_code, error_msg, page_url, severity, is_resolved)
VALUES
  ('2024-01-15 09:00:00','u001','ERR_404','Page not found','/old-product','warning',1),
  ('2024-01-15 10:00:00',NULL,'ERR_500','Internal server error','/checkout','critical',0),
  ('2024-01-15 11:00:00','u003','ERR_403','Access forbidden','/admin','warning',1),
  ('2024-01-15 12:00:00',NULL,'ERR_500','DB connection timeout','/api/data','critical',1),
  ('2024-01-15 13:00:00','u005','ERR_404','Page not found','/old-sale','warning',0),
  ('2024-01-15 14:00:00','u008','ERR_400','Bad request','/api/search','warning',1),
  ('2024-01-15 15:00:00',NULL,'ERR_503','Service unavailable','/payment','critical',0),
  ('2024-01-15 16:00:00','u010','ERR_429','Too many requests','/api/orders','warning',1),
  ('2024-01-15 17:00:00',NULL,'ERR_500','Null pointer exception','/checkout','critical',0),
  ('2024-01-15 18:00:00','u012','ERR_404','Product not found','/product/p999','warning',1);

INSERT INTO daily_metrics (report_date, country, device_type, page_views, unique_users, sessions, revenue, conversions, avg_session_sec)
VALUES
  ('2024-01-13','KR','desktop',12500,3200,4100,8500000,85,420),
  ('2024-01-13','KR','mobile',18000,4500,5800,6200000,62,280),
  ('2024-01-13','US','desktop',8200,2100,2700,5100000,51,390),
  ('2024-01-13','JP','desktop',4500,1200,1500,3200000,32,450),
  ('2024-01-14','KR','desktop',13200,3400,4300,9100000,91,430),
  ('2024-01-14','KR','mobile',19500,4800,6100,6800000,68,275),
  ('2024-01-14','US','desktop',8800,2300,2900,5500000,55,400),
  ('2024-01-14','JP','desktop',4800,1300,1600,3500000,35,460),
  ('2024-01-15','KR','desktop',14100,3600,4500,9800000,98,445),
  ('2024-01-15','KR','mobile',21000,5100,6500,7200000,72,270),
  ('2024-01-15','US','desktop',9200,2400,3100,5900000,59,410),
  ('2024-01-15','JP','desktop',5100,1400,1700,3800000,38,470);

INSERT INTO click_events (event_time, user_id, session_id, element_id, element_type, page_url, x_pos, y_pos, country)
VALUES
  ('2024-01-15 09:01:00','u001','s001','btn-cart','button','/products',450,320,'KR'),
  ('2024-01-15 09:02:00','u001','s001','img-laptop','image','/products',600,400,'KR'),
  ('2024-01-15 09:04:00','u001','s001','btn-checkout','button','/cart',400,280,'KR'),
  ('2024-01-15 10:16:00','u002','s002','banner-sale','banner','/home',800,200,'KR'),
  ('2024-01-15 11:03:00','u003','s003','nav-products','link','/home',200,50,'US'),
  ('2024-01-15 15:01:30','u008','s008','btn-checkout','button','/home',400,300,'KR'),
  ('2024-01-15 15:02:00','u008','s008','input-card','input','/checkout',400,250,'KR'),
  ('2024-01-15 17:01:00','u001','s011','img-laptop','image','/products',550,400,'KR'),
  ('2024-01-15 17:04:00','u001','s011','btn-cart','button','/products',450,320,'KR'),
  ('2024-01-15 17:09:00','u001','s011','btn-checkout','button','/cart',400,280,'KR');

-- ════════════════════════════════════════════════════════════════════
-- [7] SELECT 테스트 100개 — 기본부터 고급까지
-- ════════════════════════════════════════════════════════════════════

-- ── 기본 조회 ─────────────────────────────────────────────────────
-- [Q01] 전체 스캔 + LIMIT
SELECT * FROM user_profiles LIMIT 5;

-- [Q02] COUNT(*)
SELECT COUNT(*) AS total_pages FROM page_events;

-- [Q03] COUNT(컬럼) — NULL 제외
SELECT COUNT(user_id) AS identified_errors FROM error_events;

-- [Q04] 특정 컬럼만 조회
SELECT user_id, email, plan, country FROM user_profiles WHERE is_premium = 1;

-- [Q05] WHERE 문자열 등호
SELECT user_id, page_url, event_time FROM page_events WHERE country = 'KR' LIMIT 10;

-- [Q06] WHERE 숫자 범위
SELECT product_id, product_name, price FROM products WHERE price > 100000;

-- [Q07] WHERE BETWEEN
SELECT product_id, product_name, price FROM products WHERE price BETWEEN 50000 AND 300000;

-- [Q08] WHERE IN
SELECT user_id, plan, country FROM user_profiles WHERE country IN ('KR', 'US', 'JP');

-- [Q09] WHERE NOT IN
SELECT user_id, country FROM user_profiles WHERE country NOT IN ('KR', 'US');

-- [Q10] WHERE LIKE 패턴
SELECT product_id, product_name FROM products WHERE product_name LIKE '%노트북%';

-- [Q11] WHERE IS NULL
SELECT event_time, error_code, error_msg FROM error_events WHERE user_id IS NULL;

-- [Q12] WHERE IS NOT NULL
SELECT event_time, user_id, error_code FROM error_events WHERE user_id IS NOT NULL;

-- [Q13] BOOLEAN 조건
SELECT user_id, plan, is_premium FROM user_profiles WHERE is_premium = 1;

-- [Q14] 복합 AND 조건
SELECT user_id, page_url FROM page_events WHERE country = 'KR' AND device_type = 'mobile';

-- [Q15] 복합 OR 조건
SELECT user_id, country FROM user_profiles WHERE plan = 'premium' OR country = 'US';

-- ── GROUP BY 집계 ─────────────────────────────────────────────────
-- [Q16] GROUP BY + COUNT
SELECT device_type, COUNT(*) AS sessions FROM session_stats GROUP BY device_type;

-- [Q17] GROUP BY + SUM
SELECT category, SUM(amount) AS total_revenue FROM purchase_events GROUP BY category ORDER BY total_revenue DESC;

-- [Q18] GROUP BY + AVG
SELECT category, AVG(price) AS avg_price, AVG(rating) AS avg_rating FROM products GROUP BY category;

-- [Q19] GROUP BY + MAX/MIN
SELECT category, MAX(price) AS max_price, MIN(price) AS min_price, MAX(price)-MIN(price) AS spread FROM products GROUP BY category;

-- [Q20] GROUP BY + COUNT + SUM (다중 집계)
SELECT category, COUNT(*) AS orders, SUM(amount) AS revenue, SUM(quantity) AS units FROM purchase_events GROUP BY category;

-- [Q21] GROUP BY + AVG + ROUND
SELECT category, ROUND(AVG(price)) AS avg_price_rounded FROM products GROUP BY category ORDER BY avg_price_rounded DESC;

-- [Q22] GROUP BY 여러 컬럼
SELECT country, device_type, COUNT(*) AS cnt FROM page_events GROUP BY country, device_type ORDER BY country;

-- [Q23] GROUP BY + WHERE
SELECT user_id, COUNT(*) AS purchases, SUM(amount) AS total FROM purchase_events WHERE country = 'KR' GROUP BY user_id ORDER BY total DESC;

-- [Q24] GROUP BY 불리언
SELECT converted, COUNT(*) AS cnt, AVG(duration_sec) AS avg_duration FROM session_stats GROUP BY converted;

-- [Q25] GROUP BY + HAVING (핵심!)
SELECT category, SUM(amount) AS revenue FROM purchase_events GROUP BY category HAVING revenue > 200000 ORDER BY revenue DESC;

-- [Q26] HAVING COUNT
SELECT user_id, COUNT(*) AS event_count FROM page_events GROUP BY user_id HAVING event_count >= 3;

-- [Q27] HAVING AVG
SELECT category, AVG(price) AS avg_price FROM products GROUP BY category HAVING avg_price > 100000;

-- ── ORDER BY / LIMIT ──────────────────────────────────────────────
-- [Q28] ORDER BY DESC 단일
SELECT user_id, amount FROM purchase_events ORDER BY amount DESC LIMIT 10;

-- [Q29] ORDER BY 여러 컬럼
SELECT user_id, event_time, page_url FROM page_events ORDER BY user_id ASC, event_time DESC LIMIT 10;

-- [Q30] ORDER BY 위치 번호
SELECT category, SUM(amount) AS revenue FROM purchase_events GROUP BY category ORDER BY 2 DESC;

-- [Q31] ORDER BY + LIMIT 페이징
SELECT product_id, product_name, price FROM products ORDER BY price DESC LIMIT 5;

-- ── DISTINCT ─────────────────────────────────────────────────────
-- [Q32] SELECT DISTINCT
SELECT DISTINCT country FROM page_events ORDER BY country;

-- [Q33] DISTINCT + 여러 컬럼
SELECT DISTINCT device_type, country FROM session_stats ORDER BY country;

-- ── 함수 ─────────────────────────────────────────────────────────
-- [Q34] UPPER / LOWER
SELECT user_id, UPPER(country) AS country_upper FROM page_events WHERE user_id = 'u001' LIMIT 3;

-- [Q35] LENGTH
SELECT product_name, LENGTH(product_name) AS name_len FROM products ORDER BY name_len DESC;

-- [Q36] CONCAT
SELECT user_id, CONCAT(country, '/', device_type) AS location FROM page_events LIMIT 5;

-- [Q37] COALESCE (NULL 대체)
SELECT event_time, COALESCE(user_id, 'anonymous') AS user_label, error_code FROM error_events;

-- [Q38] IF 함수
SELECT user_id, plan, IF(is_premium=1, 'VIP', 'FREE') AS tier FROM user_profiles;

-- [Q39] CASE WHEN
SELECT user_id, amount,
  CASE
    WHEN amount >= 1000000 THEN 'high'
    WHEN amount >= 100000 THEN 'medium'
    ELSE 'low'
  END AS order_size
FROM purchase_events ORDER BY amount DESC LIMIT 10;

-- [Q40] NOW() 함수
SELECT NOW() AS current_time;

-- [Q41] SELECT 산술 연산
SELECT product_id, product_name, price, cost, price - cost AS gross_profit, ROUND((price-cost)/price*100) AS margin_pct FROM products;

-- ── 집계 심화 ─────────────────────────────────────────────────────
-- [Q42] 전체 COUNT + SUM (GROUP BY 없이)
SELECT COUNT(*) AS total_orders, SUM(amount) AS total_revenue, AVG(amount) AS avg_order FROM purchase_events;

-- [Q43] 필터된 집계
SELECT COUNT(*) AS premium_users, AVG(lifetime_value) AS avg_ltv FROM user_profiles WHERE is_premium = 1;

-- [Q44] 고객별 구매 통계
SELECT user_id, COUNT(*) AS orders, SUM(amount) AS total, AVG(amount) AS avg_order, MAX(amount) AS biggest FROM purchase_events GROUP BY user_id ORDER BY total DESC;

-- [Q45] 카테고리별 상품 통계
SELECT category, COUNT(*) AS products, AVG(price) AS avg_price, AVG(rating) AS avg_rating, SUM(stock) AS total_stock FROM products GROUP BY category;

-- [Q46] 일별 매출 추이 (report_date 기준)
SELECT report_date, SUM(revenue) AS daily_revenue, SUM(unique_users) AS daily_users FROM daily_metrics GROUP BY report_date ORDER BY report_date;

-- [Q47] 국가별 매출 (일별 메트릭)
SELECT country, SUM(revenue) AS total_revenue, SUM(conversions) AS total_conv FROM daily_metrics GROUP BY country ORDER BY total_revenue DESC;

-- [Q48] 디바이스별 세션 통계
SELECT device_type, COUNT(*) AS sessions, AVG(duration_sec) AS avg_sec, AVG(page_view_count) AS avg_pv, SUM(converted) AS conversions FROM session_stats GROUP BY device_type;

-- [Q49] 바운스율
SELECT COUNT(*) AS total, SUM(bounce) AS bounces FROM session_stats;

-- [Q50] 검색 카테고리별 CTR
SELECT category, COUNT(*) AS searches, AVG(clicked_rank) AS avg_rank, SUM(converted) AS conversions FROM search_events GROUP BY category ORDER BY searches DESC;

-- [Q51] A/B 실험별 전환율
SELECT experiment, variant, COUNT(*) AS participants, SUM(converted) AS converted, AVG(revenue) AS avg_revenue FROM ab_test_events GROUP BY experiment, variant ORDER BY experiment, variant;

-- [Q52] 결제 수단별 집계
SELECT payment_type, COUNT(*) AS orders, SUM(amount) AS revenue FROM purchase_events GROUP BY payment_type ORDER BY revenue DESC;

-- [Q53] 에러 타입별 심각도
SELECT error_code, severity, COUNT(*) AS cnt, SUM(is_resolved) AS resolved FROM error_events GROUP BY error_code, severity ORDER BY severity, error_code;

-- [Q54] 클릭 element 타입별 분포
SELECT element_type, COUNT(*) AS clicks, COUNT(DISTINCT user_id) AS unique_users FROM click_events GROUP BY element_type ORDER BY clicks DESC;

-- [Q55] 페이지별 이탈 분석 (page_view만)
SELECT page_url, COUNT(*) AS views FROM page_events WHERE event_name = 'page_view' GROUP BY page_url ORDER BY views DESC;

-- ── 복합 조건 ─────────────────────────────────────────────────────
-- [Q56] 프리미엄 사용자 국가별
SELECT country, COUNT(*) AS users, SUM(lifetime_value) AS total_ltv FROM user_profiles WHERE is_premium = 1 GROUP BY country ORDER BY total_ltv DESC;

-- [Q57] 고가 전자제품 구매
SELECT user_id, product_name, amount FROM purchase_events WHERE category = 'electronics' AND amount > 200000 ORDER BY amount DESC;

-- [Q58] KR 모바일 세션
SELECT session_id, user_id, duration_sec, page_view_count FROM session_stats WHERE country = 'KR' AND device_type = 'mobile' ORDER BY duration_sec DESC;

-- [Q59] 미해결 critical 에러
SELECT event_time, error_code, error_msg, page_url FROM error_events WHERE severity = 'critical' AND is_resolved = 0;

-- [Q60] 최근 검색 + 전환
SELECT user_id, query_text, clicked_rank FROM search_events WHERE converted = 1 AND clicked_rank <= 3 ORDER BY clicked_rank;

-- ── 고급 GROUP BY + HAVING ────────────────────────────────────────
-- [Q61] 활성 사용자 (페이지뷰 3회 이상)
SELECT user_id, COUNT(*) AS page_views FROM page_events GROUP BY user_id HAVING page_views >= 3 ORDER BY page_views DESC;

-- [Q62] 고매출 사용자 (10만원 이상)
SELECT user_id, SUM(amount) AS total FROM purchase_events GROUP BY user_id HAVING total >= 100000 ORDER BY total DESC;

-- [Q63] 재고 부족 카테고리 (평균 재고 100개 미만)
SELECT category, AVG(stock) AS avg_stock, COUNT(*) AS products FROM products GROUP BY category HAVING avg_stock < 100;

-- [Q64] 다양한 구매 카테고리 보유
SELECT user_id, COUNT(DISTINCT category) AS categories FROM purchase_events GROUP BY user_id HAVING categories >= 2 ORDER BY categories DESC;

-- [Q65] 전환된 세션만
SELECT user_id, COUNT(*) AS conv_sessions, AVG(duration_sec) AS avg_sec FROM session_stats WHERE converted = 1 GROUP BY user_id ORDER BY conv_sessions DESC;

-- ── 데이터 검증 ───────────────────────────────────────────────────
-- [Q66] 테이블별 레코드 수
SELECT COUNT(*) AS page_events_cnt FROM page_events;
SELECT COUNT(*) AS purchase_cnt FROM purchase_events;
SELECT COUNT(*) AS user_cnt FROM user_profiles;
SELECT COUNT(*) AS product_cnt FROM products;
SELECT COUNT(*) AS session_cnt FROM session_stats;
SELECT COUNT(*) AS ab_cnt FROM ab_test_events;
SELECT COUNT(*) AS search_cnt FROM search_events;
SELECT COUNT(*) AS error_cnt FROM error_events;
SELECT COUNT(*) AS metric_cnt FROM daily_metrics;
SELECT COUNT(*) AS click_cnt FROM click_events;

-- ── 표현식 / 리터럴 ───────────────────────────────────────────────
-- [Q76] 리터럴 SELECT (FROM 없이)
SELECT 1 + 1 AS two;
SELECT CONCAT('WOW-DB', ' v1.0') AS greeting;
SELECT UPPER('hello world') AS shout;
SELECT LENGTH('test string') AS len;
SELECT NOW() AS ts;
SELECT IF(1 > 0, 'yes', 'no') AS result;

-- ── WOW-DB 전용 함수 ─────────────────────────────────────────────
-- [Q82] FUNNEL_COUNT — 홈→상품→장바구니 퍼넬
SELECT FUNNEL_COUNT(
  user_key=>'user_id',
  event_col=>'page_url',
  time_col=>'event_time',
  steps=>['/home', '/products', '/cart'],
  time_window=>'INTERVAL 1 DAY'
) FROM page_events;

-- [Q83] FUNNEL_COUNT — 홈→체크아웃 2단계
SELECT FUNNEL_COUNT(
  user_key=>'user_id',
  event_col=>'page_url',
  time_col=>'event_time',
  steps=>['/home', '/checkout'],
  time_window=>'INTERVAL 1 DAY'
) FROM page_events;

-- [Q84] FUNNEL_COUNT — 구매 카테고리 퍼넬
SELECT FUNNEL_COUNT(
  user_key=>'user_id',
  event_col=>'category',
  time_col=>'event_time',
  steps=>['electronics', 'fashion'],
  time_window=>'INTERVAL 7 DAY'
) FROM purchase_events;

-- [Q85] PATH_ANALYSIS — 3단계 경로
SELECT PATH_ANALYSIS(
  user_key=>'user_id',
  event_col=>'page_url',
  time_col=>'event_time',
  path_length=>3
) FROM page_events;

-- [Q86] PATH_ANALYSIS — 2단계 경로
SELECT PATH_ANALYSIS(
  user_key=>'user_id',
  event_col=>'page_url',
  time_col=>'event_time',
  path_length=>2
) FROM page_events;

-- [Q87] COHORT_ANALYSIS — 구매 → 재구매
SELECT COHORT_ANALYSIS(
  user_key=>'user_id',
  entry_event=>'card',
  return_event=>'card',
  event_col=>'payment_type',
  time_col=>'event_time'
) FROM purchase_events;

-- [Q88] COHORT_ANALYSIS — 에러 발생 사용자 재방문
SELECT COHORT_ANALYSIS(
  user_key=>'user_id',
  entry_event=>'warning',
  return_event=>'critical',
  event_col=>'severity',
  time_col=>'event_time'
) FROM error_events;

-- ── CREATE SESSION MATERIALIZED VIEW ─────────────────────────────
-- [Q89] SMV 생성 (스펙 요구사항)
CREATE SESSION MATERIALIZED VIEW page_events_smv
ON page_events
USER KEY user_id
SESSION TIMEOUT 1800
NAME page_events_sessions;

-- ── JOIN 테스트 ───────────────────────────────────────────────────
-- [Q89b] INNER JOIN: 구매 이벤트 + 사용자 프로필
SELECT p.user_id, u.plan, u.country, p.category, p.amount
FROM purchase_events p
JOIN user_profiles u ON p.user_id = u.user_id
WHERE p.amount >= 500000
ORDER BY p.amount DESC
LIMIT 5;

-- [Q89c] LEFT JOIN: 모든 사용자 + 구매 정보 (구매 없는 사용자 포함)
SELECT u.user_id, u.plan, COUNT(p.order_id) AS purchase_count
FROM user_profiles u
LEFT JOIN purchase_events p ON u.user_id = p.user_id
GROUP BY u.user_id, u.plan
ORDER BY purchase_count DESC
LIMIT 8;

-- [Q89d] JOIN + GROUP BY + HAVING
SELECT u.country, COUNT(DISTINCT p.user_id) AS buyers, SUM(p.amount) AS revenue
FROM purchase_events p
JOIN user_profiles u ON p.user_id = u.user_id
GROUP BY u.country
HAVING revenue > 500000
ORDER BY revenue DESC;

-- ── 최종 검증 ─────────────────────────────────────────────────────
-- [Q90] SHOW TABLES
SHOW TABLES;

-- [Q91-100] 최종 카운트 검증
SELECT COUNT(*) FROM page_events;
SELECT COUNT(*) FROM purchase_events;
SELECT COUNT(*) FROM user_profiles;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM session_stats;
SELECT COUNT(*) FROM ab_test_events;
SELECT COUNT(*) FROM search_events;
SELECT COUNT(*) FROM error_events;
SELECT COUNT(*) FROM daily_metrics;
SELECT COUNT(*) FROM click_events;

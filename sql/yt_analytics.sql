-- =========================================================
-- YOUTUBE ANALYTICS — ONE FILE TO RULE THEM ALL (MySQL 8.0+)
-- Creates schema, loads CSVs, transforms/derives metrics,
-- monthly KPIs, top lists, and a full Pearson corr matrix.
-- =========================================================

-- Attention: allow client-side CSV import (requires privileges)
-- SET GLOBAL local_infile = 1;

SET NAMES utf8mb4;
SET time_zone = '+00:00';

-- --------------------------------------------
-- 1) SCHEMA
-- --------------------------------------------
CREATE DATABASE IF NOT EXISTS youtube_analytics
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
USE youtube_analytics;

DROP TABLE IF EXISTS channel_data;
CREATE TABLE channel_data (
  channel_name            VARCHAR(255),
  subscribers             BIGINT,
  views                   BIGINT,
  total_videos            INT,
  calc_channel_age_days   INT NULL,
  calc_uploads_per_year   DOUBLE NULL
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

DROP TABLE IF EXISTS video_data;
CREATE TABLE video_data (
  title            VARCHAR(512) NOT NULL,
  published_date   DATETIME NULL,
  duration_seconds INT NULL,
  views            BIGINT NULL,
  likes            BIGINT NULL,
  comments         BIGINT NULL,
  upload_month     CHAR(7) GENERATED ALWAYS AS (DATE_FORMAT(published_date, '%Y-%m')) STORED,
  published_year   INT     GENERATED ALWAYS AS (YEAR(published_date)) STORED,
  published_month  INT     GENERATED ALWAYS AS (MONTH(published_date)) STORED,
  KEY idx_pubdate (published_date),
  KEY idx_views (views),
  KEY idx_duration (duration_seconds)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- --------------------------------------------
-- 2) LOAD DATA
-- --------------------------------------------
-- VIDEO CSV -> video_data
LOAD DATA INFILE 'C:/temp/video_data_clean.csv'
INTO TABLE video_data
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@title, @published_date_raw, @duration_seconds, @views, @likes, @comments)
SET
  title            = @title,
  published_date   = STR_TO_DATE(SUBSTRING_INDEX(@published_date_raw, '+', 1), '%Y-%m-%d %H:%i:%s'),
  duration_seconds = NULLIF(@duration_seconds, 0),
  views            = NULLIF(@views, 0),
  likes            = NULLIF(@likes, 0),
  comments         = NULLIF(@comments, 0);

-- CHANNEL CSV -> channel_data
LOAD DATA INFILE 'C:/temp/channel_data_clean.csv'
INTO TABLE channel_data
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(channel_name, subscribers, views, total_videos, calc_channel_age_days, calc_uploads_per_year);

-- --------------------------------------------
-- 3) METRICS/DERIVATIONS
-- --------------------------------------------
DROP VIEW IF EXISTS vw_video_metrics;
CREATE VIEW vw_video_metrics AS
SELECT
  title,
  published_date,
  duration_seconds,
  views,
  likes,
  comments,
  upload_month,
  published_year,
  published_month,
  (likes + comments) / NULLIF(views, 0) AS engagement_rate
FROM video_data;

-- --------------------------------------------
-- 4) MONTHLY KPIs
-- --------------------------------------------
WITH base AS (
  SELECT
    DATE_FORMAT(published_date, '%Y-%m-01') AS month_start,
    views,
    (likes + comments) / NULLIF(views, 0) AS engagement_rate
  FROM video_data
  WHERE published_date IS NOT NULL
),
ranked AS (
  SELECT
    month_start,
    views,
    engagement_rate,
    ROW_NUMBER() OVER (PARTITION BY month_start ORDER BY views) AS rn,
    COUNT(*)    OVER (PARTITION BY month_start)                 AS cnt
  FROM base
),
median_calc AS (
  SELECT
    month_start,
    AVG(views) AS median_views
  FROM ranked
  WHERE rn IN (FLOOR((cnt + 1)/2), CEIL((cnt + 1)/2))
  GROUP BY month_start
)
SELECT
  b.month_start,
  COUNT(*)                AS video_count,
  AVG(b.views)            AS avg_views,
  mc.median_views         AS median_views,
  AVG(b.engagement_rate)  AS avg_engagement_rate
FROM base b
JOIN median_calc mc USING (month_start)
GROUP BY b.month_start
ORDER BY b.month_start;

-- --------------------------------------------
-- 5) TOP LISTS
-- --------------------------------------------
-- Top 15 by views
SELECT title, views, likes, comments, duration_seconds, published_date
FROM video_data
ORDER BY views DESC
LIMIT 15;

-- Top 15 by engagement (views >= 1000)
SELECT
  title,
  (likes + comments) / NULLIF(views, 0) AS engagement_rate,
  views, likes, comments, duration_seconds, published_date
FROM video_data
WHERE views >= 1000
ORDER BY engagement_rate DESC
LIMIT 15;

-- --------------------------------------------
-- PEARSON CORRELATION MATRIX
-- Metrics: views, likes, comments, duration_seconds, engagement_rate
-- --------------------------------------------
DROP VIEW IF EXISTS vw_corr_pearson;
CREATE VIEW vw_corr_pearson AS
WITH v AS (
  SELECT
    CAST(views AS DOUBLE)            AS v,
    CAST(likes AS DOUBLE)            AS l,
    CAST(comments AS DOUBLE)         AS c,
    CAST(duration_seconds AS DOUBLE) AS d,
    CAST((likes + comments) / NULLIF(views, 0) AS DOUBLE) AS e
  FROM vw_video_metrics
),
/* helpers to compute r(x,y) with pairwise non-null rows */
r_vl AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(v)=0 OR STDDEV_POP(l)=0
              THEN NULL
              ELSE (AVG(v*l) - AVG(v)*AVG(l)) / (STDDEV_POP(v)*STDDEV_POP(l))
         END AS r
  FROM v WHERE v IS NOT NULL AND l IS NOT NULL
),
r_vc AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(v)=0 OR STDDEV_POP(c)=0
              THEN NULL
              ELSE (AVG(v*c) - AVG(v)*AVG(c)) / (STDDEV_POP(v)*STDDEV_POP(c))
         END AS r
  FROM v WHERE v IS NOT NULL AND c IS NOT NULL
),
r_vd AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(v)=0 OR STDDEV_POP(d)=0
              THEN NULL
              ELSE (AVG(v*d) - AVG(v)*AVG(d)) / (STDDEV_POP(v)*STDDEV_POP(d))
         END AS r
  FROM v WHERE v IS NOT NULL AND d IS NOT NULL
),
r_ve AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(v)=0 OR STDDEV_POP(e)=0
              THEN NULL
              ELSE (AVG(v*e) - AVG(v)*AVG(e)) / (STDDEV_POP(v)*STDDEV_POP(e))
         END AS r
  FROM v WHERE v IS NOT NULL AND e IS NOT NULL
),
r_lc AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(l)=0 OR STDDEV_POP(c)=0
              THEN NULL
              ELSE (AVG(l*c) - AVG(l)*AVG(c)) / (STDDEV_POP(l)*STDDEV_POP(c))
         END AS r
  FROM v WHERE l IS NOT NULL AND c IS NOT NULL
),
r_ld AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(l)=0 OR STDDEV_POP(d)=0
              THEN NULL
              ELSE (AVG(l*d) - AVG(l)*AVG(d)) / (STDDEV_POP(l)*STDDEV_POP(d))
         END AS r
  FROM v WHERE l IS NOT NULL AND d IS NOT NULL
),
r_le AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(l)=0 OR STDDEV_POP(e)=0
              THEN NULL
              ELSE (AVG(l*e) - AVG(l)*AVG(e)) / (STDDEV_POP(l)*STDDEV_POP(e))
         END AS r
  FROM v WHERE l IS NOT NULL AND e IS NOT NULL
),
r_cd AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(c)=0 OR STDDEV_POP(d)=0
              THEN NULL
              ELSE (AVG(c*d) - AVG(c)*AVG(d)) / (STDDEV_POP(c)*STDDEV_POP(d))
         END AS r
  FROM v WHERE c IS NOT NULL AND d IS NOT NULL
),
r_ce AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(c)=0 OR STDDEV_POP(e)=0
              THEN NULL
              ELSE (AVG(c*e) - AVG(c)*AVG(e)) / (STDDEV_POP(c)*STDDEV_POP(e))
         END AS r
  FROM v WHERE c IS NOT NULL AND e IS NOT NULL
),
r_de AS (
  SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(d)=0 OR STDDEV_POP(e)=0
              THEN NULL
              ELSE (AVG(d*e) - AVG(d)*AVG(e)) / (STDDEV_POP(d)*STDDEV_POP(e))
         END AS r
  FROM v WHERE d IS NOT NULL AND e IS NOT NULL
),
-- assemble all (x,y,r) including diagonals and symmetric entries
pairs AS (
  SELECT 'views' AS x, 'views' AS y, 1.0 AS r
  UNION ALL SELECT 'likes','likes',1.0
  UNION ALL SELECT 'comments','comments',1.0
  UNION ALL SELECT 'duration_seconds','duration_seconds',1.0
  UNION ALL SELECT 'engagement_rate','engagement_rate',1.0

  UNION ALL SELECT 'views','likes',            r FROM r_vl
  UNION ALL SELECT 'likes','views',            r FROM r_vl

  UNION ALL SELECT 'views','comments',         r FROM r_vc
  UNION ALL SELECT 'comments','views',         r FROM r_vc

  UNION ALL SELECT 'views','duration_seconds', r FROM r_vd
  UNION ALL SELECT 'duration_seconds','views', r FROM r_vd

  UNION ALL SELECT 'views','engagement_rate',  r FROM r_ve
  UNION ALL SELECT 'engagement_rate','views',  r FROM r_ve

  UNION ALL SELECT 'likes','comments',         r FROM r_lc
  UNION ALL SELECT 'comments','likes',         r FROM r_lc

  UNION ALL SELECT 'likes','duration_seconds', r FROM r_ld
  UNION ALL SELECT 'duration_seconds','likes', r FROM r_ld

  UNION ALL SELECT 'likes','engagement_rate',  r FROM r_le
  UNION ALL SELECT 'engagement_rate','likes',  r FROM r_le

  UNION ALL SELECT 'comments','duration_seconds', r FROM r_cd
  UNION ALL SELECT 'duration_seconds','comments', r FROM r_cd

  UNION ALL SELECT 'comments','engagement_rate',  r FROM r_ce
  UNION ALL SELECT 'engagement_rate','comments',  r FROM r_ce

  UNION ALL SELECT 'duration_seconds','engagement_rate', r FROM r_de
  UNION ALL SELECT 'engagement_rate','duration_seconds', r FROM r_de
)
SELECT
  x AS metric,
  MAX(CASE WHEN y='views'            THEN r END) AS views,
  MAX(CASE WHEN y='likes'            THEN r END) AS likes,
  MAX(CASE WHEN y='comments'         THEN r END) AS comments,
  MAX(CASE WHEN y='duration_seconds' THEN r END) AS duration_seconds,
  MAX(CASE WHEN y='engagement_rate'  THEN r END) AS engagement_rate
FROM pairs
GROUP BY x
ORDER BY FIELD(x,'views','likes','comments','duration_seconds','engagement_rate');



-- =========================================================
-- FOR TABLEAU 
-- =========================================================
-- --------------------------------------------
-- Add two deature views
-- --------------------------------------------
-- Per-1k interaction rates (less denominator-coupling artifacts)
CREATE OR REPLACE VIEW vw_video_rates AS
SELECT
  title, published_date, duration_seconds, views, likes, comments,
  1000.0 * likes    / NULLIF(views,0) AS likes_per_1k,
  1000.0 * comments / NULLIF(views,0) AS comments_per_1k,
  (likes + comments) / NULLIF(views,0) AS engagement_rate,
  upload_month, published_year, published_month
FROM video_data;

-- Log-transformed counts (tame heavy tails)
CREATE OR REPLACE VIEW vw_video_logs AS
SELECT
  title, published_date, duration_seconds,
  LOG10(views + 1)    AS log_views,
  LOG10(likes + 1)    AS log_likes,
  LOG10(comments + 1) AS log_comments
FROM video_data;

-- --------------------------------------------
-- Add indexes for Tableau filters
-- --------------------------------------------
ALTER TABLE video_data ADD INDEX idx_year_month (published_year, published_month);
ALTER TABLE video_data ADD INDEX idx_month_char (upload_month);
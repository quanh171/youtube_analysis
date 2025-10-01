/* =========================================================
   YOUTUBE ANALYTICS
   Creates/updates schema, loads CSVs, derives metrics, KPIs,
   top lists, and a Pearson correlation matrix.

   Last updated: 2025-09-30
   By: Quan Hoang
   ========================================================= */
   
/* ---------- Session & Compatibility ---------- */
-- If needed (client CSV import)
-- SET GLOBAL local_infile = 1;

SET NAMES utf8mb4;                -- full Unicode (emoji-safe)
SET time_zone = '+00:00';         -- keep timestamps consistent (UTC)

/* =========================================================
   1) DATABASE & SCHEMA
   ========================================================= */
CREATE DATABASE IF NOT EXISTS youtube_analytics
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;
USE youtube_analytics;

/* ---------- CHANNEL TABLE ---------- */
DROP TABLE IF EXISTS channel_data;

CREATE TABLE channel_data (
  channel_name   VARCHAR(255),
  subscribers    BIGINT,
  views          BIGINT,
  total_videos   INT,
  playlist_id    VARCHAR(50),
  PRIMARY KEY (playlist_id)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci;
  
/* ---------- VIDEO TABLE ----------*/
DROP TABLE IF EXISTS video_data;
CREATE TABLE video_data (
  video_id           VARCHAR(20)  NOT NULL,
  title             VARCHAR(512) NULL,
  video_type         ENUM('Short','Regular','Live','Upcoming Live') NULL,
  live_status        ENUM('none','live','upcoming') NULL,
  category_id        INT UNSIGNED NULL,
  category_name      VARCHAR(100) NULL,
  published_date    DATETIME NULL,
  duration_seconds   INT UNSIGNED NULL,
  views             BIGINT UNSIGNED NULL,
  likes             BIGINT UNSIGNED NULL,
  comments          BIGINT UNSIGNED NULL,
  
    -- Convenient filters for BI tools
  upload_month      CHAR(7)
     GENERATED ALWAYS AS (DATE_FORMAT(published_date, '%Y-%m')) STORED,
  published_year    INT
     GENERATED ALWAYS AS (YEAR(published_date)) STORED,
  published_month   INT
     GENERATED ALWAYS AS (MONTH(published_date)) STORED,

  PRIMARY KEY (video_id)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci;
  
/* ---------- Helpful Indexes ----------*/
CREATE INDEX idx_pubdate     ON video_data (published_date);
CREATE INDEX idx_views       ON video_data (views);
CREATE INDEX idx_duration    ON video_data (duration_seconds);
CREATE INDEX idx_year_month  ON video_data (published_year, published_month);
CREATE INDEX idx_month_char  ON video_data (upload_month);
CREATE INDEX idx_category    ON video_data (category_id);
CREATE INDEX idx_type        ON video_data (video_type);

/* =========================================================
   2) LOAD DATA (CSV -> TABLES)
   =========================================================*/

/* ---------- Load VIDEO CSV ----------*/
LOAD DATA INFILE 'C:/temp/ts_video_data.csv'
INTO TABLE video_data
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'   
IGNORE 1 LINES
(
  video_id,
  title,
  video_type,
  live_status,
  category_id,
  category_name,
  @published_date_raw,
  @duration_seconds_raw,
  @views_raw,
  @likes_raw,
  @comments_raw
)
SET
  published_date =
    CASE
      WHEN NULLIF(@published_date_raw, '') IS NULL THEN NULL
      WHEN LENGTH(REPLACE(SUBSTRING_INDEX(REPLACE(SUBSTRING_INDEX(@published_date_raw,'+',1),'Z',''),'.',1),'T',' ')) = 10
        THEN STR_TO_DATE(
               REPLACE(SUBSTRING_INDEX(REPLACE(SUBSTRING_INDEX(@published_date_raw,'+',1),'Z',''),'.',1),'T',' '),
               '%Y-%m-%d'
             )
      ELSE STR_TO_DATE(
             REPLACE(SUBSTRING_INDEX(REPLACE(SUBSTRING_INDEX(@published_date_raw,'+',1),'Z',''),'.',1),'T',' '),
             '%Y-%m-%d %H:%i:%s'
           )
    END,

  /* Numeric fields: blank -> NULL */
  duration_seconds = NULLIF(@duration_seconds_raw, ''),
  views           = NULLIF(@views_raw, ''),
  likes           = NULLIF(@likes_raw, ''),
  comments        = NULLIF(@comments_raw, '');

/* ---------- Load CHANNEL CSV ----------*/
LOAD DATA INFILE 'C:/temp/ts_channel_data.csv'
INTO TABLE channel_data
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(channel_name, subscribers, views, total_videos, playlist_id);

/* ---------- Base Metrics View ----------
   Adds a consistent engagement rate (guarding divide-by-zero).
*/
DROP VIEW IF EXISTS vw_video_metrics;
CREATE VIEW vw_video_metrics AS
SELECT
  VideoId,
  Title,
  VideoType,
  LiveStatus,
  CategoryId,
  CategoryName,
  Published_date,
  DurationSeconds,
  Views, Likes, Comments,
  upload_month, published_year, published_month,
  (Likes + Comments) / NULLIF(Views, 0) AS engagement_rate
FROM video_data;

/* =========================================================
   3) METRICS & VIEWS
   ========================================================= */

/* ---------- Base Metrics View ----------*/
DROP VIEW IF EXISTS vw_video_metrics;
CREATE VIEW vw_video_metrics AS
SELECT
  video_id,
  title,
  video_type,
  live_status,
  category_id,
  category_name,
  published_date,
  duration_seconds,
  views, likes, comments,
  upload_month, published_year, published_month,
  (likes + comments) / NULLIF(views, 0) AS engagement_rate
FROM video_data;

/* ---------- Summary by Video Type ---------- */
DROP VIEW IF EXISTS vw_video_type_summary;
CREATE VIEW vw_video_type_summary AS
SELECT
  video_type,
  COUNT(*)                                  AS video_count,
  SUM(views)                                AS total_views,
  AVG(duration_seconds)                     AS avg_duration_sec,
  AVG((likes + comments)/NULLIF(views, 0))  AS avg_engagement_rate
FROM video_data
GROUP BY video_type;

/* ---------- Top Categories (by total views) ---------- */
DROP VIEW IF EXISTS vw_top_categories;
CREATE VIEW vw_top_categories AS
SELECT
  category_id,
  category_name,
  COUNT(*)                                  AS video_count,
  SUM(views)                                AS total_views,
  AVG((likes + comments)/NULLIF(views, 0))  AS avg_engagement_rate
FROM video_data
GROUP BY category_id, category_name
ORDER BY total_views DESC;

/* ---------- Monthly KPIs (avg/median views, engagement) ----------*/
DROP VIEW IF EXISTS vw_monthly_kpis;
CREATE VIEW vw_monthly_kpis AS
WITH base AS (
  SELECT
    DATE_FORMAT(published_date, '%Y-%m-01') AS month_start,
    views,
    (likes + comments) / NULLIF(views, 0)   AS engagement_rate
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
  COUNT(*)               AS video_count,
  AVG(b.views)           AS avg_views,
  mc.median_views        AS median_views,
  AVG(b.engagement_rate) AS avg_engagement_rate
FROM base b
JOIN median_calc mc USING (month_start)
GROUP BY b.month_start
ORDER BY b.month_start;

/* ---------- Top Lists (examples) ---------- */
-- Top 15 by views
DROP VIEW IF EXISTS vw_top15_by_views;
CREATE VIEW vw_top15_by_views AS
SELECT
  title, views, likes, comments, duration_seconds, published_date, video_type, category_name
FROM video_data
ORDER BY views DESC
LIMIT 15;

-- Top 15 by engagement (guarding low-view noise with a floor)
DROP VIEW IF EXISTS vw_top15_by_engagement;
CREATE VIEW vw_top15_by_engagement AS
SELECT
  title,
  (likes + comments) / NULLIF(views, 0) AS engagement_rate,
  views, likes, comments, duration_seconds, published_date, video_type, category_name
FROM video_data
WHERE views >= 1000
ORDER BY engagement_rate DESC
LIMIT 15;

/* =========================================================
   4) PEARSON CORRELATION MATRIX
   Metrics: views, likes, comments, duration_seconds, engagement_rate
   (pairwise non-null selection; diagonals = 1.0)
   ========================================================= */
   
DROP VIEW IF EXISTS vw_corr_pearson;
CREATE VIEW vw_corr_pearson AS
WITH v AS (
  SELECT
    CAST(views AS DOUBLE)             AS v,
    CAST(likes AS DOUBLE)             AS l,
    CAST(comments AS DOUBLE)          AS c,
    CAST(duration_seconds AS DOUBLE)  AS d,
    CAST((likes + comments) / NULLIF(views, 0) AS DOUBLE) AS e
  FROM vw_video_metrics
),
r_vl AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(v)=0 OR STDDEV_POP(l)=0
                      THEN NULL
                      ELSE (AVG(v*l) - AVG(v)*AVG(l)) / (STDDEV_POP(v)*STDDEV_POP(l)) END AS r
          FROM v WHERE v IS NOT NULL AND l IS NOT NULL ),
r_vc AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(v)=0 OR STDDEV_POP(c)=0
                      THEN NULL
                      ELSE (AVG(v*c) - AVG(v)*AVG(c)) / (STDDEV_POP(v)*STDDEV_POP(c)) END AS r
          FROM v WHERE v IS NOT NULL AND c IS NOT NULL ),
r_vd AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(v)=0 OR STDDEV_POP(d)=0
                      THEN NULL
                      ELSE (AVG(v*d) - AVG(v)*AVG(d)) / (STDDEV_POP(v)*STDDEV_POP(d)) END AS r
          FROM v WHERE v IS NOT NULL AND d IS NOT NULL ),
r_ve AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(v)=0 OR STDDEV_POP(e)=0
                      THEN NULL
                      ELSE (AVG(v*e) - AVG(v)*AVG(e)) / (STDDEV_POP(v)*STDDEV_POP(e)) END AS r
          FROM v WHERE v IS NOT NULL AND e IS NOT NULL ),
r_lc AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(l)=0 OR STDDEV_POP(c)=0
                      THEN NULL
                      ELSE (AVG(l*c) - AVG(l)*AVG(l)) / (STDDEV_POP(l)*STDDEV_POP(c)) END AS r
          FROM v WHERE l IS NOT NULL AND c IS NOT NULL ),
r_ld AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(l)=0 OR STDDEV_POP(d)=0
                      THEN NULL
                      ELSE (AVG(l*d) - AVG(l)*AVG(d)) / (STDDEV_POP(l)*STDDEV_POP(d)) END AS r
          FROM v WHERE l IS NOT NULL AND d IS NOT NULL ),
r_le AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(l)=0 OR STDDEV_POP(e)=0
                      THEN NULL
                      ELSE (AVG(l*e) - AVG(l)*AVG(e)) / (STDDEV_POP(l)*STDDEV_POP(e)) END AS r
          FROM v WHERE l IS NOT NULL AND e IS NOT NULL ),
r_cd AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(c)=0 OR STDDEV_POP(d)=0
                      THEN NULL
                      ELSE (AVG(c*d) - AVG(c)*AVG(d)) / (STDDEV_POP(c)*STDDEV_POP(d)) END AS r
          FROM v WHERE c IS NOT NULL AND d IS NOT NULL ),
r_ce AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(c)=0 OR STDDEV_POP(e)=0
                      THEN NULL
                      ELSE (AVG(c*e) - AVG(c)*AVG(e)) / (STDDEV_POP(c)*STDDEV_POP(e)) END AS r
          FROM v WHERE c IS NOT NULL AND e IS NOT NULL ),
r_de AS ( SELECT CASE WHEN COUNT(*) < 2 OR STDDEV_POP(d)=0 OR STDDEV_POP(e)=0
                      THEN NULL
                      ELSE (AVG(d*e) - AVG(d)*AVG(e)) / (STDDEV_POP(d)*STDDEV_POP(e)) END AS r
          FROM v WHERE d IS NOT NULL AND e IS NOT NULL ),
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

/* =========================================================
   End of file
   ========================================================= */
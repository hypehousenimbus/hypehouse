{{ config(materialized='view') }}

WITH ranked_videos AS (
    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        VIDEO_ID,
        TRENDING_DATE,
        VIEW_COUNT,
        LIKES,
        COMMENT_COUNT,
        ROW_NUMBER() OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS rn,
        LAG(TRENDING_DATE, 1) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS prev_date
    FROM {{ ref('dim_trending_youtube_videos_metrics') }}
),

consecutive_check AS (
    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        VIDEO_ID,
        TRENDING_DATE,
        VIEW_COUNT,
        LIKES,
        COMMENT_COUNT,
        CASE 
            WHEN prev_date IS NULL OR DATEDIFF(day, prev_date, TRENDING_DATE) = 1 THEN 1
            ELSE 0
        END AS is_consecutive
    FROM ranked_videos
),

strict_consecutive_trending AS (
    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        VIDEO_ID,
        TRENDING_DATE,
        VIEW_COUNT,
        LIKES,
        COMMENT_COUNT,
        SUM(is_consecutive) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS consecutive_days
    FROM consecutive_check
    WHERE is_consecutive = 1
)

SELECT 
    CHANNEL_ID,
    CHANNEL_TITLE,
    VIDEO_ID,
    COUNT(1) AS num_consecutive_trending_days,
    ROUND(AVG(VIEW_COUNT), 2) AS avg_view_count,
    ROUND(AVG(LIKES), 2) AS avg_likes,
    ROUND(AVG(COMMENT_COUNT), 2) AS avg_comment_count
FROM strict_consecutive_trending
GROUP BY CHANNEL_ID, CHANNEL_TITLE, VIDEO_ID

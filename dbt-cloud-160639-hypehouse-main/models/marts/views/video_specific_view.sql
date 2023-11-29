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
),

video_metrics AS (
    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        VIDEO_ID,
        TRENDING_DATE,
        VIEW_COUNT,
        LIKES,
        COMMENT_COUNT,
        LAG(VIEW_COUNT, 1, 0) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE),
        LAG(LIKES, 1, 0) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE)
    FROM strict_consecutive_trending
)

SELECT 
    CHANNEL_ID,
    CHANNEL_TITLE,
    VIDEO_ID,
    TRENDING_DATE,
    VIEW_COUNT,
    VIEW_COUNT - LAG(VIEW_COUNT, 1, 0) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS daily_view_increase,
    LIKES,
    LIKES - LAG(LIKES, 1, 0) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS daily_likes_increase,
    COMMENT_COUNT,
    COMMENT_COUNT - LAG(COMMENT_COUNT, 1, 0) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS daily_comment_increase
FROM video_metrics
ORDER BY CHANNEL_ID, VIDEO_ID, TRENDING_DATE


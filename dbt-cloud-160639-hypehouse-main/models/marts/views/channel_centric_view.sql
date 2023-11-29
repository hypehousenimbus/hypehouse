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

aggregated_channel_metrics AS (
    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        COUNT(DISTINCT VIDEO_ID) AS num_consecutive_trending_videos,
        ROUND(AVG(VIEW_COUNT), 2) AS avg_view_count,
        MIN(VIEW_COUNT) AS min_view_count,
        MAX(VIEW_COUNT) AS max_view_count,
        ROUND(AVG(LIKES), 2) AS avg_likes,
        MIN(LIKES) AS min_likes,
        MAX(LIKES) AS max_likes,
        ROUND(AVG(COMMENT_COUNT), 2) AS avg_comment_count,
        MIN(COMMENT_COUNT) AS min_comment_count,
        MAX(COMMENT_COUNT) AS max_comment_count
    FROM strict_consecutive_trending
    GROUP BY CHANNEL_ID, CHANNEL_TITLE
    HAVING COUNT(DISTINCT VIDEO_ID) > 1
)

SELECT 
    CHANNEL_ID,
    CHANNEL_TITLE,
    num_consecutive_trending_videos,
    avg_view_count,
    min_view_count,
    max_view_count,
    avg_likes,
    min_likes,
    max_likes,
    avg_comment_count,
    min_comment_count,
    max_comment_count
FROM aggregated_channel_metrics

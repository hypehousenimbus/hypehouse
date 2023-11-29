-- view with creators that have videos that are trending for multiple consecutive days (day 1-2-3-4-5) 
-- with the average , min , max and amount of vids that trend consecutively vs other found vids

-- creator-centric view

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
        *,
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
        rn,
        SUM(is_consecutive) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS consecutive_days
    FROM consecutive_check
    WHERE consecutive_check.is_consecutive = 1
),

final_consecutive_videos AS (
    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        VIDEO_ID,
        MAX(consecutive_days) AS max_consecutive_days,
        AVG(VIEW_COUNT) AS avg_view_count,
        MIN(VIEW_COUNT) AS min_view_count,
        MAX(VIEW_COUNT) AS max_view_count,
        AVG(LIKES) AS avg_likes,
        MIN(LIKES) AS min_likes,
        MAX(LIKES) AS max_likes
    FROM strict_consecutive_trending
    GROUP BY CHANNEL_ID, CHANNEL_TITLE, VIDEO_ID
    HAVING MAX(consecutive_days) > 1
)

SELECT *
FROM final_consecutive_videos
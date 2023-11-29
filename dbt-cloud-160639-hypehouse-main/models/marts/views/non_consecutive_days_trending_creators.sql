WITH ranked_videos AS (
    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        VIDEO_ID,
        TRENDING_DATE,
        LEAD(TRENDING_DATE, 1) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS next_date,
        FIRST_VALUE(TRENDING_DATE) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS first_trending_date,
        LAST_VALUE(TRENDING_DATE) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last_trending_date
    FROM dim_trending_youtube_videos_metrics
),

trending_gaps AS (
    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        VIDEO_ID,
        TRENDING_DATE AS end_trending,
        next_date AS start_next_trending,
        first_trending_date,
        last_trending_date
    FROM ranked_videos
    WHERE next_date IS NOT NULL AND DATEDIFF(day, TRENDING_DATE, next_date) > 1
),

recursive_dates (CHANNEL_ID, CHANNEL_TITLE, VIDEO_ID, missing_day, end_trending, start_next_trending, first_trending_date, last_trending_date) AS (
    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        VIDEO_ID,
        DATEADD(day, 1, end_trending) AS missing_day,
        end_trending,
        start_next_trending,
        first_trending_date,
        last_trending_date
    FROM trending_gaps

    UNION ALL

    SELECT 
        CHANNEL_ID,
        CHANNEL_TITLE,
        VIDEO_ID,
        DATEADD(day, 1, missing_day),
        end_trending,
        start_next_trending,
        first_trending_date,
        last_trending_date
    FROM recursive_dates
    WHERE DATEADD(day, 1, missing_day) < start_next_trending
)

SELECT 
    CHANNEL_ID,
    CHANNEL_TITLE,
    VIDEO_ID,
    first_trending_date,
    last_trending_date,
    missing_day
FROM recursive_dates
WHERE missing_day <= last_trending_date
GROUP BY CHANNEL_ID, CHANNEL_TITLE, VIDEO_ID, missing_day, first_trending_date, last_trending_date
ORDER BY CHANNEL_ID, VIDEO_ID
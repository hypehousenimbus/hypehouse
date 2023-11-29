SELECT
    DATE_TRUNC('day', trending_date) AS published_date,
    ROUND(AVG(number_of_tags),2) AS avg_number_of_tags
FROM {{ ref('dim_trending_youtube_videos_metrics') }}
GROUP BY DATE_TRUNC('day', trending_date)
ORDER BY published_date ASC
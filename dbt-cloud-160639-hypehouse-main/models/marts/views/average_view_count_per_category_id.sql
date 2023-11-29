SELECT
    category_id,
    category,
    ROUND(AVG(view_count), 2) AS avg_view_count 
FROM {{ ref('dim_trending_youtube_videos_metrics') }}
GROUP BY category_id, category


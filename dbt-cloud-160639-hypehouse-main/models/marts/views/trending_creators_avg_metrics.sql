-- view with creators that have multiple trending vids with the avg values for likes,tags,views

WITH trending_videos AS (
    SELECT 
        channel_id,
        channel_title,
        COUNT(*) AS num_trending_videos,
        AVG(likes) AS avg_likes,
        AVG(number_of_tags) AS avg_tags,
        AVG(view_count) AS avg_views
    FROM {{ ref('dim_trending_youtube_videos_metrics') }}
    GROUP BY channel_id, channel_title
)

SELECT 
    channel_id,
    channel_title,
    num_trending_videos,
    avg_likes,
    avg_tags,
    avg_views
FROM trending_videos
WHERE num_trending_videos > 1

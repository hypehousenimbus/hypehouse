{{ config(materialized='view') }}

SELECT
    channel_title,
    COUNT(video_id) AS trending_videos_count
FROM {{ ref('dim_trending_youtube_videos_metrics') }}
GROUP BY channel_title
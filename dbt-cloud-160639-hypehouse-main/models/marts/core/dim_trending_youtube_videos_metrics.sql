{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

WITH
video_metrics AS (
    SELECT * FROM {{ ref('video_metrics') }}
),
video_info AS (
    SELECT * FROM {{ ref('video_info') }}
    {% if is_incremental() %}
        WHERE trending_date >= (SELECT MAX(trending_date) FROM {{ this }})
    {% endif %}
),
youtube_categories AS (
    SELECT * FROM {{ ref('youtube_categories') }}
),

trending_youtube_videos_metrics AS ( 
    SELECT DISTINCT
        video_metrics.id,
        video_metrics.video_id,
        video_info.title,
        video_info.description,
        video_info.published_at,
        video_info.tags,
        CASE 
            WHEN video_info.tags = '[none]' THEN 0
            ELSE LENGTH(video_info.tags) - LENGTH(REPLACE(video_info.tags, '|', '')) + 1
        END AS number_of_tags,
        video_metrics.comment_count AS comment_count,
        video_info.trending_date AS trending_date,
        video_info.comments_disabled,
        video_metrics.view_count AS view_count,
        video_metrics.comment_count / NULLIF(video_metrics.view_count, 0)*100 AS comment_views_ratio_percent,
        video_metrics.likes AS likes,
        video_metrics.dislikes AS dislikes,
        video_info.ratings_disabled AS ratings_disabled,
        video_info.categoryId AS category_id,
        youtube_categories.category_name AS category,
        video_info.channelId AS channel_id,
        video_info.channelTitle AS channel_title,
        video_info.thumbnail_link AS thumbnail_link
    FROM 
        video_metrics
        INNER JOIN video_info ON video_metrics.id = video_info.id
        INNER JOIN youtube_categories ON youtube_categories.category_id = video_info.categoryId
)

SELECT * FROM trending_youtube_videos_metrics


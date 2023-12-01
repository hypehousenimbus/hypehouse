with 

staged as (SELECT * FROM {{ ref('stg_video_info') }}),

video_info as (
    SELECT DISTINCT
        concat(file_date,'_',video_id) as id,
        trending_date,
        video_id,
        title,
        description,
        published_at,
        DATEDIFF(day, published_at, trending_date) AS days_to_trend,
        tags,
        categoryId,
        channelId,
        channelTitle,
        comments_disabled,
        ratings_disabled,
        thumbnail_link
    FROM staged
)

select * from video_info
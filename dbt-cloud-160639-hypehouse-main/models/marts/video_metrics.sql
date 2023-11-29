{{ config(materialized='view') }}

WITH

stg_comment_count AS (SELECT * FROM {{ ref('stg_comment_count') }}),
stg_likes_count AS (SELECT * FROM {{ ref('stg_likes_count') }}),
stg_view_count AS (SELECT * FROM {{ ref('stg_view_count') }}),

video_metrics AS (
    SELECT 
           stg_comment_count.id AS id, 
           stg_comment_count.file_date AS file_date,
           stg_comment_count.video_id AS video_id,
           stg_comment_count.comment_count AS comment_count,
           stg_view_count.view_count AS view_count,
           stg_likes_count.likes AS likes,
           stg_likes_count.dislikes AS dislikes
    FROM stg_comment_count
    INNER JOIN stg_likes_count
    ON stg_comment_count.id = stg_likes_count.id 
    INNER JOIN stg_view_count
    ON stg_likes_count.id = stg_view_count.id
)

SELECT * FROM video_metrics

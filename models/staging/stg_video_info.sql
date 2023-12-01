{{ config(materialized='view') }}

with

source as (
    select * from {{ source('raw', 'video_info') }}
),

staged as (
    SELECT DISTINCT
        FILE_NAME,
        TO_DATE(REGEXP_SUBSTR(FILE_NAME, '[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}'), 'YY.DD.MM') as file_date,
        VALUE:index::INTEGER as index,
        VALUE:categoryId_for_client_HyPeHoUsE::INTEGER as categoryId,
        VALUE:channelId_for_client_HyPeHoUsE::STRING as channelId,
        VALUE:channelTitle_for_client_HyPeHoUsE::STRING as channelTitle,
        VALUE:comments_disabled_for_client_HyPeHoUsE::BOOLEAN as comments_disabled,
        VALUE:description_for_client_HyPeHoUsE::STRING as description,
        VALUE:dislikes_for_client_HyPeHoUsE::STRING AS dislikes,
        VALUE:garbled_1_for_client_HyPeHoUsE::STRING AS garbled_1,
        VALUE:garbled_2_for_client_HyPeHoUsE::STRING AS garbled_2,
        VALUE:garbled_3_for_client_HyPeHoUsE::STRING AS garbled_3,
        VALUE:garbled_4_for_client_HyPeHoUsE::STRING AS garbled_4,
        VALUE:garbled_5_for_client_HyPeHoUsE::STRING AS garbled_5,
        VALUE:index::INTEGER AS additional_index,
        TO_TIMESTAMP(VALUE:publishedAt_for_client_HyPeHoUsE::STRING, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS published_at,
        VALUE:ratings_disabled_for_client_HyPeHoUsE::BOOLEAN AS ratings_disabled,
        VALUE:tags_for_client_HyPeHoUsE::STRING AS tags,
        VALUE:thumbnail_link_for_client_HyPeHoUsE::STRING AS thumbnail_link,
        VALUE:title_for_client_HyPeHoUsE::STRING AS title,
        TO_DATE(VALUE:trending_date_for_client_HyPeHoUsE::STRING, 'YY.DD.MM') as trending_date,            
        VALUE:video_id_for_client_HyPeHoUsE::STRING as video_id
    FROM 
        source, 
        LATERAL FLATTEN(input => src:data)
) 

SELECT * FROM staged

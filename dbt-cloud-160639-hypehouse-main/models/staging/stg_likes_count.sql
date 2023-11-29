{{ config(materialized='view') }}

with

source as (
    select * from {{ source('raw', 'likes_count') }}
),

staged as (
    SELECT DISTINCT
        concat(TO_DATE(REGEXP_SUBSTR(FILE_NAME, '[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}'), 'YY.DD.MM'),'_',VALUE:video_id_for_client_HyPeHoUsE::STRING) as id,
        FILE_NAME,
        TO_DATE(REGEXP_SUBSTR(FILE_NAME, '[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}'), 'YY.DD.MM') as file_date,
        VALUE:index::INTEGER as index,
        VALUE:dislikes_for_client_HyPeHoUsE::INTEGER as dislikes,
        VALUE:likes_for_client_HyPeHoUsE::INTEGER as likes,
        VALUE:video_id_for_client_HyPeHoUsE::STRING as video_id
    FROM 
        source, 
        LATERAL FLATTEN(input => src:data)
)

SELECT * FROM staged

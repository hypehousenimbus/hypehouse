{{ config(materialized='view') }}

with

source as (
    SELECT * FROM {{ source('raw', 'view_count') }}
),

staged as (
    SELECT DISTINCT
        concat(TO_DATE(REGEXP_SUBSTR(FILE_NAME, '[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}'), 'YY.DD.MM'),'_',VALUE:video_id_for_client_HyPeHoUsE::STRING) as id,
        FILE_NAME,
        TO_DATE(REGEXP_SUBSTR(FILE_NAME, '[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}'), 'YY.DD.MM') as file_date,
        VALUE:index::INTEGER as index,
        VALUE:video_id_for_client_HyPeHoUsE::STRING as video_id,
        VALUE:view_count_for_client_HyPeHoUsE::INTEGER as view_count
    FROM 
        source,
        LATERAL FLATTEN(input => src:data)
)

select * from staged
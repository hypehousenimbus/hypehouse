{% snapshot video_info %}

{{
    config(
      target_database='raw',
      target_schema='raw',
      unique_key='file_name',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('raw', 'video_info') }}

{% endsnapshot %}
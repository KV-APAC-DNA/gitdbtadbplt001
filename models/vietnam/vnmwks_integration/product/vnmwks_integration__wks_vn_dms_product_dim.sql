{{
  config(
    materialized = 'incremental',
    unique_key =  ['product_code'],
    merge_exclude_columns = ['curr_date','run_id', 'source_file_name']
  )
}}

with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_product_dim') }}
),
final as(
    select * from source
)
select * from final
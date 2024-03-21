{{
  config(
    materialized = 'incremental',
    unique_key =  ['dstrbtr_id','promotion_id'],
    merge_exclude_columns = ['curr_date','run_id', 'source_file_name']
  )
}}

with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_promotion_list') }}
),
final as(
    select * from source
)
select * from final
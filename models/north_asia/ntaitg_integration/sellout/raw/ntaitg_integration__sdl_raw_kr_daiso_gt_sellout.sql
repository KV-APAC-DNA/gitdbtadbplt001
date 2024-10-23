{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_daiso_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_daiso_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_daiso_gt_sellout__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_daiso_gt_sellout__lookup_test') }}
    )
),
final as (
SELECT dstr_nm,
  year,
  month,
  ean,
  name,
  qty,
  sub_customer_name,
  cust_cd,
  sysdate() as crtd_dttm,
  file_name::varchar(255) as file_name
FROM sdl_kr_daiso_gt_sellout
)
select * from final 
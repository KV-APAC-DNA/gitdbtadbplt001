{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=  ['sku','month','branch_code']
      
    )
}}
with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_pos_rks_rose_pharma') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as
(
  select
    branch_code::VARCHAR(30) as branch_code,
	branch_name::VARCHAR(100) as branch_name,
    '20'||split_part(month,'/',1) :: integer as jj_year,
   split_part(month,'/',2) :: integer as  jj_month,
   '20'||replace(month,'/','') :: integer  as jj_month_id,
   qrtr_no  as jj_qtr,
    sku::varchar(100) as sku,
    sku_description::VARCHAR(100) as sku_description,
    qty :: number(20,0) as qty,
    filename::varchar(100) as filename,
    crtd_dttm :: TIMESTAMP_NTZ(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    
from source 
join edw_vw_os_time_dim on (source.jj_month_id=edw_vw_os_time_dim.mnth_id)
)
select * from final
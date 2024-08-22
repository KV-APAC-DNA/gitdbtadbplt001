{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['month']
       
    )
}}
with source as
(
    select * from {{ source('phlsdl_raw'.'sdl_pos_rks_rose_pharma') }}
    
),
final as
(
  select
    branch_code::VARCHAR(30) as branch_code,
	branch_name::VARCHAR(100) as branch_name,
    month::varchar(10) as month,
    sku::varchar(100) as sku,
    sku_description::VARCHAR(100) as sku_description,
    qty :: number(20,0) as qty,
    file_name::varchar(100) as filename,
    crtd_dttm :: TIMESTAMP_NTZ(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    
from source
)
select * from final
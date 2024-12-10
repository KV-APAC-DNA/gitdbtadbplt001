{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        unique_keys=['jj_month_id','branch_code','sku'],
        pre_hook = "delete from {{this}} where filename in
         (
        select distinct filename from {{ source('phlsdl_raw', 'sdl_pos_rks_rose_pharma_consumer') }}
        union 
        select distinct filename from {{ source('phlsdl_raw', 'sdl_pos_rks_rose_pharma') }} 
        );"
    )
}}    
with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_pos_rks_rose_pharma') }}
    union all
     select * from {{ source('phlsdl_raw', 'sdl_pos_rks_rose_pharma_consumer') }}  
       
),
source1 as
(
    select * from source 
    where filename not in (
        select distinct filename from 
         {% if target.name=='prod' %}
                        phlwks_integration.TRATBL_sdl_ph_pos_rosepharma_product__lookup_test
         {% else %}
                        {{schema}}.TRATBL_sdl_ph_pos_rosepharma_product__lookup_test
         {% endif %}
         )
        
),
final as
(
  select
    branch_code::VARCHAR(30) as branch_code,
	branch_name::VARCHAR(100) as branch_name,
    '20'||split_part(month,'/',1) :: integer as jj_year,
   split_part(month,'/',2) :: integer as  jj_month,
   '20'||replace(month,'/','') :: integer  as jj_month_id,
    sku::varchar(100) as sku,
    sku_description::VARCHAR(100) as sku_description,
    qty :: number(20,0) as qty,
    null as pos_gts,
    null as pos_item_prc,
    null as pos_tax,
    null as pos_nts,
    filename::varchar(100) as filename,
    crtd_dttm :: TIMESTAMP_NTZ(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    
from source4
)
select * from final

with source1 as
(
    select * from {{ source('phlsdl_raw', 'sdl_pos_rks_rose_pharma') }}
),
source2 as
(
    select * from {{ source('phlsdl_raw', 'sdl_pos_rks_rose_pharma_consumer') }}
),
source3 as 
(
    select * from source1 
    union 
    select * from  source2
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
    
from source3 
)
select * from final
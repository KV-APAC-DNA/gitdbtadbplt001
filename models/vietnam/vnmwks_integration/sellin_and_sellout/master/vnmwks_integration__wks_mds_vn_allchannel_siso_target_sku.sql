with sdl_mds_vn_allchannel_siso_target_sku as (
    select * from {{ source('vnmsdl_raw', 'sdl_mds_vn_allchannel_siso_target_sku') }}

),
final as (
select 
    cycle::number(31,0) as cycle,
    "data type"::varchar(200) as data_type,
    "jnj code"::number(31,0) as jnj_code,
    description::varchar(200) as description,
    "dksh - mti"::number(31,7) as dksh_mti,
    "dksh - ecom"::number(31,7) as dksh_ecom,
    mtd::number(31,7) as mtd,
    "gt - tien thanh"::number(31,7) as gt_tien_thanh,
    "gt - duong anh"::number(31,7) as gt_duong_anh,
    '0'::varchar(1) as run_id,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    otc::varchar(200) as otc,
from sdl_mds_vn_allchannel_siso_target_sku )
select * from final
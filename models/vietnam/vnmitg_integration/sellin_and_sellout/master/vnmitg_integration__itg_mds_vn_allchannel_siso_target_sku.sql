{{
    config
    (
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["cycle","data_type","jnj_code"]
    )
}}
with wks_mds_vn_allchannel_siso_target_sku as (
    select * from DEV_DNA_CORE.SNAPOSEWKS_INTEGRATION.WKS_MDS_VN_ALLCHANNEL_SISO_TARGET_SKU

),
final as (
select
    cycle::number(10,0) as cycle,
    data_type::varchar(30) as data_type,
    jnj_code::number(31,0) as jnj_code,
    description::varchar(200) as description,
    dksh_mti::number(31,7) as dksh_mti,
    dksh_ecom::number(31,7) as dksh_ecom,
    mtd::number(31,7) as mtd,
    gt_tien_thanh::number(31,7) as gt_tien_thanh,
    gt_duong_anh::number(31,7) as gt_duong_anh,
    run_id::number(14,0) as run_id,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    otc::varchar(100) as otc
from wks_mds_vn_allchannel_siso_target_sku
)
select * from final 
{{
    config
    (
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["cycle","platform"]
    )
}}
with wks_mds_vn_ecom_target as (
    select * from DEV_DNA_CORE.SNAPOSEWKS_INTEGRATION.wks_mds_vn_ecom_target

),
final as (
select
    cycle::number(10,0) as cycle,
    platform::varchar(50) as platform,
    target::number(31,1) as target,
    run_id::number(14,0) as run_id,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from wks_mds_vn_ecom_target
)
select * from final 
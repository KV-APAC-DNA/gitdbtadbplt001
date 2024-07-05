{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook ="{% if is_incremental() %}
                    delete from {{this}} WHERE mnth_id >= '202303';
                    {% endif %}"
    )
}}

with itg_sku_recom_flag as 
(
    select * from {{ ref('inditg_integration__itg_sku_recom_flag_msl_spike') }}
),
final as 
(
    select
        mnth_id::varchar(50) as mnth_id,
        mother_sku_cd::varchar(50) as mother_sku_cd,
        dist_outlet_cd::varchar(50) as dist_outlet_cd,
        cust_cd::varchar(50) as cust_cd,
        oos_flag::varchar(50) as oos_flag,
        ms_flag::varchar(50) as ms_flag,
        cs_flag::varchar(50) as cs_flag,
        soq::varchar(50) as soq,
        unique_ret_cd::varchar(50) as unique_ret_cd,
        retailer_cd::varchar(50) as retailer_cd,
        route_code::varchar(50) as route_code,
        salesman_code::varchar(50) as salesman_code,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    from itg_sku_recom_flag
)
select * from final

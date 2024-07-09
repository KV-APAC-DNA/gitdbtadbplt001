with source as (
    select * from {{ ref('ntaitg_integration__itg_kr_trade_promotion') }}
),
final as (
    select
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        customer_code::varchar(20) as customer_code,
        activity_name::varchar(100) as activity_name,
        product_code::varchar(50) as product_code,
        line_begin_date::date as line_begin_date,
        line_end_date::date as line_end_date,
        or_tp_qty::number(38,5) as or_tp_qty,
        or_tp_rebate_a::number(38,5) as or_tp_rebate_a,
        ttl_cost::number(38,5) as ttl_cost,
        remark::varchar(500) as remark,
        sap_sgrp::varchar(10) as sap_sgrp,
        or_tp_rebate::number(38,5) as or_tp_rebate,
        application_code::varchar(50) as application_code,
        crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final
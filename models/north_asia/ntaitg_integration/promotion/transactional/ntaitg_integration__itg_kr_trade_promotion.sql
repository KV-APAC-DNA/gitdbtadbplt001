{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where (coalesce(customer_code, 'N/A'), product_code, line_begin_date, line_end_date, sap_sgrp, application_code) in (select distinct coalesce(cus_code, 'N/A'), pr_code, to_date(line_begin_date, 'YYYY-MM-DD'), to_date(line_end_date, 'YYYY-MM-DD'), sap_sgrp, application_code from {{ source('ntasdl_raw', 'sdl_kr_trade_promotion') }});
        {% endif %}"
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_trade_promotion') }}
),
transformed as (
    SELECT 
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        cus_code as customer_code,
        act_name as activity_name,
        pr_code as product_code,
        to_date(line_begin_date, 'YYYY-MM-DD') as line_begin_date,
        to_date(line_end_date, 'YYYY-MM-DD') as line_end_date,
        or_tp_qty as or_tp_qty,
        or_tp_rebate_a as or_tp_rebate_a,
        ttl_cost as ttl_cost,
        remark as remark,
        sap_sgrp as sap_sgrp,
        or_tp_rebate as or_tp_rebate,
        application_code as application_code,
        convert_timezone('UTC', current_timestamp()) as crt_dttm
    from source
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
    from transformed
)
select * from final
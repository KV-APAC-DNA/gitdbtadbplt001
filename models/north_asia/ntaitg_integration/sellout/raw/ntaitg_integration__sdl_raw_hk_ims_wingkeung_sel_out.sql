{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_hk_ims_wingkeung_sel_out') }}
),
final as (
    select calendar_sid,
        sales_office,
        sales_group,
        sales_office_name,
        sales_group_name,
        account_types,
        sales_volume,
        sales_order_quantity,
        net_trade_sales,
        customer_name,
        customer_number,
        base_product,
        variant,
        mvgr1_base,
        mvgr2_variant,
        mega_brand,
        brand,
        mvgr4_mega,
        mvgr5_brand,
        product_description,
        product_number,
        local_curr_exch_rate,
        employee,
        employee_name,
        transactiontype,
        return_reason,
        country_code,
        currency,
        crt_dttm,
        updt_dttm as upd_dttm,
        null::varchar(100) as filename,
        null::varchar(14) as run_id
    from source
)
select * from final
{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_api_dstr') }}
),
final as
(
    select 
        invt_dt,
        article_id,
        article_desc,
        product_ean,
        site_id,
        site_desc,
        vendor,
        vendor_desc,
        product_sap_id,
        cost_price,
        cross_site_status,
        month_13,
        month_12,
        month_11,
        month_10,
        month_09,
        month_08,
        month_07,
        month_06,
        month_05,
        month_04,
        month_03,
        month_02,
        month_01,
        null_coloumn,
        mth_total_invoiced_qty,
        soh_qty,
        dc_soo_qty,
        so_backorder_qty,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final

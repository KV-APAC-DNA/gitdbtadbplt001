{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw', 'sdl_distributor_ivy_order') }}
),
final as (
    select 
        distributor_code,
        user_code,
        retailer_code,
        route_code,
        order_date,
        order_id,
        product_code,
        uom,
        uom_count,
        qty,
        line_value,
        piece_price,
        order_value,
        lines_per_call,
        scheme_code,
        scheme_description,
        scheme_discount,
        scheme_precentage,
        billdiscount,
        billdisc_percentage,
        po_number,
        payment_type,
        delivery_date,
        invoice_address,
        shipping_address,
        order_status,
        order_latitude,
        order_longitude,
        cdl_dttm,
        run_id,
        source_file_name
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where cdl_dttm > (select coalesce(max(cdl_dttm),'1971-01-01 00:00:00.000000') from {{ this }}) 
    {% endif %}
)

select * from final
{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw', 'sdl_distributor_ivy_invoice') }}
),
final as (
    select 
        distributor_code,
        user_code,
        retailer_code,
        invoice_date,
        order_id,
        invoice_no,
        product_code,
        uom,
        uom_count,
        qty,
        piece_price,
        line_value,
        invoice_amount,
        lines_per_call,
        scheme_code,
        scheme_description,
        scheme_discount,
        scheme_percentage,
        billdiscount,
        billdisc_percentage,
        po_number,
        payment_type,
        exp_delivery_date,
        invoice_address,
        shipping_address,
        invoice_status,
        efaktur_no,
        tax_value,
        batch_no,
        cdl_dttm,
        run_id,
        source_file_name
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where cdl_dttm > (select coalesce(max(cdl_dttm),'1971-01-01 00:00:00.000000') from {{ this }} ) 
    {% endif %}
)

select * from final
with source as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_sm_goods') }}
),
final as 
(
    select 
        business_name,
        title,
        date,
        vendor_code,
        vendor_name,
        receipt_date,
        terms_and_discount,
        site_code,
        site_name,
        ship_to,
        gr_number,
        po_number,
        cancel_date,
        total_articles,
        article_number,
        article_description,
        upc,
        uom,
        received_qty,
        remarks,
        file_name,
        crt_dttm
    from source
)
select * from final
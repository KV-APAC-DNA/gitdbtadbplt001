with source as (
    select * from snapntaitg_integration.itg_sfmc_invoice_data
),
final as
(
    select 
        cntry_cd as country_code,
        purchase_date,
        invoice_num as invoive_number,
        channel as channel_name,
        product as product_name,
        status,
        created_date,
        completed_date,
        subscriber_key,
        points,
        show_record,
        qty as quantity,
        invoice_type,
        seller_nm as seller_name,
        product_category,
        website_unique_id
    from source
)
select * from source
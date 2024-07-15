with source as (
    select * from {{ ref('aspedw_integration__edw_vw_regaine_tw_invoice_record_master') }}
),
final as (
    select
        amount as "amount",
        channel as "channel",
        completed_date as "completed_date",
        created_date as "created_date",
        invoice_number as "invoice_number",
        invoice_type as "invoice_type",
        points as "points",
        product as "product",
        product_category as "product_category",
        purchase_date as "purchase_date",
        quantity as "quantity",
        seller_name as "seller_name",
        show_record as "show_record",
        status as "status",
        subscriber_key as "subscriber_key",
        website_unique_id as "website_unique_id",
        full_name as "full_name",
        email as "email"
    from source
)
select * from final
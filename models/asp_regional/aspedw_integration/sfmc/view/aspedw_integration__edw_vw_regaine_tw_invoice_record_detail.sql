with itg_sfmc_invoice_data as (
    select * from {{ ref('aspitg_integration__itg_sfmc_invoice_data') }}
),
itg_sfmc_consumer_master as (
    select * from {{ ref('aspitg_integration__itg_sfmc_consumer_master') }}
),
final as (
    select c.epsilon_amount as amount,
        c.channel as channel,
        c.completed_date as completed_date,
        c.created_date as created_date,
        c.invoice_num as invoice_number,
        c.invoice_type as invoice_type,
        c.points as points,
        c.epsilon_price_per_unit as price_per_unit,
        c.product as product,
        c.product_category as product_category,
        c.purchase_date as purchase_date,
        c.qty as quantity,
        c.seller_nm as seller_name,
        c.show_record as show_record,
        c.status as status,
        c.subscriber_key as subscriber_key,
        c.website_unique_id as website_unique_id,
        d.full_name as full_name,
        d.email as email
    from itg_sfmc_invoice_data c
        left join itg_sfmc_consumer_master d on lower(c.subscriber_key) = lower(d.subscriber_key)
    where c.subscriber_key ilike 'Regaine_TW_%'
)
select * from final
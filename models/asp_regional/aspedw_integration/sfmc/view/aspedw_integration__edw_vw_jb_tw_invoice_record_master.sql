with itg_sfmc_invoice_data as (
    select * from {{ ref('aspitg_integration__itg_sfmc_invoice_data') }}
),
itg_sfmc_consumer_master as (
    select * from {{ ref('aspitg_integration__itg_sfmc_consumer_master') }}
),
final as (
    select any_value(c.epsilon_total_amount) as amount,
        any_value(c.channel) as channel,
        any_value(c.completed_date) as completed_date,
        any_value(c.created_date) as created_date,
        c.invoice_num as invoice_number,
        any_value(c.invoice_type) as invoice_type,
        sum(c.points) as points,
        any_value(c.product) as product,
        any_value(c.product_category) as product_category,
        any_value(c.purchase_date) as purchase_date,
        any_value(c.qty) as quantity,
        any_value(c.seller_nm) as seller_name,
        any_value(c.show_record) as show_record,
        any_value(c.status) as status,
        any_value(c.subscriber_key) as subscriber_key,
        min(c.website_unique_id) as website_unique_id,
        any_value(d.full_name) as full_name,
        any_value(d.email) as email
    from itg_sfmc_invoice_data c
        left join itg_sfmc_consumer_master d on lower(c.subscriber_key) = lower(d.subscriber_key)
    where c.subscriber_key ilike 'JB_TW_%'
        and c.status = 'Approved'
    group by c.invoice_num
)
select * from final
{{
    config(
        transient = false
    )
}}
with source as (
    select * from {{ ref('sgpitg_integration__itg_sg_pos_sales_fact') }}
),
final as (
    select distinct 
        cust_name as customer_name,
        brand as customer_brand,
        item_code as customer_product_code,
        item_desc as customer_product_name,
        source,
        count(*) as record_count
    from source where product_key is null group by 1,2,3,4,5
)
select * from final
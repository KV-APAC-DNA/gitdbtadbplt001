with source as (
    select * from snapntaitg_integration.itg_sfmc_redemption_data
),
final as
(    
    select 
        cntry_cd as country_code,
        prod_nm as product_name,
        redeemed_points,
        qty as quantity,
        redeemed_date,
        status as redemption_status,
        completed_date,
        subscriber_key,
        created_date,
        order_num as order_number,
        website_unique_id
    from source
)
select * from final
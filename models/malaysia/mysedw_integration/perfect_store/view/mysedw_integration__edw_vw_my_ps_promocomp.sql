with source as 
(
    select * from snaposeitg_integration.itg_my_perfectstore_promocomp
),

final as 
(
    SELECT 
        yearmo,
        date,
        channel,
        chain,
        region,
        outlet,
        outlet_no,
        category,
        brand,
        activation,
        promo_comp_on_time,
        promo_comp_on_time_in_full,
        promo_comp_successfully_set_up,
        non_compliance_reason
    FROM source
)
select * from final
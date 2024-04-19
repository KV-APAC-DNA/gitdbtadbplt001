with source as
(
    select * from {{ ref('pcfitg_integration__itg_px_forecast') }}
),
final as
(  
    select
    ac_code::varchar(10) as ac_code,
    ac_attribute::varchar(20) as ac_attribute,
    sku_stockcode::varchar(18)as sku_stockcode,
    sku_attribute::varchar(20) as sku_attribute,
    sku_profitcentre::varchar(10) as sku_profitcentre,
    est_date::date as est_date,
    est_normal::number(18,0) as est_normal,
    est_promotional::number(18,0) as est_promotional,
    est_estimate::number(18,0) as est_estimate
    from source
    where (est_normal <> 0) or (est_promotional <> 0) or (est_estimate <> 0)
)
select * from final
with source as
(
    select * from {{ ref('pcfitg_integration__itg_px_apn') }}
),
final as
(  select 
    sku_apncode::varchar(18) as sku_apncode,
    apn::number(38,0) as apn,
    sku_stockcode::varchar(18) as sku_stockcode
  from source
)
select * from final
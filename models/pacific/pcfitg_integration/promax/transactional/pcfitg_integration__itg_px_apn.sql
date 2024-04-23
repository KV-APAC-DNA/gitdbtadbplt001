with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_px_apn') }}
),
final as
(  select 
    sku_apncode::varchar(18) as sku_apncode,
    apn::number(38,0) as apn,
    sku_stockcode::varchar(18) as sku_stockcode
  from source
)
select * from final
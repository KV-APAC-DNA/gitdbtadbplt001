with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_px_listprice') }}
),
final as(  
  select
    sku_stockcode::varchar(18) as sku_stockcode,
    lp_price::float as lp_price,
    lp_cost::float as lp_cost,
    lp_salestax::float as lp_salestax,
    lp_startdate::timestamp_ntz(9) as lp_startdate,
    lp_stopdate::timestamp_ntz(9) as lp_stopdate,
    sku_uompersaleable::number(18,0) as sku_uompersaleable,
    sales_org::varchar(10) as sales_org
  from source
)
select * from final
with source as
(
    select * from {{ ref('pcfitg_integration__itg_px_listprice') }}
),
transformed as
(
    select *, 
        case 
            when lp_stopdate is null then dateadd('days', 1800, current_timestamp()) 
            else lp_stopdate 
        end as new_lp_stopdate
    from source

),
final as(  
  select
    sku_stockcode::varchar(18) as sku_stockcode,
    lp_price::float as lp_price,
    lp_cost::float as lp_cost,
    lp_salestax::float as lp_salestax,
    lp_startdate::timestamp_ntz(9) as lp_startdate,
    new_lp_stopdate::timestamp_ntz(9) as lp_stopdate,
    sku_uompersaleable::number(18,0) as sku_uompersaleable,
    sales_org::varchar(10) as sales_org
  from transformed
)
select * from final
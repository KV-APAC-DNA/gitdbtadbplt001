with source as(
    select * from {{ ref('pcfitg_integration__itg_px_uom') }}
),
final as(
    select 
        sku_longname::varchar(40) as sku_longname,
        sku_stockcode::varchar(18) as sku_stockcode,
        sku_uom::varchar(3) as sku_uom,
        sku_uompersaleable::number(38,0) as sku_uompersaleable,
        sku_packspercase::number(38,0) as sku_packspercase
    from source
)
select * from final
with source as(
    select * from {{ ref('pcfitg_integration__itg_px_scan_data') }}
),
final as(
    select 
        ac_shortname::varchar(40) as ac_shortname,
        ac_longname::varchar(40) as ac_longname,
        ac_code::varchar(50) as ac_code,
        ac_attribute::varchar(20) as cust_id,
        sc_date::date as sc_date,
        sc_scanvolume::float as sc_scanvolume,
        sc_scanvalue::float as sc_scanvalue,
        sc_scanprice::float as sc_scanprice,
        sku_shortname::varchar(40) as sku_shortname,
        sku_longname::varchar(40) as sku_longname,
        sku_tuncode::varchar(50) as sku_tuncode,
        sku_apncode::varchar(50) as sku_apncode,
        sku_stockcode::varchar(18) as matl_id,
        ac_salesorg::varchar(10) as ac_salesorg
    from source
)
select * from final
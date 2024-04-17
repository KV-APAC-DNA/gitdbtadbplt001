with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_px_scan_data') }}
),
transformed as(
    select 
        ac_shortname as ac_shortname,
        ac_longname as ac_longname,
        ac_code as ac_code,
        ac_attribute as ac_attribute,
        sc_date as sc_date,
        sc_scanvolume as sc_scanvolume,
        sc_scanvalue as sc_scanvalue,
        sc_scanprice as sc_scanprice,
        sku_shortname as sku_shortname,
        sku_longname as sku_longname,
        sku_tuncode as sku_tuncode,
        sku_apncode as sku_apncode,
        sku_stockcode as sku_stockcode,
        ac_salesorg as ac_salesorg
    from source
),
final as(
    select 
        ac_shortname::varchar(40) as ac_shortname,
        ac_longname::varchar(40) as ac_longname,
        ac_code::varchar(50) as ac_code,
        ac_attribute::varchar(20) as ac_attribute,
        sc_date::date as sc_date,
        sc_scanvolume::float as sc_scanvolume,
        sc_scanvalue::float as sc_scanvalue,
        sc_scanprice::float as sc_scanprice,
        '2682308504'::varchar(40) as sku_shortname,
        sku_longname::varchar(40) as sku_longname,
        sku_tuncode::varchar(50) as sku_tuncode,
        sku_apncode::varchar(50) as sku_apncode,
        '2682308504'::varchar(18) as sku_stockcode,
        ac_salesorg::varchar(10) as ac_salesorg
    from transformed where sku_stockcode = '2682308501'
)
select * from final
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['sc_date'],
        pre_hook= "delete from {{this}} where sc_date >= DATEADD('month', -18, CURRENT_DATE());
"
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_px_scan_data') }}
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
        case when sku_stockcode= '2682308501'
        then sku_shortname='2682308504'
        else sku_shortname end::varchar(40) as sku_shortname,
        sku_longname::varchar(40) as sku_longname,
        sku_tuncode::varchar(50) as sku_tuncode,
        sku_apncode::varchar(50) as sku_apncode,
        case when sku_stockcode= '2682308501'
        then sku_stockcode='2682308504'
        else sku_stockcode end::varchar(18) as sku_stockcode,
        ac_salesorg::varchar(10) as ac_salesorg
    from source
)
select * from final
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=["p_startdate"],
        pre_hook= "delete from {{this}} where p_startdate >= DATEADD(MONTH, -42, current_date())::timestamp_ntz"
    )
}}
with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_px_master') }}
),
final as
(
  select 
    ac_code::varchar(50) as ac_code,
    ac_longname::varchar(40) as ac_longname,
    ac_attribute::varchar(20) as ac_attribute,
    p_promonumber::varchar(10) as p_promonumber,
    p_startdate::timestamp_ntz(9) as p_startdate,
    p_stopdate::timestamp_ntz(9) as p_stopdate,
    promo_length::number(38,0) as promo_length,
    p_buystartdatedef::timestamp_ntz(9) as p_buystartdatedef,
    p_buystopdatedef::timestamp_ntz(9) as p_buystopdatedef,
    buyperiod_length::number(38,0) as buyperiod_length,
    hierarchy_rowid::number(18,0) as hierarchy_rowid,
    hierarchy_longname::varchar(40) as hierarchy_longname,
    activity_longname::varchar(40) as activity_longname,
    confirmed_switch::number(38,0) as confirmed_switch,
    closed_switch::number(38,0) as closed_switch,
    sku_longname::varchar(40) as sku_longname,
    sku_stockcode::varchar(18) as sku_stockcode,
    sku_profitcentre::varchar(10) as sku_profitcentre,
    sku_attribute::varchar(20) as sku_attribute,
    gltt_rowid::number(18,0) as gltt_rowid,
    transaction_longname::varchar(40) as transaction_longname,
    case_deal::float as case_deal,
    case_quantity::number(18,0) as case_quantity,
    planspend_total::float as planspend_total,
    paid_total::float as paid_total,
    p_deleted::number(38,0) as p_deleted,
    transaction_attribute::varchar(10) as transaction_attribute,
    promotionrowid::number(18,0) as promotionrowid,
    normal_qty::number(18,0) as normal_qty
  from source
)
select * from final
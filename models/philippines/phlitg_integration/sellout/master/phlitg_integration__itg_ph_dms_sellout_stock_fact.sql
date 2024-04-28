{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_grp_cd', 'inv_dt'],
        pre_hook= ["delete from {{this}} where dstrbtr_grp_cd || inv_dt in ( select distinct dstrbtr_grp_cd || to_date(invoice_dt, 'YYYYMMDD') from {{ source('phlsdl_raw', 'sdl_ph_dms_sellout_stock_fact') }} );"]
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_ph_dms_sellout_stock_fact') }}
),
final as(
    select 
        dstrbtr_grp_cd::varchar(20) as dstrbtr_grp_cd,
        cntry_cd::varchar(20) as cntry_cd,
        wh_cd::varchar(20) as wh_cd,
        to_date(invoice_dt,'YYYYMMDD') as inv_dt,
        dstrbtr_prod_id::varchar(20) as dstrbtr_prod_id,
        cast(qty as numeric(15, 4)) as qty,
        uom::varchar(20) as uom,
        cast(amt as numeric(15, 4)) as amt,
        cdl_dttm as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        null::timestamp_ntz(9)  as updt_dttm
    from source
)

select * from final
{{
    config(
        materialized="incremental",
        incremental_strategy = "delete+insert",
        unique_key=["cust_id","inv_dt"]
    )
}}

with source as (
    select * from {{ source('myssdl_raw', 'sdl_my_monthly_sellout_stock_fact') }}
),

imier as (
    select * from {{ ref('mysitg_integration__itg_my_ids_exchg_rate') }}
),

logical as (
    select
        source.cust_id as cust_id,
        to_date(inv_dt, 'DD.MM.YYYY') as inv_dt,
        dstrbtr_wh_id,
        item_cd,
        dstrbtr_prod_cd,
        ean_num,
        dstrbtr_prod_desc,
        cast(replace(qty, ',', '') as decimal(20, 4)) as qty,
        uom,
        cast(replace(qty_on_ord, ',', '') as decimal(20, 4)) as qty_on_ord,
        uom_on_ord as uom_on_ord,
        cast(replace(qty_committed, ',', '') as decimal(20, 4)) as qty_committed,
        uom_committed as uom_committed,
        cast(replace(available_qty_pc, ',', '') as decimal(20, 4)) as available_qty_pc,
        cast(replace(qty_on_ord_pc, ',', '') as decimal(20, 4)) as qty_on_ord_pc,
        cast(replace(qty_committed_pc, ',', '') as decimal(20, 4)) as qty_committed_pc,
        cast(unit_prc as decimal(20, 4)) as unit_prc,
        cast(total_val as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as total_val,
        custom_field1,
        custom_field2 as sap_matl_num,
        filename,
        source.cdl_dttm as cdl_dttm,
        source.curr_dt as crtd_dttm,
        current_timestamp() as updt_dttm
    from source, imier
    where imier.cust_id(+) = source.cust_id
        and imier.yearmo(+) = replace(substring(to_date(inv_dt, 'DD.MM.YYYY'), 0, 7), '-', '')

),

final as (
    select 
        cust_id::varchar(50) as cust_id,
        inv_dt::date as inv_dt,
        dstrbtr_wh_id::varchar(50) as dstrbtr_wh_id,
        item_cd::varchar(50) as item_cd,
        dstrbtr_prod_cd::varchar(50) as dstrbtr_prod_cd,
        ean_num::varchar(50) as ean_num,
        dstrbtr_prod_desc::varchar(100) as dstrbtr_prod_desc,
        qty::number(20,4) as qty,
        uom::varchar(20) as uom,
        qty_on_ord::number(20,4) as qty_on_ord,
        uom_on_ord::varchar(100) as uom_on_ord,
        qty_committed::number(20,4) as qty_committed,
        uom_committed::varchar(100) as uom_committed,
        available_qty_pc::number(20,4) as available_qty_pc,
        qty_on_ord_pc::number(20,4) as qty_on_ord_pc,
        qty_committed_pc::number(20,4) as qty_committed_pc,
        unit_prc::number(20,4) as unit_prc,
        total_val::number(20,4) as total_val,
        custom_field1::varchar(255) as custom_field1,
        sap_matl_num::varchar(255) as sap_matl_num,
        filename::varchar(255) as filename,
        cdl_dttm::varchar(255) as cdl_dttm,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from logical
)

select * from final
{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw','sdl_id_pos_idm_stock') }}
),

final as (
    select
        no as no,
        item as item,
        branch as branch,
        dc_qty as dc_qty,
        store_qty as store_qty,
        units as units,
        pos_cust as pos_cust,
        yearmonth as yearmonth,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        filename as filename
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        where crtd_dttm > (select max(crtd_dttm) from {{ this }})
    {% endif %}
)

select * from final

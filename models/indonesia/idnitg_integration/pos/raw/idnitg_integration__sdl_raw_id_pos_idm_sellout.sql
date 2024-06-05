{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('idnsdl_raw','sdl_id_pos_idm_sellout') }}
),
final as(
    select 
        item as item,
        description as description,
        plu as plu,
        branch as branch,
        type as type,
        "values" as "values",
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
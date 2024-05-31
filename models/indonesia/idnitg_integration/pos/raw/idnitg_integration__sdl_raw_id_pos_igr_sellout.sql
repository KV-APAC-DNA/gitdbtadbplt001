{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw','sdl_id_pos_igr_sellout') }}
),

final as (
    select
        no,
        description,
        branch,
        type,
        "values",
        pos_cust,
        yearmonth,
        run_id,
        crtd_dttm,
        filename
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        where crtd_dttm > (select max(crtd_dttm) from {{ this }})
    {% endif %}
)

select * from final

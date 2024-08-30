{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw','sdl_id_pos_matahari_jb_stock') }}
    where filename not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_matahari_jb_stock__null_test') }}
    )
),

final as (
    select *
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        where crtd_dttm > (select max(crtd_dttm) from {{ this }})
    {% endif %}
)

select * from final

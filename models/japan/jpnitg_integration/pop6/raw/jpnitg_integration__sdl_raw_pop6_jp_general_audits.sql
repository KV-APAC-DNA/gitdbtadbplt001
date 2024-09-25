{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_general_audits') }}
    where file_name not in (
            select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_general_audits__null_test') }}
            union all
            select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_general_audits__test_duplicate') }}
    )
),

final as
(
    select * from source
        {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        where crtd_dttm > (select max(crtd_dttm) from {{ this }})
        {% endif %}
)

select * from final
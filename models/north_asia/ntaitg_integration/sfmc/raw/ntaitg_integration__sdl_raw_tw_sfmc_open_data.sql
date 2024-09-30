{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_tw_sfmc_open_data') }}
    where file_name not in (
            select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_sfmc_open_data__format_test') }}
            union all
            select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_sfmc_open_data__test_duplicate__ff') }}
			union all
            select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_sfmc_open_data__test_lookup__ff') }}
			union all
            select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_sfmc_open_data__test_null__ff') }}
    )
),

final as
(
    select
        *
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
{% endif %} 
)

select * from final
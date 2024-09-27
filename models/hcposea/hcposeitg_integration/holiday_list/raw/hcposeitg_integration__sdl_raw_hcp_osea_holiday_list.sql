{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with sdl_hcp_osea_holiday_list as (
    select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_holiday_list') }}
    where filename not in (
            select distinct file_name from {{ source('hcposewks_integration', 'TRATBL_sdl_hcp_osea_holiday_list__null_test') }}
            union all
            select distinct file_name from {{ source('hcposewks_integration', 'TRATBL_sdl_hcp_osea_holiday_list__duplicate_test') }}
    )
),

result as (
    select * from sdl_hcp_osea_holiday_list 
),

final as (
    select
        country::varchar(5) as country,
        holiday_key::varchar(20) as holiday_key,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        filename::varchar(100) as filename,
        run_id::varchar(14) as run_id
    from result
    {% if is_incremental() %}
        where result.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final
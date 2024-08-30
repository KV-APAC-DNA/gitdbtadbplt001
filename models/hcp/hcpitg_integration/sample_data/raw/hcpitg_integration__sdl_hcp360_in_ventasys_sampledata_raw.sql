{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_sampledata') }}
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_sampledata__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_sampledata__duplicate_test') }}
    )
),
final as(
    select * from source
)

select * from final
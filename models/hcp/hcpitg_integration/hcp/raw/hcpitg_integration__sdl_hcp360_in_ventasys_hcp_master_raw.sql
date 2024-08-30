{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_hcp_master') }}
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_hcp_master__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_hcp_master__duplicate_test') }}
    )
),
final as(
    select * from source
)

select * from final
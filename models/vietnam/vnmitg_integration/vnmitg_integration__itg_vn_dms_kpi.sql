{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['from_cycle','to_cycle','customer_code']
    )
}}

with source as (
    select * from {{ source('snaposesdl_raw','sdl_vn_dms_kpi') }}
),

final as
(
    select
   
    from source
)
select * from final
{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_hk_ims_viva_sel_out') }}
),
final as (
    select *,
        null::varchar(100) as filename,
        null::varchar(14) as run_id
    from source
)
select * from final
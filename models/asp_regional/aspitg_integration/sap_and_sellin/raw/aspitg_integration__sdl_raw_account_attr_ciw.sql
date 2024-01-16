{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
     select * from {{ ref('aspitg_integration__vw_stg_sdl_account_attr_ciw') }} ),
final as (
    select * from source
    -- Need to add delta logic
)

select * from final
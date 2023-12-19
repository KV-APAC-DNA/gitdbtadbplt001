{{
    config(
        materialized="view",
        alias="vw_stg_sdl_sap_bw_strongholds_text",
        tags=["sap_bw"]
    )
}}

with sources as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_strongholds_text') }}

),

final as(
    select * from sources
)

select * from final
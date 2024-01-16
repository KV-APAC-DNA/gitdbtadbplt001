with sources as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_strongholds_text') }}

),

final as(
    select * from sources
)

select * from final
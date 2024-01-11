with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_price_list') }}
),
final as(
    select * from source
)

select * from final
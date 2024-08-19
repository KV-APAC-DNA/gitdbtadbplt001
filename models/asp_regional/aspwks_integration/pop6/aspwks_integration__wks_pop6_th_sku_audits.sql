with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_sku_audits_test') }}
),

final as
(
    select * from source
)
select * from final
with 
sdl_ittarget as 
(
    select * from {{ source('indsdl_raw', 'sdl_ittarget') }}
),
final as 
(
    SELECT 
    lakshyat_territory_name,
    target_variant,
    janamount,
    febamount,
    maramount,
    apramount,
    mayamount,
    junamount,
    julyamount,
    augamount,
    sepamount,
    octamount,
    novamount,
    decamount,
    ytdamount
    FROM sdl_ittarget
)
select * from final
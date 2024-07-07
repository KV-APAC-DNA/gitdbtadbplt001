with sdl_lks_businesscalender as
(
    select * from {{ source('indsdl_raw', 'sdl_lks_businesscalender') }}
),
final as
(
    SELECT 
        src.salinvdate::timestamp_ntz(9) as salinvdate,
        month::varchar(30) as month,
        year::number(18,0) as year,
        week::varchar(50) as week,
        monthkey::number(18,0) as monthkey,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM (select distinct * from sdl_lks_businesscalender) src
)
select * from final
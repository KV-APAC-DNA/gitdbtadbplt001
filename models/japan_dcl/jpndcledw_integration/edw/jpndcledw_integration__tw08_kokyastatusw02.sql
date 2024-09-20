with tw08_kokyastatusw01 as (
    select * from {{ ref('jpndcledw_integration__tw08_kokyastatusw01') }}
),	
transformed as(
select
    kokyano
    ,juchdate
    ,konyukaisu
    ,row_number() over (partition by kokyano order by juchdate) as konyunisu
from  tw08_kokyastatusw01 t
),
final as (
    select
        kokyano::varchar(15) as kokyano,
        juchdate::number(18,0) as juchdate,
        konyukaisu::number(38,18) as konyukaisu,
        konyunisu::number(38,18) as konyunisu,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final

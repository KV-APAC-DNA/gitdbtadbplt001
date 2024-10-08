with tw06_02konyusihyokbn as (
    select * from {{ ref('jpndcledw_integration__tw06_02konyusihyokbn') }}
),
tm28rfmsegment as (
    select * from {{ source('jpdcledw_integration', 'tm28rfmsegment') }}
),

transformed as (
    select
        f2.kokyano
        ,ifnull(segkbncode,'00') as segkbncode
    from 
        tw06_02konyusihyokbn f2
    left join tm28rfmsegment m28
        on  m28.rkbncode = f2.konyurkbncode
        and m28.fkbncode = f2.nenfkbncode
        and m28.mkbncode = f2.ruimkbncode
),
final as (
    select
        kokyano::varchar(15) as kokyano,
        segkbncode::varchar(3) as segkbncode,  
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final

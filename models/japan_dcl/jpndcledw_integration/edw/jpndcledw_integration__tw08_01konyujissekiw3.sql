with TW08_01KONYUJISSEKIW1 as (
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.TW08_01KONYUJISSEKIW1
),
TW08_KOKYASTATUSW03 as (
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.TW08_KOKYASTATUSW03
),	
transformed as (
    select
        W1.SALENO   AS SALENO
        ,W1.JUCHDATE AS JUCHDATE
        ,W1.KOKYANO  AS KOKYANO
        ,ifnull((CASE WHEN W03.KONYUDATESU02 <> 0 THEN ROUND(W1.KEIKADAYSKEI / W03.KONYUDATESU02) ELSE 0 END),0) AS RUIINDAYS
        ,(CASE WHEN W03.KONYUDATESU03 <> 0 THEN ROUND(W1.NENKEIKADAYSKEI / W03.KONYUDATESU03) ELSE 0 END) AS NENINDAYS
    from 
        TW08_01KONYUJISSEKIW1 W1
    INNER JOIN  
        TW08_KOKYASTATUSW03 W03 ON W1.SALENO = W03.SALENO
),
final as (
    select
        saleno::varchar(61) as saleno,
        juchdate::number(38,18) as juchdate,
        kokyano::varchar(60) as kokyano,
        ruiindays::number(38,18) as ruiindays,
        nenindays::number(38,18) as nenindays,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final

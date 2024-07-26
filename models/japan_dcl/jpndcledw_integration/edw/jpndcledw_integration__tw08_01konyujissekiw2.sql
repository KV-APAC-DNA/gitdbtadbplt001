with tw08_01konyujissekiw1 as (
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.tw08_01konyujissekiw1
),
transformed as (
    select
        SALENO
        ,JUCHDATE 
        ,KOKYANO
        ,CASE WHEN LASTJUCHDATE <> 0 THEN TO_DATE(JUCHDATE::INTEGER::STRING,'YYYYMMDD') - TO_DATE(LASTJUCHDATE::INTEGER::STRING,'YYYYMMDD') ELSE 0 END AS JUCHUKEIKADAYS
        ,CASE WHEN LASTKONYUDATE <> 0 THEN TO_DATE(JUCHDATE::INTEGER::STRING,'YYYYMMDD') - TO_DATE(LASTKONYUDATE::INTEGER::STRING,'YYYYMMDD') ELSE 0 END AS KONYUKEIKADAYS
    from 
        tw08_01konyujissekiw1
),
final as (
    select
        saleno::varchar(61) as saleno,
        juchdate::number(38,18) as juchdate,
        kokyano::varchar(60) as kokyano,
        juchukeikadays::number(38,18) as juchukeikadays,
        konyukeikadays::number(38,18) as konyukeikadays,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final

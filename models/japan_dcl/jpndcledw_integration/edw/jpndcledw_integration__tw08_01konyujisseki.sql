with tw08_01konyujissekiw1 as (
    select * from {{ ref('jpndcledw_integration__tw08_01konyujissekiw1') }}
),
tw08_kokyastatusw03 as (
    select * from {{ ref('jpndcledw_integration__tw08_kokyastatusw03') }}
),
tw08_01konyujissekiw2 as (
    select * from  {{ ref('jpndcledw_integration__tw08_01konyujissekiw2') }}
),
tw08_01konyujissekiw3 as (
    select * from  {{ ref('jpndcledw_integration__tw08_01konyujissekiw3') }}
),
transformed as (
    select
        t1.saleno                   as saleno
        ,w3.konyudatesu01           as ruikaisu
        ,nvl(t1.ruikingaku,0)       as ruikingaku
        ,nvl(t3.ruiindays, 0)       as ruiindays
        ,t1.lastjuchdate            as lastjuchdate
        ,t2.juchukeikadays          as juchukeikadays
        ,t1.lastkonyudate           as lastkonyudate
        ,t2.konyukeikadays          as konyukeikadays
        ,w3.konyudatesu04           as nenkaisu
        ,t1.nenkingaku              as nenkingaku
        ,t3.nenindays               as nenindays
        ,0                          as nengelryo
        ,0                          as tsukigelryo
    from tw08_01konyujissekiw1 t1
    inner join tw08_kokyastatusw03   w3 on t1.saleno = w3.saleno
    inner join tw08_01konyujissekiw2 t2 on t1.saleno = t2.saleno
    inner join tw08_01konyujissekiw3 t3 on t1.saleno = t3.saleno
),
final as (
    select
        saleno::varchar(18) as saleno,
        ruikaisu::number(18,0) as ruikaisu,
        ruikingaku::number(38,0) as ruikingaku,
        ruiindays::number(18,0) as ruiindays,
        lastjuchdate::number(18,0) as lastjuchdate,
        juchukeikadays::number(18,0) as juchukeikadays,
        lastkonyudate::number(18,0) as lastkonyudate,
        konyukeikadays::number(18,0) as konyukeikadays,
        nenkaisu::number(38,0) as nenkaisu,
        nenkingaku::number(38,0) as nenkingaku,
        nenindays::number(18,0) as nenindays,
        nengelryo::number(18,0) as nengelryo,
        tsukigelryo::number(5,1) as tsukigelryo,     
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final

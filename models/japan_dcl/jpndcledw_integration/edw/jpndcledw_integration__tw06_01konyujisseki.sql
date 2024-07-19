with tt05kokyakonyu as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tt05kokyakonyu
),
tt01kokyastsh_mv as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tt01kokyastsh_mv
),
tw05kokyarecalc as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tw05kokyarecalc
),
ttxx as (
    select
        kokyano
        , min(juchdate) as firstkonyudate
        , max(juchdate) as lastkonyudate
        , min(case when torikeikbn = '01' then juchdate else 99991231 end) as firsttsuhandate
        , min(case when torikeikbn in ('03','04','05') then juchdate else 99991231 end) as firsttenpodate
        , max(zaisekidays) as zaisekidays
        , max(zaisekimonth) as zaisekimonth
        , count(distinct juchdate) as ruikaisu
        , sum(meisainukikingaku) as ruikingaku
        , case
                when max(zaisekidays) = 0 then 0
                else ceil(max(zaisekidays) / count(distinct juchdate))
            end as ruiindays
        ,ceil((datediff(hours,to_date(max(juchdate)::STRING, 'YYYYMMDD'), current_timestamp())::decimal)/24) as konyukeikadays
    from tt05kokyakonyu
    where meisainukikingaku > 0
    group by kokyano
),
tt1y as (
    select
        kokyano
        , count(distinct juchdate) as nenkaisu
        , sum(meisainukikingaku) as nenkingaku
        , case
            when count(distinct juchdate) <= 1 then 0
            else  ceil(max(zaisekidays) / count(distinct juchdate))
        end as nenindays
    from tt05kokyakonyu 
    where 
        to_date(juchdate::STRING,'YYYYMMDD') between
                add_months(current_timestamp(), 0-12) and dateadd(day, -1, current_timestamp())
                and meisainukikingaku > 0
    group by kokyano
),
tt01 as (
    select
        kokyano
        , min(juchdate) as firstjuchdate
        , max(juchdate) as lastjuchdate
        ,ceil(datediff(hour,to_date(max(juchdate)::STRING, 'YYYYMMDD'), current_timestamp())::decimal/24) as juchukeikadays 
    from tt01kokyastsh_mv
    group by kokyano
),

transformed as (
    select
        wrke.kokyano                               as kokyano
        , ifnull(tt01.firstjuchdate,99991231)      as firstjuchdate
        , ifnull(ttxx.firstkonyudate,99991231)     as firstkonyudate
        , ifnull(ttxx.zaisekidays,0)               as zaisekidays
        , ifnull(ttxx.zaisekimonth,0)              as zaisekimonth
        , ifnull(ttxx.firsttsuhandate,99991231)    as firsttsuhandate
        , ifnull(ttxx.firsttenpodate,99991231)     as firsttenpodate
        , ifnull(ttxx.ruikaisu,0)                  as ruikaisu
        , ifnull(ttxx.ruikingaku,0)                as ruikingaku
        , ifnull(ttxx.ruiindays,0)                 as ruiindays
        , ifnull(tt01.lastjuchdate,99991231)       as lastjuchdate
        , ifnull(tt01.juchukeikadays,0)            as juchukeikadays
        , ifnull(ttxx.lastkonyudate,99991231)      as lastkonyudate
        , ifnull(ttxx.konyukeikadays,0)            as konyukeikadays
        , ifnull(tt1y.nenkaisu,0)                  as nenkaisu
        , ifnull(tt1y.nenkingaku,0)                as nenkingaku
        , ifnull(tt1y.nenindays,0)                 as nenindays
        , 0                                        as nengelryo
        , 0                                        as tsukigelryo
    from
        tw05kokyarecalc as wrke
    left join ttxx on wrke.kokyano = ttxx.kokyano
    left join tt1y on wrke.kokyano = tt1y.kokyano
    left join tt01 on wrke.kokyano = tt01.kokyano
),
final as (
    select
        kokyano::varchar(15) as kokyano,
        firstjuchdate::number(38,0) as firstjuchdate,
        firstkonyudate::number(38,0) as firstkonyudate,
        zaisekidays::number(38,0) as zaisekidays,
        zaisekimonth::number(38,0) as zaisekimonth,
        firsttsuhandate::number(38,0) as firsttsuhandate,
        firsttenpodate::number(38,0) as firsttenpodate,
        ruikaisu::number(38,0) as ruikaisu,
        ruikingaku::number(38,0) as ruikingaku,
        ruiindays::number(38,0) as ruiindays,
        lastjuchdate::number(38,0) as lastjuchdate,
        juchukeikadays::number(38,0) as juchukeikadays,
        lastkonyudate::number(38,0) as lastkonyudate,
        konyukeikadays::number(38,0) as konyukeikadays,
        nenkaisu::number(38,0) as nenkaisu,
        nenkingaku::number(38,0) as nenkingaku,
        nenindays::number(38,0) as nenindays,
        nengelryo::number(38,0) as nengelryo,
        tsukigelryo::number(8,1) as tsukigelryo,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final

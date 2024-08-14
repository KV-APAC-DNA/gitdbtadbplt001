with tt01kokyastsh_mv as (
    select * from {{ ref('jpndcledw_integration__tt01kokyastsh_mv') }}
),
tw09kokyakonyu as (
    select * from {{ ref('jpndcledw_integration__tw09kokyakonyu') }}
),
tw05kokyarecalc as(
    select * from {{ source('jpdcledw_integration', 'tw05kokyarecalc') }} 
),
tt01kokyastsh_mv_filtered as (
    select
        h.kokyano
        ,h.juchdate
    from tt01kokyastsh_mv h
    where 
        kokyano <> '0000000011' 
            and kokyano <> '9999999944'
        and cancelflg = 0
        and juchkbn not like '9%'
        and kokyano in (select kokyano from tw05kokyarecalc) 
    group by
        h.kokyano
        ,h.juchdate
),
tw09kokyakonyu_filtered as (
    select
        s.kokyano
        ,s.juchdate
        ,max(case when s.juchitemkbnshohin > 0 then 1 else 0 end) as juchitemkbnshohin
        ,max(case when s.juchitemkbntrial  > 0 then 1 else 0 end) as juchitemkbntrial
        ,max(case when s.juchitemkbnsample > 0 then 1 else 0 end) as juchitemkbnsample
    from tw09kokyakonyu s
    group by
        s.kokyano
        ,s.juchdate
),
transformed as (
    select
        hh.kokyano
        ,hh.juchdate
        ,sum(nvl(ss.juchitemkbnshohin,0)) as ruijuchkaisushohin
        ,sum(nvl(ss.juchitemkbntrial ,0)) as ruijuchkaisutrial
        ,sum(nvl(ss.juchitemkbnsample,0)) as ruijuchkaisusample
        ,max(case when ss.juchitemkbnshohin > 0 then ss.juchdate else 0 end) as lastjuchdateshohin 
    from
        tt01kokyastsh_mv_filtered hh
    left join tw09kokyakonyu_filtered ss 
        on hh.kokyano = ss.kokyano
        and hh.juchdate > ss.juchdate
    group by
        hh.kokyano
        ,hh.juchdate
),
final as (
    select
        kokyano::varchar(60) as kokyano,
        juchdate::number(38,18) as juchdate,
        ruijuchkaisushohin::number(38,18) as ruijuchkaisushohin,
        ruijuchkaisutrial::number(38,18) as ruijuchkaisutrial,
        ruijuchkaisusample::number(38,18) as ruijuchkaisusample,
        lastjuchdateshohin::number(38,18) as lastjuchdateshohin,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final

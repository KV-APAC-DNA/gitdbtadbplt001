with tw06_kokyastatus as (
    select * from dev_dna_core.snapjpdcledw_integration.tw06_kokyastatus
),
tt01kokyastsh_mv as (
    select * from dev_dna_core.snapjpdcledw_integration.tt01kokyastsh_mv
),
tt05kokyakonyu as (
    select * from dev_dna_core.snapjpdcledw_integration.tt05kokyakonyu
),
tt05kokyakonyu_filtered as (
    select 
        saleno,
        juchdate,
        kokyano,
        keikadays,
        meisainukikingaku,
        to_date(juchdate::string, 'YYYYMMDD') as ju 
    from 
        tt05kokyakonyu 
    where meisainukikingaku > 0
),
tt01kokyastsh_mv_filtered as (
    select
        saleno_trm saleno
        ,juchdate
        ,kokyano
        ,ifnull( 
            datediff(day,to_date(
             (lag(juchdate) over (partition by kokyano order by kokyano, juchdate, saleno))::string
             ,'YYYYMMDD'),to_date(juchdate::string,'YYYYMMDD'))
             ,0)    as juchukeikadays
        ,row_number() over(partition by kokyano order by juchdate desc) rn

    from tt01kokyastsh_mv 
    where 
        kokyano in (select kokyano from tw06_kokyastatus)
        and juchkbn not like '9%'
),
transformed as (
    select 
  a.saleno
  
 ,a.juchdate
  ,a.kokyano
  
 ,nvl((sum(nvl(b.meisainukikingaku,0)) over (partition by a.kokyano order by a.kokyano,a.juchdate,a.saleno rows between unbounded preceding and 1 preceding)) -
  (max(nvl(b.meisainukikingaku,0)) over (partition by a.kokyano,a.juchdate order by a.kokyano,a.juchdate,a.saleno rows between unbounded preceding and 1 preceding)), 0)
	as ruikingaku
 ,sum(nvl(b.keikadays,0)) over (partition by a.kokyano order by a.kokyano,a.juchdate,a.saleno rows between unbounded preceding and 1 preceding) as keikadayskei
 ,nvl(max(a.juchdate)
    over(partition by a.kokyano order by a.juchdate
    rows between unbounded preceding and 1 preceding) ,0)
  as lastjuchdate
 ,nvl(max(b.juchdate)
    over(partition by a.kokyano order by a.juchdate
    rows between unbounded preceding and 1 preceding) ,0)
  as lastkonyudate
 ,a.juchukeikadays
 ,nvl(sum(b.meisainukikingaku)
    over(partition by rolling2.kokyano) , 0)
  as nenkingaku
 ,nvl(sum(b.keikadays)
    over(partition by rolling.kokyano) , 0)
  as nenkeikadayskei
 ,nvl(
	sum(case
	when b.saleno is not null and b.keikadays <> 0 then 1
	else 0 end)
    over(partition by rolling2.kokyano) , 0)
  as nenruikaisu
from
    tt01kokyastsh_mv_filtered a
left join tt01kokyastsh_mv_filtered rolling
    on a.kokyano =rolling.kokyano and rolling.rn between a.rn and a.rn+10000
left join tt01kokyastsh_mv_filtered rolling2
    on a.kokyano =rolling2.kokyano and rolling2.rn between a.rn+1 and a.rn+10000
left join   
    tt05kokyakonyu_filtered b
    on a.saleno = b.saleno
),
final as (
    select
        saleno::varchar(61) as saleno,
        juchdate::number(38,18) as juchdate,
        kokyano::varchar(60) as kokyano,
        ruikingaku::number(38,18) as ruikingaku,
        keikadayskei::number(38,18) as keikadayskei,
        lastjuchdate::number(38,18) as lastjuchdate,
        lastkonyudate::number(38,18) as lastkonyudate,
        juchukeikadays::number(38,18) as juchukeikadays,
        nenkingaku::number(38,18) as nenkingaku,
        nenkeikadayskei::number(38,18) as nenkeikadayskei,
        nenruikaisu::number(38,18) as nenruikaisu,      
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final


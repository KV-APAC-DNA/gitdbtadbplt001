with tw06_kokyastatus as (
    select * from dev_dna_core.snapjpdcledw_integration.tw06_kokyastatus
),
tt01kokyastsh_mv as (
    select * from dev_dna_core.snapjpdcledw_integration.tt01kokyastsh_mv
),
tw08_kokyastatusw02 as (
    select * from dev_dna_core.snapjpdcledw_integration.tw08_kokyastatusw02
),
transformed as (
    
    select
        trim(h.saleno) saleno
        ,h.kokyano
        ,h.juchdate
        ,sum(case when w.kokyano is not null and w.juchdate < h.juchdate then w.konyukaisu else 0 end) as konyukaisu01
        ,sum(case when w.kokyano is not null and w.juchdate < h.juchdate then 1 else 0 end) as konyudatesu01
        ,sum(case when w.kokyano is not null and w.juchdate <= h.juchdate then w.konyukaisu else 0 end) as konyukaisu02
        ,sum(case when w.kokyano is not null and w.konyunisu <> 1 and w.juchdate <= h.juchdate then 1 else 0 end) as konyudatesu02
        ,sum(case when w.kokyano is not null and w.juchdate >= h.juchdate-10000 and w.juchdate < h.juchdate then w.konyukaisu else 0 end) as konyukaisu04
        ,sum(case when w.kokyano is not null and w.juchdate >= h.juchdate-10000 and w.juchdate < h.juchdate then 1 else 0 end) as konyudatesu04
        ,sum(case when w.kokyano is not null and w.juchdate >= h.juchdate-10000 and w.juchdate <= h.juchdate then w.konyukaisu else 0 end) as konyukaisu03
        ,sum(case when w.kokyano is not null and w.konyunisu <> 1 and w.juchdate >= h.juchdate-10000 and w.juchdate <= h.juchdate then 1 else 0 end) as konyudatesu03
    from tw06_kokyastatus t
    inner join tt01kokyastsh_mv      h on t.kokyano = h.kokyano
    left outer join tw08_kokyastatusw02  w on h.kokyano = w.kokyano
    where
        h.juchkbn not like '9%'
    group by
        h.saleno
        ,h.kokyano
        ,h.juchdate
),
final as (
    select
        saleno::varchar(61) as saleno,
        kokyano::varchar(60) as kokyano,
        juchdate::number(38,18) as juchdate,
        konyukaisu01::number(38,18) as konyukaisu01,
        konyudatesu01::number(38,18) as konyudatesu01,
        konyukaisu02::number(38,18) as konyukaisu02,
        konyudatesu02::number(38,18) as konyudatesu02,
        konyukaisu04::number(38,18) as konyukaisu04,
        konyudatesu04::number(38,18) as konyudatesu04,
        konyukaisu03::number(38,18) as konyukaisu03,
        konyudatesu03::number(38,18) as konyudatesu03,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final

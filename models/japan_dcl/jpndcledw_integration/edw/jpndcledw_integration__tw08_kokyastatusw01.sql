with tw06_kokyastatus as (
    select * from dev_dna_core.snapjpdcledw_integration.tw06_kokyastatus
),
tt05kokyakonyu as (
    select * from dev_dna_core.snapjpdcledw_integration.tt05kokyakonyu
),

transformed as(
    select
        t.kokyano
        ,s.juchdate
        ,count(s.saleno) as konyukaisu
    from tw06_kokyastatus t
    inner join tt05kokyakonyu s on s.kokyano = t.kokyano
    where 
        s.meisainukikingaku > 0

    group by
        t.kokyano
        ,s.juchdate
),
final as (
    select
        kokyano::varchar(15) as kokyano,
        juchdate::number(18,0) as juchdate,
        konyukaisu::number(38,18) as konyukaisu,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final

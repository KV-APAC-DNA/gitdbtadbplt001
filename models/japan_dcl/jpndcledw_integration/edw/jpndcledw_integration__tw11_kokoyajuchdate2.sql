
with tw11_kokoyajuchdate1 as (
    select * from dev_dna_core.jpdcledw_integration.tw11_kokoyajuchdate1
),
transformed as(
    select
        t.kokyano
        ,t.juchdate
        ,t.ruijuchkaisushohin
        ,t.ruijuchkaisutrial
        ,t.ruijuchkaisusample
        ,(case when t.lastjuchdateshohin = 0 then 0
        else
            cast(
                cast(
                    datediff(day, to_date(cast(t.lastjuchdateshohin as varchar), 'YYYYMMDD'),to_date(cast(t.juchdate as varchar), 'YYYYMMDD') ) as varchar
                ) as numeric
            )
        end) as juchukeikadaysshohin
    from tw11_kokoyajuchdate1 t
),
final as (
    select
        kokyano::varchar(60) as kokyano,
        juchdate::number(38,18) as juchdate,
        ruijuchkaisushohin::number(38,18) as ruijuchkaisushohin,
        ruijuchkaisutrial::number(38,18) as ruijuchkaisutrial,
        ruijuchkaisusample::number(38,18) as ruijuchkaisusample,
        juchukeikadaysshohin::number(38,18) as juchukeikadaysshohin,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final

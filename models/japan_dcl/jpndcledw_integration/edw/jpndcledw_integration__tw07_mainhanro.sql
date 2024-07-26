with tw07_01kokyahanro as (
    select * from dev_dna_core.jpdcledw_integration.tw07_01kokyahanro
),
tw07_02kokyatenpo as (
    select * from dev_dna_core.jpdcledw_integration.tw07_02kokyatenpo
),

transformed as (
    select distinct
        w1.kokyano            as kokyano
        , case
            when w1.syobuncode = '直営・百貨店' then '店舗'
            when w1.syobuncode = '不明' then '不明'
            else '通販'
            end as maindaibuncode
        , w1.daibuncode    as mainchubuncode
        , w1.chubuncode    as mainsyobuncode
        , w1.syobuncode    as mainsaibuncode
        , w2.tenpocode     as maintenpocode
    from 
        tw07_01kokyahanro w1
    left join tw07_02kokyatenpo w2 on w1.kokyano = w2.kokyano    
    
),
final as (
    select
        kokyano::varchar(15) as kokyano,
        maindaibuncode::varchar(30) as maindaibuncode,
        mainchubuncode::varchar(30) as mainchubuncode,
        mainsyobuncode::varchar(30) as mainsyobuncode,
        mainsaibuncode::varchar(30) as mainsaibuncode,
        maintenpocode::varchar(7) as maintenpocode,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final

with tw06_01konyujisseki as (
    select * from dev_dna_core.snapjpdcledw_integration.tw06_01konyujisseki
),
tw06_02konyusihyokbn as (
    select * from dev_dna_core.snapjpdcledw_integration.tw06_02konyusihyokbn
),
tw06_03kokyasegment as (
    select * from dev_dna_core.snapjpdcledw_integration.tw06_03kokyasegment
),
transformed as (
    select
        f1.kokyano
        , f1.firstjuchdate
        , f1.firstkonyudate
        , f1.zaisekidays
        , f1.zaisekimonth
        , f1.firsttsuhandate
        , f1.firsttenpodate
        , f1.ruikaisu
        , f1.ruikingaku
        , f1.ruiindays
        , f1.lastjuchdate
        , f1.juchukeikadays
        , f1.lastkonyudate
        , f1.konyukeikadays
        , f1.nenkaisu
        , f1.nenkingaku
        , f1.nenindays
        , f1.nengelryo
        , f1.tsukigelryo
        , f2.juchurkbncode
        , f2.konyurkbncode
        , f2.ruifkbncode
        , f2.nenfkbncode
        , f2.ruiikbncode
        , f2.nenikbncode
        , f2.ruimkbncode
        , f2.nenmkbncode1
        , f2.nenmkbncode2
        , f2.nenmkbncode3
        , f2.nenmkbncode4
        , f2.nenmkbncode5
        , f2.tsukigkbncode
        , f3.segkbncode
    from tw06_01konyujisseki f1
    inner join tw06_02konyusihyokbn f2 on f1.kokyano = f2.kokyano
    inner join tw06_03kokyasegment f3 on f1.kokyano = f3.kokyano 
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
        tsukigelryo::number(4,1) as tsukigelryo,
        juchurkbncode::varchar(4) as juchurkbncode,
        konyurkbncode::varchar(4) as konyurkbncode,
        ruifkbncode::varchar(4) as ruifkbncode,
        nenfkbncode::varchar(4) as nenfkbncode,
        ruiikbncode::varchar(4) as ruiikbncode,
        nenikbncode::varchar(4) as nenikbncode,
        ruimkbncode::varchar(4) as ruimkbncode,
        nenmkbncode1::varchar(4) as nenmkbncode1,
        nenmkbncode2::varchar(4) as nenmkbncode2,
        nenmkbncode3::varchar(4) as nenmkbncode3,
        nenmkbncode4::varchar(4) as nenmkbncode4,
        nenmkbncode5::varchar(4) as nenmkbncode5,
        tsukigkbncode::varchar(4) as tsukigkbncode,
        segkbncode::varchar(3) as segkbncode,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final

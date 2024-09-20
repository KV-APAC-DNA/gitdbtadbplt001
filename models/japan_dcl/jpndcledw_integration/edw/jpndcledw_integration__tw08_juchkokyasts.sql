with tw08_01konyujisseki as (
    select * from  {{ ref('jpndcledw_integration__tw08_01konyujisseki') }}
),
tw08_02konyusihyokbn as (
    select * from {{ ref('jpndcledw_integration__tw08_02konyusihyokbn') }}
),
tw08_03kokyasegment as (
    select * from {{ ref('jpndcledw_integration__tw08_03kokyasegment') }}
),
tw11_kokyakbncode as (
    select * from {{ ref('jpndcledw_integration__tw11_kokyakbncode') }}
),
	
	

transformed as(

    select
        f1.saleno
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
        , f4.kokyakbncode 
    from tw08_01konyujisseki f1
    inner join tw08_02konyusihyokbn f2 on f1.saleno = f2.saleno
    inner join tw08_03kokyasegment f3 on f1.saleno = f3.saleno
    inner join tw11_kokyakbncode f4 on f1.saleno = f4.saleno

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
        kokyakbncode::varchar(3) as kokyakbncode,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final

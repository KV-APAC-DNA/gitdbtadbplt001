{{
    config
    (
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key= ["kokyano"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} 
                where kokyano  in (
                    select kokyakuno from {{ ref('jpndcledw_integration__cil02nayol') }}
                );

        {% endif %}"
    )
}}

with tw06_kokyastatus as (
    select * from {{ ref('jpndcledw_integration__tw06_kokyastatus') }}
),
cil02nayol as
(
    select * from {{ ref('jpndcledw_integration__cil02nayol') }}
),
tw07_mainhanro as (
    select * from {{ ref('jpndcledw_integration__tw07_mainhanro') }}
),
transformed as (
    select
        w06.kokyano,
        w06.firstjuchdate,
        w06.firstkonyudate,
        w06.zaisekidays,
        w06.zaisekimonth,
        w06.firsttsuhandate,
        w06.firsttenpodate,
        w06.ruikaisu,
        w06.ruikingaku,
        w06.ruiindays,
        w06.lastjuchdate,
        w06.juchukeikadays,
        w06.lastkonyudate,
        w06.konyukeikadays,
        w06.nenkaisu,
        w06.nenkingaku,
        w06.nenindays,
        w06.nengelryo,
        w06.tsukigelryo,
        w06.juchurkbncode,
        w06.konyurkbncode,
        w06.ruifkbncode,
        w06.nenfkbncode,
        w06.ruiikbncode,
        w06.nenikbncode,
        w06.ruimkbncode,
        w06.nenmkbncode1,
        w06.nenmkbncode2,
        w06.nenmkbncode3,
        w06.nenmkbncode4,
        w06.nenmkbncode5,
        w06.tsukigkbncode,
        w06.segkbncode,
        w07.maindaibuncode,
        w07.mainchubuncode,
        w07.mainsyobuncode,
        w07.mainsaibuncode,
        w07.maintenpocode,
        {% if is_incremental() %}
        case 
            when m22.kokyano is not null then m22.insertdate
            else cast(to_char(current_timestamp(),'YYYYMMDD')as integer) 
        end
        {% else %}
            cast(to_char(current_timestamp(),'YYYYMMDD')as integer) 
        {% endif %} as insertdate,
        {% if is_incremental() %}
        case 
            when m22.kokyano is not null then m22.inserttime
            else cast(to_char(current_timestamp(),'HH24MISS')as integer)
        end 
        {% else %}
            cast(to_char(current_timestamp(),'HH24MISS')as integer)
        {% endif %} as inserttime,
        '090554' as insertid,
        cast(to_char(current_timestamp(),'YYYYMMDD')as integer) as updatedate,
        cast(to_char(current_timestamp(),'HH24MISS')as integer) as updatetime,
        '090554' as updateid,
        {% if is_incremental() %}
        case 
            when m22.kokyano is not null then 'ETL_Batch'
            else null
        end 
        {% else %}
            null
        {% endif %} as updated_by,
        {% if is_incremental() %}
            m22.bk_kokyano,
            m22.bk_maindaibuncode,
            m22.bk_mainchubuncode,
            m22.bk_mainsyobuncode,
            m22.bk_mainsaibuncode,
            m22.bk_firstjuchdate,
            m22.bk_firstkonyudate,
            m22.bk_firsttsuhandate,
            m22.bk_firsttenpodate,
            m22.bk_zaisekidays,
            m22.bk_zaisekimonth,
            m22.bk_ruikaisu,
            m22.bk_ruikingaku,
            m22.bk_ruiindays,
            m22.bk_nenkaisu,
            m22.bk_nenkingaku,
            m22.bk_nenindays,
            m22.bk_nengelryo
        {% else %}
            null as bk_kokyano,
            null as bk_maindaibuncode,
            null as bk_mainchubuncode,
            null as bk_mainsyobuncode,
            null as bk_mainsaibuncode,
            null as bk_firstjuchdate,
            null as bk_firstkonyudate,
            null as bk_firsttsuhandate,
            null as bk_firsttenpodate,
            null as bk_zaisekidays,
            null as bk_zaisekimonth,
            null as bk_ruikaisu,
            null as bk_ruikingaku,
            null as bk_ruiindays,
            null as bk_nenkaisu,
            null as bk_nenkingaku,
            null as bk_nenindays,
            null as bk_nengelryo
        {% endif %}
    from
        tw06_kokyastatus w06
    left join 
        tw07_mainhanro w07
        on w07.kokyano = w06.kokyano
    {% if is_incremental() %}
    left join {{this}} m22
	    on m22.kokyano = w06.kokyano
    {% endif %}

),
final as (
    select
        kokyano::varchar(60) as kokyano,
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
        tsukigelryo::number(6,1) as tsukigelryo,
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
        maindaibuncode::varchar(60) as maindaibuncode,
        mainchubuncode::varchar(60) as mainchubuncode,
        mainsyobuncode::varchar(60) as mainsyobuncode,
        mainsaibuncode::varchar(60) as mainsaibuncode,
        maintenpocode::varchar(7) as maintenpocode,
        insertdate::number(38,0) as insertdate,
        inserttime::number(38,0) as inserttime,
        insertid::varchar(9) as insertid,
        updatedate::number(38,0) as updatedate,
        updatetime::number(38,0) as updatetime,
        updateid::varchar(9) as updateid,
        bk_kokyano::varchar(12) as bk_kokyano,
        bk_maindaibuncode::varchar(12) as bk_maindaibuncode,
        bk_mainchubuncode::varchar(3) as bk_mainchubuncode,
        bk_mainsyobuncode::varchar(3) as bk_mainsyobuncode,
        bk_mainsaibuncode::varchar(3) as bk_mainsaibuncode,
        bk_firstjuchdate::number(38,0) as bk_firstjuchdate,
        bk_firstkonyudate::number(38,0) as bk_firstkonyudate,
        bk_firsttsuhandate::number(38,0) as bk_firsttsuhandate,
        bk_firsttenpodate::number(38,0) as bk_firsttenpodate,
        bk_zaisekidays::number(38,0) as bk_zaisekidays,
        bk_zaisekimonth::number(38,0) as bk_zaisekimonth,
        bk_ruikaisu::number(38,0) as bk_ruikaisu,
        bk_ruikingaku::number(38,0) as bk_ruikingaku,
        bk_ruiindays::number(38,0) as bk_ruiindays,
        bk_nenkaisu::number(38,0) as bk_nenkaisu,
        bk_nenkingaku::number(38,0) as bk_nenkingaku,
        bk_nenindays::number(38,0) as bk_nenindays,
        bk_nengelryo::number(38,0) as bk_nengelryo,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        updated_by::varchar(100) as updated_by
    from transformed
)
select * from final

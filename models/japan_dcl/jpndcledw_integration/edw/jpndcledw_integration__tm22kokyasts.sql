{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} 
                            where W01.kokyano  in (
                                select kokyakuno from {{ ref('jpndcledw_integration__cil02nayol') }} W01
                            );
                    
                    UPDATE {{this}}
                    SET FIRSTJUCHDATE = W0607.FIRSTJUCHDATE, FIRSTKONYUDATE = W0607.FIRSTKONYUDATE, ZAISEKIDAYS = W0607.ZAISEKIDAYS, ZAISEKIMONTH = W0607.ZAISEKIMONTH, FIRSTTSUHANDATE = W0607.FIRSTTSUHANDATE, FIRSTTENPODATE = W0607.FIRSTTENPODATE, RUIKAISU = W0607.RUIKAISU, RUIKINGAKU = W0607.RUIKINGAKU, RUIINDAYS = W0607.RUIINDAYS, LASTJUCHDATE = W0607.LASTJUCHDATE, JUCHUKEIKADAYS = W0607.JUCHUKEIKADAYS, LASTKONYUDATE = W0607.LASTKONYUDATE, KONYUKEIKADAYS = W0607.KONYUKEIKADAYS, NENKAISU = W0607.NENKAISU, NENKINGAKU = W0607.NENKINGAKU, NENINDAYS = W0607.NENINDAYS, NENGELRYO = W0607.NENGELRYO, TSUKIGELRYO = W0607.TSUKIGELRYO, JUCHURKBNCODE = W0607.JUCHURKBNCODE, KONYURKBNCODE = W0607.KONYURKBNCODE, RUIFKBNCODE = W0607.RUIFKBNCODE, NENFKBNCODE = W0607.NENFKBNCODE, RUIIKBNCODE = W0607.RUIIKBNCODE, NENIKBNCODE = W0607.NENIKBNCODE, RUIMKBNCODE = W0607.RUIMKBNCODE, NENMKBNCODE1 = W0607.NENMKBNCODE1, NENMKBNCODE2 = W0607.NENMKBNCODE2, NENMKBNCODE3 = W0607.NENMKBNCODE3, NENMKBNCODE4 = W0607.NENMKBNCODE4, NENMKBNCODE5 = W0607.NENMKBNCODE5, TSUKIGKBNCODE = W0607.TSUKIGKBNCODE, SEGKBNCODE = W0607.SEGKBNCODE, MAINDAIBUNCODE = W0607.MAINDAIBUNCODE, MAINCHUBUNCODE = W0607.MAINCHUBUNCODE, MAINSYOBUNCODE = W0607.MAINSYOBUNCODE, MAINSAIBUNCODE = W0607.MAINSAIBUNCODE, MAINTENPOCODE = W0607.MAINTENPOCODE, UPDATEDATE = CAST(TO_CHAR(current_timestamp(), 'YYYYMMDD') AS INTEGER), UPDATETIME = CAST(TO_CHAR(current_timestamp(), 'HH24MISS') AS INTEGER), UPDATEID = '090554', updated_date = GETDATE(), updated_by = 'ETL_Batch'
                    FROM (
                        SELECT W06.KOKYANO, W06.FIRSTJUCHDATE, W06.FIRSTKONYUDATE, W06.ZAISEKIDAYS, W06.ZAISEKIMONTH, W06.FIRSTTSUHANDATE, W06.FIRSTTENPODATE, W06.RUIKAISU, W06.RUIKINGAKU, W06.RUIINDAYS, W06.LASTJUCHDATE, W06.JUCHUKEIKADAYS, W06.LASTKONYUDATE, W06.KONYUKEIKADAYS, W06.NENKAISU, W06.NENKINGAKU, W06.NENINDAYS, W06.NENGELRYO, W06.TSUKIGELRYO, W06.JUCHURKBNCODE, W06.KONYURKBNCODE, W06.RUIFKBNCODE, W06.NENFKBNCODE, W06.RUIIKBNCODE, W06.NENIKBNCODE, W06.RUIMKBNCODE, W06.NENMKBNCODE1, W06.NENMKBNCODE2, W06.NENMKBNCODE3, W06.NENMKBNCODE4, W06.NENMKBNCODE5, W06.TSUKIGKBNCODE, W06.SEGKBNCODE, W07.MAINDAIBUNCODE, W07.MAINCHUBUNCODE, W07.MAINSYOBUNCODE, W07.MAINSAIBUNCODE, W07.MAINTENPOCODE
                        FROM {{ ref('jpndcledw_integration__tw06_kokyastatus') }} W06
                        LEFT JOIN {{ ref('jpndcledw_integration__tw07_mainhanro') }} W07 ON W07.KOKYANO = W06.KOKYANO
                        ) W0607
                    WHERE {{this}}.KOKYANO = W0607.KOKYANO;
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

tm22kokyasts as 
(
    select * from {{this}}
),

W0607 as
(
    SELECT W06.KOKYANO,
        W06.FIRSTJUCHDATE,
        W06.FIRSTKONYUDATE,
        W06.ZAISEKIDAYS,
        W06.ZAISEKIMONTH,
        W06.FIRSTTSUHANDATE,
        W06.FIRSTTENPODATE,
        W06.RUIKAISU,
        W06.RUIKINGAKU,
        W06.RUIINDAYS,
        W06.LASTJUCHDATE,
        W06.JUCHUKEIKADAYS,
        W06.LASTKONYUDATE,
        W06.KONYUKEIKADAYS,
        W06.NENKAISU,
        W06.NENKINGAKU,
        W06.NENINDAYS,
        W06.NENGELRYO,
        W06.TSUKIGELRYO,
        W06.JUCHURKBNCODE,
        W06.KONYURKBNCODE,
        W06.RUIFKBNCODE,
        W06.NENFKBNCODE,
        W06.RUIIKBNCODE,
        W06.NENIKBNCODE,
        W06.RUIMKBNCODE,
        W06.NENMKBNCODE1,
        W06.NENMKBNCODE2,
        W06.NENMKBNCODE3,
        W06.NENMKBNCODE4,
        W06.NENMKBNCODE5,
        W06.TSUKIGKBNCODE,
        W06.SEGKBNCODE,
        W07.MAINDAIBUNCODE,
        W07.MAINCHUBUNCODE,
        W07.MAINSYOBUNCODE,
        W07.MAINSAIBUNCODE,
        W07.MAINTENPOCODE
    FROM TW06_KOKYASTATUS W06
    LEFT JOIN TW07_MAINHANRO W07 ON W07.KOKYANO = W06.KOKYANO
),

transformed as (
    SELECT W0607.KOKYANO,
		W0607.FIRSTJUCHDATE,
		W0607.FIRSTKONYUDATE,
		W0607.ZAISEKIDAYS,
		W0607.ZAISEKIMONTH,
		W0607.FIRSTTSUHANDATE,
		W0607.FIRSTTENPODATE,
		W0607.RUIKAISU,
		W0607.RUIKINGAKU,
		W0607.RUIINDAYS,
		W0607.LASTJUCHDATE,
		W0607.JUCHUKEIKADAYS,
		W0607.LASTKONYUDATE,
		W0607.KONYUKEIKADAYS,
		W0607.NENKAISU,
		W0607.NENKINGAKU,
		W0607.NENINDAYS,
		W0607.NENGELRYO,
		W0607.TSUKIGELRYO,
		W0607.JUCHURKBNCODE,
		W0607.KONYURKBNCODE,
		W0607.RUIFKBNCODE,
		W0607.NENFKBNCODE,
		W0607.RUIIKBNCODE,
		W0607.NENIKBNCODE,
		W0607.RUIMKBNCODE,
		W0607.NENMKBNCODE1,
		W0607.NENMKBNCODE2,
		W0607.NENMKBNCODE3,
		W0607.NENMKBNCODE4,
		W0607.NENMKBNCODE5,
		W0607.TSUKIGKBNCODE,
		W0607.SEGKBNCODE,
		W0607.MAINDAIBUNCODE,
		W0607.MAINCHUBUNCODE,
		W0607.MAINSYOBUNCODE,
		W0607.MAINSAIBUNCODE,
		W0607.MAINTENPOCODE,
		CAST(TO_CHAR(current_timestamp(), 'YYYYMMDD') AS INTEGER) as insertdate,
		CAST(TO_CHAR(current_timestamp(), 'HH24MISS') AS INTEGER) as inserttime,
		'090554' as insertid,
		CAST(TO_CHAR(current_timestamp(), 'YYYYMMDD') AS INTEGER) as updatedate,
		CAST(TO_CHAR(current_timestamp(), 'HH24MISS') AS INTEGER) as updatetime,
		'090554' as updateid
	FROM  W0607
	LEFT JOIN TM22KOKYASTS M22 ON M22.KOKYANO = W0607.KOKYANO
	WHERE M22.KOKYANO IS NULL

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
        null::varchar(12) as bk_kokyano,
        null::varchar(12) as bk_maindaibuncode,
        null::varchar(3) as bk_mainchubuncode,
        null::varchar(3) as bk_mainsyobuncode,
        null::varchar(3) as bk_mainsaibuncode,
        null::number(38,0) as bk_firstjuchdate,
        null::number(38,0) as bk_firstkonyudate,
        null::number(38,0) as bk_firsttsuhandate,
        null::number(38,0) as bk_firsttenpodate,
        null::number(38,0) as bk_zaisekidays,
        null::number(38,0) as bk_zaisekimonth,
        null::number(38,0) as bk_ruikaisu,
        null::number(38,0) as bk_ruikingaku,
        null::number(38,0) as bk_ruiindays,
        null::number(38,0) as bk_nenkaisu,
        null::number(38,0) as bk_nenkingaku,
        null::number(38,0) as bk_nenindays,
        null::number(38,0) as bk_nengelryo,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final

{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "
                    {% if is_incremental() %}
                    UPDATE
		            {{this}}
                    SET
                        RUIKAISU  = W08.RUIKAISU, RUIKINGAKU  = W08.RUIKINGAKU, RUIINDAYS  = W08.RUIINDAYS, LASTJUCHDATE  = W08.LASTJUCHDATE, JUCHUKEIKADAYS  = W08.JUCHUKEIKADAYS, LASTKONYUDATE  = W08.LASTKONYUDATE, KONYUKEIKADAYS  = W08.KONYUKEIKADAYS, NENKAISU  = W08.NENKAISU, NENKINGAKU  = W08.NENKINGAKU, NENINDAYS  = W08.NENINDAYS, NENGELRYO  = W08.NENGELRYO, TSUKIGELRYO  = W08.TSUKIGELRYO, JUCHURKBNCODE  = W08.JUCHURKBNCODE, KONYURKBNCODE  = W08.KONYURKBNCODE, RUIFKBNCODE  = W08.RUIFKBNCODE, NENFKBNCODE  = W08.NENFKBNCODE, RUIIKBNCODE  = W08.RUIIKBNCODE, NENIKBNCODE  = W08.NENIKBNCODE, RUIMKBNCODE  = W08.RUIMKBNCODE, NENMKBNCODE1  = W08.NENMKBNCODE1, NENMKBNCODE2  = W08.NENMKBNCODE2, NENMKBNCODE3  = W08.NENMKBNCODE3, NENMKBNCODE4  = W08.NENMKBNCODE4, NENMKBNCODE5  = W08.NENMKBNCODE5, TSUKIGKBNCODE  = W08.TSUKIGKBNCODE, SEGKBNCODE  = W08.SEGKBNCODE, UPDATEDATE  = CAST(TO_CHAR(current_timestamp(),'YYYYMMDD')as INTEGER), UPDATETIME  = CAST(TO_CHAR(current_timestamp(),'HH24MISS')as INTEGER), UPDATEID  = '090554', KOKYAKBNCODE  = W08.KOKYAKBNCODE, updated_date = GETDATE(), updated_by = 'ETL_Batch'
                    FROM
                        {{ ref('jpndcledw_integration__tw08_juchkokyasts') }} W08
                    WHERE
                        {{this}}.SALENO = W08.SALENO
                    {% endif %}
                    "
    )
}}

with tw08_juchkokyasts as (
    select * from {{ ref('jpndcledw_integration__tw08_juchkokyasts') }}
),

tt06juchkokyasts as
(
    select * from {{this}}
),
transformed as (
    SELECT
		       TRIM(W08.SALENO) as saleno,
		       W08.RUIKAISU,
		       W08.RUIKINGAKU,
		       W08.RUIINDAYS,
		       W08.LASTJUCHDATE,
		       W08.JUCHUKEIKADAYS,
		       W08.LASTKONYUDATE,
		       W08.KONYUKEIKADAYS,
		       W08.NENKAISU,
		       W08.NENKINGAKU,
		       W08.NENINDAYS,
		       W08.NENGELRYO,
		       W08.TSUKIGELRYO,
		       W08.JUCHURKBNCODE,
		       W08.KONYURKBNCODE,
		       W08.RUIFKBNCODE,
		       W08.NENFKBNCODE,
		       W08.RUIIKBNCODE,
		       W08.NENIKBNCODE,
		       W08.RUIMKBNCODE,
		       W08.NENMKBNCODE1,
		       W08.NENMKBNCODE2,
		       W08.NENMKBNCODE3,
		       W08.NENMKBNCODE4,
		       W08.NENMKBNCODE5,
		       W08.TSUKIGKBNCODE,
		       W08.SEGKBNCODE,
		       CAST(TO_CHAR(current_timestamp(),'YYYYMMDD')as INTEGER) as insertdate,
		       CAST(TO_CHAR(current_timestamp(),'HH24MISS')as INTEGER) as inserttime,
		       '090554' as insertid,
		       CAST(TO_CHAR(current_timestamp(),'YYYYMMDD')as INTEGER) as updatedate,
		       CAST(TO_CHAR(current_timestamp(),'HH24MISS')as INTEGER) as updatetime,
		       '090554' as updateid,
		       W08.KOKYAKBNCODE
		FROM
		    TW08_JUCHKOKYASTS W08
		LEFT JOIN
		       TT06JUCHKOKYASTS T06
		  ON
		       T06.SALENO = W08.SALENO
		WHERE 
		    T06.SALENO IS NULL 

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
        insertdate::number(18,0) as insertdate,
        inserttime::number(18,0) as inserttime,
        insertid::varchar(9) as insertid,
        updatedate::number(18,0) as updatedate,
        updatetime::number(18,0) as updatetime,
        updateid::varchar(9) as updateid,
        kokyakbncode::varchar(3) as kokyakbncode,
        null::varchar(18) as bk_saleno,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final

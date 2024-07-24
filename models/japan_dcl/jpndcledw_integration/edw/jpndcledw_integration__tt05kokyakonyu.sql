{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = ["{% if is_incremental() %}
        delete from {{this}} 
                where kokyano  in (
                    SELECT DISTINCT KOKYANO FROM dev_dna_core.jpdcledw_integration.tw09kokyakonyu
                );

        {% endif %}"
		,		
		"{% if is_incremental() %}
        delete from {{this}} 
                where SALENO  in (
                    SELECT DISTINCT TRIM(SALENO) FROM dev_dna_core.jpdcledw_integration.tw09kokyakonyu
                );

        {% endif %}"]
    )
}}
WITH tw09kokyakonyu
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.tw09kokyakonyu
  ),
transformed
AS (
        SELECT
            TRIM(W09.SALENO) AS SALENO
            ,W09.JUCHKBN
            ,W09.TORIKEIKBN
            ,W09.JUCHDATE
            ,W09.KOKYANO
            ,W09.HANROCODE
            ,W09.SYOHANROBUNNAME
            ,W09.CHUHANROBUNNAME
            ,W09.DAIHANROBUNNAME
            ,W09.SOGOKEI
            ,W09.TENPOCODE
            ,W09.MEISAINUKIKINGAKU
            ,W09.WARIMAENUKIKINGAKU
            ,W09.GMEISAINUKIKINGAKU
            ,W09.KONYUKAISU
            ,W09.ZAISEKIDAYS
            ,W09.ZAISEKIMONTH
            ,W09.KEIKADAYS
            ,TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD'))  AS INSERTDATE
            ,TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP(), 'HH24MISS')) AS INSERTTIME
            ,'090554' AS INSERTID
            ,TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD')) AS UPDATEDATE
            ,TO_NUMBER(TO_CHAR(CURRENT_TIMESTAMP(), 'HH24MISS')) AS UPDATETIME
            ,'090554' AS UPDATEID
            ,NULL BK_JUCHKBN
            ,NULL BK_KOKYANO
            ,NULL BK_HANROCODE
            ,NULL INSERTED_BY
            ,NULL UPDATED_BY
        FROM  tw09kokyakonyu W09
),
final
AS (
        SELECT 
            saleno::varchar(18) as saleno,
            juchkbn::varchar(3) as juchkbn,
            torikeikbn::varchar(3) as torikeikbn,
            juchdate::number(18,0) as juchdate,
            kokyano::varchar(15) as kokyano,
            hanrocode::varchar(60) as hanrocode,
            syohanrobunname::varchar(60) as syohanrobunname,
            chuhanrobunname::varchar(60) as chuhanrobunname,
            daihanrobunname::varchar(60) as daihanrobunname,
            sogokei::number(38,0) as sogokei,
            tenpocode::varchar(7) as tenpocode,
            meisainukikingaku::number(38,0) as meisainukikingaku,
            warimaenukikingaku::number(38,0) as warimaenukikingaku,
            gmeisainukikingaku::number(38,0) as gmeisainukikingaku,
            konyukaisu::number(18,0) as konyukaisu,
            zaisekidays::number(18,0) as zaisekidays,
            zaisekimonth::number(38,0) as zaisekimonth,
            keikadays::number(18,0) as keikadays,
            insertdate::number(18,0) as insertdate,
            inserttime::number(18,0) as inserttime,
            insertid::varchar(9) as insertid,
            updatedate::number(18,0) as updatedate,
            updatetime::number(18,0) as updatetime,
            updateid::varchar(9) as updateid,
            bk_juchkbn::varchar(3) as bk_juchkbn,
            bk_kokyano::varchar(12) as bk_kokyano,
            bk_hanrocode::varchar(30) as bk_hanrocode,
            current_timestamp()::timestamp_ntz(9) AS inserted_date,
            inserted_by::varchar(100) as inserted_by,
            current_timestamp()::timestamp_ntz(9) AS updated_date,
            updated_by::varchar(100) as updated_by
        FROM transformed
)
SELECT *
FROM final
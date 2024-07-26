with TW11_KOKOYAJUCHDATE2 as (
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.TW11_KOKOYAJUCHDATE2
),
tt01kokyastsh_mv as (
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.tt01kokyastsh_mv
),
TW05KOKYARECALC as (
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.TW05KOKYARECALC
),
TW09KOKYAKONYU as (
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.TW09KOKYAKONYU
),
transformed as (
    SELECT
        H.SALENO
        ,CASE WHEN S.RUIJUCHKAISUSHOHIN > 0
        THEN
            CASE WHEN S.JUCHUKEIKADAYSSHOHIN <= 365
            THEN
                CASE WHEN S.RUIJUCHKAISUSHOHIN > 1
                THEN '07'
                ELSE
                    CASE WHEN T.JUCHITEMKBNSHOHIN = 1
                    THEN '07'
                    ELSE '04'
                    END
                END
            ELSE
                CASE WHEN T.JUCHITEMKBNSHOHIN = 1 
                THEN '06'  
                ELSE '05'  
                END
            END
        ELSE 
            CASE WHEN T.JUCHITEMKBNSHOHIN = 1 
            THEN '04' 
            ELSE
                    CASE WHEN T.JUCHITEMKBNTRIAL = 1 
                    THEN '03'  
                ELSE
                    CASE WHEN S.RUIJUCHKAISUTRIAL > 0
                    THEN '03'
                    ELSE
                        CASE WHEN T.JUCHITEMKBNSAMPLE = 1
                        THEN '02' 
                        ELSE
                            CASE WHEN S.RUIJUCHKAISUSAMPLE > 0
                            THEN '02'
                            ELSE '00'
                            END
                        END
                    END
                END
            END
        END AS KOKYAKBNCODE
        FROM (SELECT * FROM TT01KOKYASTSH_MV WHERE KOKYANO IN(SELECT KOKYANO FROM TW05KOKYARECALC) AND JUCHKBN NOT LIKE '9%') H
        LEFT JOIN TW09KOKYAKONYU T ON( trim(H.SALENO) = trim(T.SALENO))
        LEFT JOIN TW11_KOKOYAJUCHDATE2 S ON H.KOKYANO = S.KOKYANO AND H.JUCHDATE = S.JUCHDATE
),
final as (
    select
        saleno::varchar(61) as saleno,
        kokyakbncode::varchar(3) as kokyakbncode,      
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final
